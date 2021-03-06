package fs

import (
	"context"
	"errors"
	"immufs/pkg/config"
	"io"
	"math"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
)

// Immufs is a filesystem backed by Immudb. All inodes are kept in the `inode` table.
// The file content is stored in the `content` table.
type Immufs struct {
	fuseutil.NotImplementedFileSystem

	idb *ImmuDbClient
	log *logrus.Entry

	uid uint32
	gid uint32

	mu sync.Mutex
}

// Immufs constructor
func NewImmufs(ctx context.Context, cfg *config.Config, logger *logrus.Logger) (*Immufs, error) {
	log := logger.WithField("component", "immufs")
	cl, err := NewImmuDbClient(ctx, cfg, logger)
	if err != nil {
		return nil, errors.New("failed to create immudb client: " + err.Error())
	}

	fs := &Immufs{
		idb: cl,
		log: log,
		uid: cfg.Uid,
		gid: cfg.Gid,
	}

	// Lookup root
	_, err = fs.idb.GetInode(ctx, 1)
	if err != nil {
		if !errors.Is(err, ErrInodeNotFound) {
			return nil, err
		}

		// Set up the root inode.
		rootAttrs := fuseops.InodeAttributes{
			Mode:  0700 | os.ModeDir,
			Uid:   fs.uid,
			Gid:   fs.gid,
			Nlink: 1,
		}
		// Adding root if not exists
		root := NewInode(fuseops.RootInodeID, rootAttrs, fs.idb)
		rootEnts := make([]fuseutil.Dirent, 0)
		root.writeChildrenOrDie(rootEnts)
		fs.log.Info("root inode created")
	}

	return fs, nil
}

////////////////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////////////////

// Find the given inode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *Immufs) getInodeOrDie(id fuseops.InodeID) *Inode {
	inode, err := fs.idb.GetInode(context.TODO(), int64(id))
	if err != nil {
		fs.log.Panicf("could not get inode %d: %s", id, err)
	}

	return inode
}

// nextInumber calculates the next available inumber. The function takes the maximum inumber from the db and increments it by 1.
// In this implementation, inodes are never re-used.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *Immufs) nextInumber() int64 {
	next, err := fs.idb.NextInumber(context.TODO())
	if err != nil {
		fs.log.Panic("could not get an available inumber: %s", err)
	}

	return next
}

// Allocate a new inode, assigning it an ID that is not in use.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *Immufs) allocateInode(
	attrs fuseops.InodeAttributes) (id fuseops.InodeID, inode *Inode) {
	// Create the inode.
	inode = NewInode(fs.nextInumber(), attrs, fs.idb)

	return fuseops.InodeID(inode.Inumber), inode
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fs *Immufs) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	fs.log.Infof("--> StatFS")

	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.BlockSize = 1
	op.Blocks = uint64(math.Pow(2, 31)) // Max FS size is 2GB

	space, err := fs.idb.SpaceUsed(context.TODO())
	if err != nil {
		space = 0 // We decide that in case of error the FS appears empty
	}
	op.BlocksFree = op.Blocks - uint64(space)
	op.BlocksAvailable = op.BlocksFree

	op.IoSize = 1

	op.Inodes = uint64(fs.nextInumber() - 1)
	op.InodesFree = math.MaxInt64 - op.Inodes

	fs.log.WithField("API", "StatFS").Debugf("Stat: %+v", op)

	return nil
}

func (fs *Immufs) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {
	fs.log.Infof("--> LookupInode: %s in parent inode: %d", op.Name, op.Parent)
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "LookupInode").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent directory.
	inode := fs.getInodeOrDie(op.Parent)

	// Does the directory have an entry with the given name?
	childID, _, ok := inode.LookUpChild(op.Name)
	if !ok {
		fs.log.WithField("API", "LookupInode").Warningf("Entry %s not found", op.Name)

		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Increment ref cnt
	child.Nlink++

	// Update access time
	child.Atime = time.Now()
	child.writeOrDie()

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attributes()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	fs.log.WithField("API", "LookupInode").Infof("Inode found: %+v", *op)

	return nil
}

func (fs *Immufs) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	fs.log.Infof("--> GetInodeAttributes: %d", op.Inode)
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "GetInodeAttributes").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the inode.
	inode := fs.getInodeOrDie(op.Inode)

	// Fill in the response.
	op.Attributes = inode.Attributes()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	// Update atime
	inode.Atime = time.Now()
	inode.writeOrDie()

	fs.log.WithField("API", "GetInodeAttributes").Infof("Attributes got: %+v", *op)
	return nil
}

func (fs *Immufs) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	fs.log.Infof("--> SetInodeAttributes")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "SetInodeAttributes").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	var err error
	if op.Size != nil && op.Handle == nil && *op.Size != 0 {
		// require that truncate to non-zero has to be ftruncate()
		// but allow open(O_TRUNC)
		fs.log.WithField("API", "SetInodeAttributes").Warningf("Bad file size")
		err = syscall.EBADF
	}

	// Grab the inode.
	inode := fs.getInodeOrDie(op.Inode)

	// Handle the request.
	inode.SetAttributes(op.Size, op.Mode, op.Mtime)

	// atime is managed by the SetAttributes func

	// Fill in the response.
	op.Attributes = inode.Attributes()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return err
}

func (fs *Immufs) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) error {
	fs.log.Infof("--> MkDir: %s", op.Name)
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "MkDir").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		fs.log.WithField("API", "MkDir").Warningf("Entry %s already exists", op.Name)

		return fuse.EEXIST
	}

	// Set up attributes from the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Mode:   op.Mode,
		Uid:    fs.uid,
		Gid:    fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, op.Name, fuseutil.DT_Directory)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attributes()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	fs.log.WithField("API", "MkDir").Infof("Directory created: %+v", *op)

	return nil
}

func (fs *Immufs) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	fs.log.Infof("--> MkNode")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "MkDir").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	var err error
	op.Entry, err = fs.createFile(op.Parent, op.Name, op.Mode)
	return err
}

// LOCKS_REQUIRED(fs.mu)
func (fs *Immufs) createFile(
	parentID fuseops.InodeID,
	name string,
	mode os.FileMode) (fuseops.ChildInodeEntry, error) {
	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(parentID)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(name)
	if exists {
		fs.log.WithField("API", "createFile").Warningf("Entry %s already exists", name)
		return fuseops.ChildInodeEntry{}, fuse.EEXIST
	}

	// Set up attributes for the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Mode:   mode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.uid,
		Gid:    fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

	// Add an entry in the parent.
	parent.AddChild(childID, name, fuseutil.DT_File)

	// Fill in the response entry.
	var entry fuseops.ChildInodeEntry
	entry.Child = childID
	entry.Attributes = child.Attributes()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	entry.EntryExpiration = entry.AttributesExpiration

	return entry, nil
}

func (fs *Immufs) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {
	fs.log.Infof("--> CreateFile")
	if op.OpContext.Pid == 0 {
		// CreateFileOp should have a valid pid in context.
		fs.log.WithField("API", "MkDir").Warningf("Invalid PID 0")
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.Entry, err = fs.createFile(op.Parent, op.Name, op.Mode)
	return err
}

//NOTE These methods are currently not implemented as we must have a rock solid
// nlink management before proceeding
/*
func (fs *Immufs) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		return fuse.EEXIST
	}

	// Set up attributes from the child.
	now := time.Now()
	childAttrs := fuseops.InodeAttributes{
		Nlink:  1,
		Mode:   0444 | os.ModeSymlink,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.uid,
		Gid:    fs.gid,
	}

	// Allocate a child.
	childID, child := fs.allocateInode(childAttrs)

	// Set up its target.
	child.target = op.Target

	// Add an entry in the parent.
	parent.AddChild(childID, op.Name, fuseutil.DT_Link)

	// Fill in the response entry.
	op.Entry.Child = childID
	op.Entry.Attributes = child.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fs *Immufs) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Ensure that the name doesn't already exist, so we don't wind up with a
	// duplicate.
	_, _, exists := parent.LookUpChild(op.Name)
	if exists {
		return fuse.EEXIST
	}

	// Get the target inode to be linked
	target := fs.getInodeOrDie(op.Target)

	// Update the attributes
	now := time.Now()
	target.attrs.Nlink++
	target.attrs.Ctime = now

	// Add an entry in the parent.
	parent.AddChild(op.Target, op.Name, fuseutil.DT_File)

	// Return the response.
	op.Entry.Child = op.Target
	op.Entry.Attributes = target.attrs

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}
*/

//BUG: This function has a weird behaviour: it might not find the inode to rename or even crash.
// The received parameters appear corrupted...
func (fs *Immufs) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	fs.log.Infof("--> Rename: %+v", *op)
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "Rename").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ask the old parent for the child's inode ID and type.
	oldParent := fs.getInodeOrDie(op.OldParent)
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
		fs.log.WithField("API", "Rename").Warningf("Entry '%s' not found in parent: %d", op.OldName, op.OldParent)

		return fuse.ENOENT
	}

	// If the new name exists already in the new parent, make sure it's not a
	// non-empty directory, then delete it.
	newParent := fs.getInodeOrDie(op.NewParent)
	existingID, _, ok := newParent.LookUpChild(op.NewName)
	if ok {
		existing := fs.getInodeOrDie(existingID)

		var buf [4096]byte
		if existing.isDir() && existing.ReadDir(buf[:], 0) > 0 {
			fs.log.WithField("API", "Rename").Warningf("Entry %s not empty", op.NewName)

			return fuse.ENOTEMPTY
		}

		newParent.RemoveChild(op.NewName)
	}

	// Link the new name.
	newParent.AddChild(
		childID,
		op.NewName,
		childType)

	// Finally, remove the old name from the old parent.
	oldParent.RemoveChild(op.OldName)

	return nil
}

func (fs *Immufs) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) error {
	fs.log.Infof("--> RmDir")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "RmDir").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		fs.log.WithField("API", "RmDir").Warningf("Entry %s not found", op.Name)

		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Make sure the child is empty.
	if child.Len() != 0 {
		fs.log.WithField("API", "RmDir").Warningf("Entry %s not empty", op.Name)

		return fuse.ENOTEMPTY
	}

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	child.Nlink--
	child.ToBeDeleted = true
	child.Atime = time.Now()
	child.writeOrDie()

	return nil
}

func (fs *Immufs) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	fs.log.Infof("--> Unlink")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "Unlink").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		fs.log.WithField("API", "Unlink").Warningf("Entry %s not found", op.Name)

		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	child.Nlink--
	child.ToBeDeleted = true
	child.Atime = time.Now()
	child.writeOrDie()

	return nil
}

// TODO should I implement a dir handler?
func (fs *Immufs) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	fs.log.Infof("--> OpenDir")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "OpenDir").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// inode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	inode := fs.getInodeOrDie(op.Inode)

	if !inode.isDir() {
		panic("Found non-dir.")
	}

	// Update atime
	inode.Atime = time.Now()
	inode.writeOrDie()

	return nil
}

func (fs *Immufs) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	fs.log.Infof("--> ReadDir")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "ReadDir").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the directory.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	op.BytesRead = inode.ReadDir(op.Dst, int(op.Offset))

	// Update atime
	inode.Atime = time.Now()
	inode.writeOrDie()

	return nil
}

// TODO should I implement a file handler?
func (fs *Immufs) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	fs.log.Infof("--> OpenFile")
	if op.OpContext.Pid == 0 {
		// OpenFileOp should have a valid pid in context.
		fs.log.WithField("API", "OpenFile").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// We don't mutate spontaneosuly, so if the VFS layer has asked for an
	// inode that doesn't exist, something screwed up earlier (a lookup, a
	// cache invalidation, etc.).
	inode := fs.getInodeOrDie(op.Inode)

	if !inode.isFile() {
		panic("Found non-file.")
	}

	// Update atime
	inode.Atime = time.Now()
	inode.writeOrDie()

	return nil
}

func (fs *Immufs) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	fs.log.Infof("--> ReadFile")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "ReadFile").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode in question.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	var err error
	op.BytesRead, err = inode.ReadAt(op.Dst, op.Offset)

	// Don't return EOF errors; we just indicate EOF to fuse using a short read.
	if err == io.EOF {
		return nil
	}

	// Update atime
	inode.Atime = time.Now()
	inode.writeOrDie()

	return err
}

func (fs *Immufs) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	fs.log.Infof("--> WriteFile")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "WriteFile").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode in question.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	_, err := inode.WriteAt(op.Data, op.Offset)

	inode.writeOrDie()

	return err
}

// FlushFile is not required as we immediately write the bytes into the database.
// There's not local caching, hence there's no need to write any buffer.
func (fs *Immufs) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {
	fs.log.Infof("--> FlushFile")
	if op.OpContext.Pid == 0 {
		// FlushFileOp should have a valid pid in context.
		fs.log.WithField("API", "FlushFile").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	return
}

/*
func (fs *Immufs) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode in question.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	op.Target = inode.target

	return nil
}

func (fs *Immufs) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode := fs.getInodeOrDie(op.Inode)
	if value, ok := inode.xattrs[op.Name]; ok {
		op.BytesRead = len(value)
		if len(op.Dst) >= len(value) {
			copy(op.Dst, value)
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
	} else {
		return fuse.ENOATTR
	}

	return nil
}

func (fs *Immufs) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode := fs.getInodeOrDie(op.Inode)

	dst := op.Dst[:]
	for key := range inode.xattrs {
		keyLen := len(key) + 1

		if len(dst) >= keyLen {
			copy(dst, key)
			dst = dst[keyLen:]
		} else if len(op.Dst) != 0 {
			return syscall.ERANGE
		}
		op.BytesRead += keyLen
	}

	return nil
}

func (fs *Immufs) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	inode := fs.getInodeOrDie(op.Inode)

	if _, ok := inode.xattrs[op.Name]; ok {
		delete(inode.xattrs, op.Name)
	} else {
		return fuse.ENOATTR
	}
	return nil
}

func (fs *Immufs) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	inode := fs.getInodeOrDie(op.Inode)

	_, ok := inode.xattrs[op.Name]

	switch op.Flags {
	case unix.XATTR_CREATE:
		if ok {
			return fuse.EEXIST
		}
	case unix.XATTR_REPLACE:
		if !ok {
			return fuse.ENOATTR
		}
	}

	value := make([]byte, len(op.Value))
	copy(value, op.Value)
	inode.xattrs[op.Name] = value
	return nil
}
*/

func (fs *Immufs) Fallocate(ctx context.Context,
	op *fuseops.FallocateOp) error {
	fs.log.Infof("--> Fallocate")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "Fallocate").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	inode := fs.getInodeOrDie(op.Inode)
	inode.Fallocate(op.Mode, op.Offset, op.Length)

	return nil
}

func (fs *Immufs) ForgetInode(ctx context.Context,
	op *fuseops.ForgetInodeOp) error {
	fs.log.Infof("--> ForgetInode")
	if op.OpContext.Pid == 0 {
		fs.log.WithField("API", "ForgetInode").Warningf("Invalid PID 0")

		return fuse.EINVAL
	}

	inode := fs.getInodeOrDie(op.Inode)
	cnt := inode.DecrRef(op.N)
	if cnt == 0 && inode.ToBeDeleted {
		inode.Del()
	}

	return nil
}
