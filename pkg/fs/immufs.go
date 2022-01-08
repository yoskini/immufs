package fs

import (
	"context"
	"errors"
	"immufs/pkg/config"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
)

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
			Mode: 0700 | os.ModeDir,
			Uid:  fs.uid,
			Gid:  fs.gid,
		}
		// Adding root if not exists
		root := NewInode(fuseops.RootInodeID, rootAttrs, fs.idb)
		rootEnts := make([]fuseutil.Dirent, 0)
		root.writeChildrenOrDie(rootEnts)
		fs.log.Info("root inode created")
	}

	return fs, nil
}

// Utilities
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

// Calculate the next available inumber
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
	return nil
}

func (fs *Immufs) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent directory.
	inode := fs.getInodeOrDie(op.Parent)

	// Does the directory have an entry with the given name?
	childID, _, ok := inode.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Fill in the response.
	op.Entry.Child = childID
	op.Entry.Attributes = child.Attributes()

	// We don't spontaneously mutate, so the kernel can cache as long as it wants
	// (since it also handles invalidation).
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration

	return nil
}

func (fs *Immufs) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	if op.OpContext.Pid == 0 {
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

	return nil
}

func (fs *Immufs) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	var err error
	if op.Size != nil && op.Handle == nil && *op.Size != 0 {
		// require that truncate to non-zero has to be ftruncate()
		// but allow open(O_TRUNC)
		err = syscall.EBADF
	}

	// Grab the inode.
	inode := fs.getInodeOrDie(op.Inode)

	// Handle the request.
	inode.SetAttributes(op.Size, op.Mode, op.Mtime)

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
	childAttrs := fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  op.Mode,
		Uid:   fs.uid,
		Gid:   fs.gid,
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

	return nil
}

func (fs *Immufs) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	if op.OpContext.Pid == 0 {
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
	if op.OpContext.Pid == 0 {
		// CreateFileOp should have a valid pid in context.
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.Entry, err = fs.createFile(op.Parent, op.Name, op.Mode)
	return err
}

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

func (fs *Immufs) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ask the old parent for the child's inode ID and type.
	oldParent := fs.getInodeOrDie(op.OldParent)
	childID, childType, ok := oldParent.LookUpChild(op.OldName)

	if !ok {
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
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Make sure the child is empty.
	if child.Len() != 0 {
		return fuse.ENOTEMPTY
	}

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	child.Nlink--
	child.writeOrDie()

	return nil
}

func (fs *Immufs) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the parent, which we will update shortly.
	parent := fs.getInodeOrDie(op.Parent)

	// Find the child within the parent.
	childID, _, ok := parent.LookUpChild(op.Name)
	if !ok {
		return fuse.ENOENT
	}

	// Grab the child.
	child := fs.getInodeOrDie(childID)

	// Remove the entry within the parent.
	parent.RemoveChild(op.Name)

	// Mark the child as unlinked.
	child.Nlink--
	child.writeOrDie()

	return nil
}

// TODO should I implement a dir handler?
func (fs *Immufs) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	if op.OpContext.Pid == 0 {
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

	return nil
}

func (fs *Immufs) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab the directory.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	op.BytesRead = inode.ReadDir(op.Dst, int(op.Offset))

	return nil
}

// TODO should I implement a file handler?
func (fs *Immufs) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	if op.OpContext.Pid == 0 {
		// OpenFileOp should have a valid pid in context.
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

	return nil
}

func (fs *Immufs) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	if op.OpContext.Pid == 0 {
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

	return err
}

func (fs *Immufs) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode in question.
	inode := fs.getInodeOrDie(op.Inode)

	// Serve the request.
	_, err := inode.WriteAt(op.Data, op.Offset)

	return err
}

// TODO shoud I support file handler management here?
func (fs *Immufs) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {
	if op.OpContext.Pid == 0 {
		// FlushFileOp should have a valid pid in context.
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
	if op.OpContext.Pid == 0 {
		return fuse.EINVAL
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	inode := fs.getInodeOrDie(op.Inode)
	inode.Fallocate(op.Mode, op.Offset, op.Length)
	return nil
}
