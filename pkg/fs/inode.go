package fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type Inode struct {
	Inumber int64
	Size    int64
	Nlink   int64
	Mode    int64
	Atime   time.Time
	Mtime   time.Time
	Ctime   time.Time
	Crtime  time.Time
	Uid     int64
	Gid     int64

	ToBeDeleted bool
	cl          *ImmuDbClient
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

func (in *Inode) isDir() bool {
	return fs.FileMode(in.Mode)&os.ModeDir != 0
}

func (in *Inode) isSymlink() bool {
	return fs.FileMode(in.Mode)&os.ModeSymlink != 0
}

func (in *Inode) isFile() bool {
	return !(in.isDir() || in.isSymlink())
}

// getChildrenOrDie returns the list of children of a directory
//
// REQUIRES in.isDir()
func (in *Inode) getChildrenOrDie() []fuseutil.Dirent {
	entries, err := in.cl.GetChildren(context.TODO(), in.Inumber)
	if err != nil {
		panic(err)
	}

	return entries
}

func (in *Inode) writeChildrenOrDie(children []fuseutil.Dirent) {
	err := in.cl.WriteChildren(context.TODO(), in.Inumber, children)
	if err != nil {
		panic(err)
	}
}

// Return the index of the child within in.entries, if it exists.
//
// REQUIRES: in.isDir()
func (in *Inode) findChild(name string) (i int, ok bool) {
	if !in.isDir() {
		panic("findChild called on non-directory.")
	}

	var e fuseutil.Dirent
	entries := in.getChildrenOrDie()
	for i, e = range entries {
		if e.Name == name {
			return i, true
		}
	}

	return 0, false
}

// Like findChild, but returns the Dirent
func (in *Inode) findChild2(name string) (d fuseutil.Dirent, ok bool) {
	if !in.isDir() {
		panic("findChild called on non-directory.")
	}

	var e fuseutil.Dirent
	entries := in.getChildrenOrDie()
	for _, e = range entries {
		if e.Name == name {
			return e, true
		}
	}

	return e, false
}

func (in *Inode) readContentOrDie() []byte {
	content, err := in.cl.ReadContent(context.TODO(), in.Inumber)
	if err != nil {
		panic(err)
	}

	return content
}

func (in *Inode) writeContentOrDie(content []byte) {
	if err := in.cl.WriteContent(context.TODO(), in.Inumber, content); err != nil {
		panic(err)
	}
}

// Flush inode to immudb. It must be called to make every change to the inode permanent.
func (in *Inode) writeOrDie() {
	if err := in.cl.WriteInode(context.TODO(), in); err != nil {
		panic(err)
	}
}

////////////////////////////////////////////////////////////////////////
// Public methods
////////////////////////////////////////////////////////////////////////

// Constructor
// Create a new inode with the supplied attributes, which need not contain
// time-related information (the inode object will take care of that).
func NewInode(inumber int64, attrs fuseops.InodeAttributes, db *ImmuDbClient) *Inode {
	// Update time info.
	now := time.Now()
	attrs.Mtime = now
	attrs.Crtime = now

	// Create the object.
	inode := Inode{
		Inumber:     inumber,
		Size:        int64(attrs.Size),
		Nlink:       int64(attrs.Nlink),
		Mode:        int64(attrs.Mode),
		Atime:       attrs.Atime,
		Mtime:       attrs.Mtime,
		Ctime:       attrs.Ctime,
		Crtime:      attrs.Crtime,
		Uid:         int64(attrs.Uid),
		Gid:         int64(attrs.Gid),
		ToBeDeleted: false,
		cl:          db,

		// TODO manage extended attr?
		//xattrs: make(map[string][]byte),
	}
	inode.writeOrDie()
	if inode.isDir() {
		inode.writeChildrenOrDie([]fuseutil.Dirent{})
	}

	return &inode
}

// Return the number of children of the directory.
//
// REQUIRES: in.isDir()
func (in *Inode) Len() int {
	entries := in.getChildrenOrDie()
	var n int
	for _, e := range entries {
		if e.Type != fuseutil.DT_Unknown {
			n++
		}
	}

	return n
}

// Find an entry for the given child name and return its inode ID.
//
// REQUIRES: in.isDir()
func (in *Inode) LookUpChild(name string) (
	id fuseops.InodeID,
	typ fuseutil.DirentType,
	ok bool) {
	dirent, ok := in.findChild2(name)
	if ok {
		id = dirent.Inode
		typ = dirent.Type
	}

	return id, typ, ok
}

func (in *Inode) Attributes() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Size:   uint64(in.Size),
		Nlink:  uint32(in.Nlink),
		Mode:   os.FileMode(in.Mode),
		Atime:  in.Atime,
		Mtime:  in.Mtime,
		Ctime:  in.Ctime,
		Crtime: in.Crtime,
		Uid:    uint32(in.Uid),
		Gid:    uint32(in.Gid),
	}
}

// Add an entry for a child.
// It updated the Atime and Mtime of the parent
//
// REQUIRES: in.isDir()
// REQUIRES: dt != fuseutil.DT_Unknown
func (in *Inode) AddChild(
	id fuseops.InodeID,
	name string,
	dt fuseutil.DirentType) {
	var index int

	// Update the modification time.
	in.Mtime = time.Now()

	// Update the access time.
	in.Atime = time.Now()

	// Set up the entry.
	e := fuseutil.Dirent{
		Inode: id,
		Name:  name,
		Type:  dt,
	}

	// Look for a gap in which we can insert it.
	entries := in.getChildrenOrDie()
	for index = range entries {
		if entries[index].Type == fuseutil.DT_Unknown {
			entries[index] = e
			// No matter where we place the entry, make sure it has the correct Offset
			// field.
			entries[index].Offset = fuseops.DirOffset(index + 1)

			in.writeChildrenOrDie(entries)
			in.writeOrDie()
			return
		}
	}

	// Append it to the end.
	index = len(entries)
	// No matter where we place the entry, make sure it has the correct Offset
	// field.
	e.Offset = fuseops.DirOffset(index + 1)
	entries = append(entries, e)
	in.writeChildrenOrDie(entries)
	in.writeOrDie()
}

// Remove an entry for a child.
// It also updates the Atime and Mtime of the parent.
//
// REQUIRES: in.isDir()
// REQUIRES: An entry for the given name exists.
func (in *Inode) RemoveChild(name string) {
	// Update the modification time.
	in.Mtime = time.Now()

	// Update the acccess time
	in.Atime = time.Now()

	// Find the entry.
	i, ok := in.findChild(name)
	if !ok {
		panic(fmt.Sprintf("Unknown child: %s", name))
	}

	// Mark it as unused.
	entries := in.getChildrenOrDie()
	entries[i] = fuseutil.Dirent{
		Type:   fuseutil.DT_Unknown,
		Offset: fuseops.DirOffset(i + 1),
	}
	in.writeChildrenOrDie(entries)
	in.writeOrDie()
}

// Serve a ReadDir request.
//
// REQUIRES: in.isDir()
func (in *Inode) ReadDir(p []byte, offset int) int {
	if !in.isDir() {
		panic("ReadDir called on non-directory.")
	}

	var n int
	entries := in.getChildrenOrDie()

	// Update the acccess time
	in.Atime = time.Now()
	in.writeOrDie()

	for i := offset; i < len(entries); i++ {
		e := entries[i]

		// Skip unused entries.
		if e.Type == fuseutil.DT_Unknown {
			continue
		}

		tmp := fuseutil.WriteDirent(p[n:], entries[i])
		if tmp == 0 {
			break
		}

		n += tmp
	}

	return n
}

// Read from the file's contents. See documentation for ioutil.ReaderAt.
//
// REQUIRES: in.isFile()
func (in *Inode) ReadAt(p []byte, off int64) (int, error) {
	if !in.isFile() {
		panic("ReadAt called on non-file.")
	}

	content := in.readContentOrDie()
	// Ensure the offset is in range.
	if off > int64(len(content)) {
		return 0, io.EOF
	}

	// Read what we can.
	n := copy(p, content[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// Write to the file's contents. See documentation for ioutil.WriterAt.
//
// REQUIRES: in.isFile()
func (in *Inode) WriteAt(p []byte, off int64) (int, error) {
	if !in.isFile() {
		panic("WriteAt called on non-file.")
	}

	// Update the modification time.
	in.Atime = time.Now()
	in.Mtime = time.Now()
	content := in.readContentOrDie()

	// Ensure that the contents slice is long enough.
	newLen := int(off) + len(p)
	if len(content) < newLen {
		padding := make([]byte, newLen-len(content))
		content = append(content, padding...)
		in.Size = int64(newLen)
	}

	// Copy in the data.
	n := copy(content[off:], p)

	// Sanity check.
	if n != len(p) {
		panic(fmt.Sprintf("Unexpected short copy: %v", n))
	}

	in.writeContentOrDie(content)
	in.writeOrDie()

	return n, nil
}

// Update attributes from non-nil parameters.
func (in *Inode) SetAttributes(
	size *uint64,
	mode *os.FileMode,
	mtime *time.Time) {
	// Update the modification time.
	in.Atime = time.Now()
	in.Mtime = time.Now()
	in.Ctime = time.Now()

	// Truncate?
	if size != nil {
		intSize := int(*size)

		// Update contents.
		content := in.readContentOrDie()
		if intSize <= len(content) {
			content = content[:intSize]
			in.writeContentOrDie(content)
		} else {
			padding := make([]byte, intSize-len(content))
			content = append(content, padding...)
			in.writeContentOrDie(content)
		}

		// Update attributes.
		in.Size = int64(*size)
	}

	// Change mode?
	if mode != nil {
		in.Mode = int64(*mode)
	}

	// Change mtime?
	if mtime != nil {
		in.Mtime = *mtime
	}

	// Write Inode data
	in.writeOrDie()
}

// Allocate space for the file. Updates the Atime
func (in *Inode) Fallocate(mode uint32, offset uint64, length uint64) error {
	if mode != 0 {
		return fuse.ENOSYS
	}
	newSize := int(offset + length)
	content := in.readContentOrDie()
	if newSize > len(content) {
		padding := make([]byte, newSize-len(content))
		content = append(content, padding...)
		in.Size = int64(offset + length)

		in.Atime = time.Now()
		in.Mtime = time.Now()
		in.Ctime = time.Now()

		in.writeOrDie()
		in.writeContentOrDie(content)
	}
	return nil
}

// DecrRef decrements the reference counter and returns its current value.
// The reference count can't become negative.
func (in *Inode) DecrRef(N uint64) int64 {
	in.Nlink -= int64(N)
	if in.Nlink < 0 {
		in.Nlink = 0
	}

	in.writeOrDie()

	return in.Nlink
}

// Delete an Inode from Immudb
func (in *Inode) Del() {
	err := in.cl.DeleteInode(context.TODO(), in.Inumber)
	if err != nil {
		panic(err)
	}
}
