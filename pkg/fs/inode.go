package fs

import (
	"context"
	"io/fs"
	"os"
	"time"

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

	cl *ImmuDbClient
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

/*
// Add an entry for a child.
//
// REQUIRES: in.isDir()
// REQUIRES: dt != fuseutil.DT_Unknown
func (in *inode) AddChild(
	id fuseops.InodeID,
	name string,
	dt fuseutil.DirentType) {
	var index int

	// Update the modification time.
	in.attrs.Mtime = time.Now()

	// No matter where we place the entry, make sure it has the correct Offset
	// field.
	defer func() {
		in.entries[index].Offset = fuseops.DirOffset(index + 1)
	}()

	// Set up the entry.
	e := fuseutil.Dirent{
		Inode: id,
		Name:  name,
		Type:  dt,
	}

	// Look for a gap in which we can insert it.
	for index = range in.entries {
		if in.entries[index].Type == fuseutil.DT_Unknown {
			in.entries[index] = e
			return
		}
	}

	// Append it to the end.
	index = len(in.entries)
	in.entries = append(in.entries, e)
}

// Remove an entry for a child.
//
// REQUIRES: in.isDir()
// REQUIRES: An entry for the given name exists.
func (in *inode) RemoveChild(name string) {
	// Update the modification time.
	in.attrs.Mtime = time.Now()

	// Find the entry.
	i, ok := in.findChild(name)
	if !ok {
		panic(fmt.Sprintf("Unknown child: %s", name))
	}

	// Mark it as unused.
	in.entries[i] = fuseutil.Dirent{
		Type:   fuseutil.DT_Unknown,
		Offset: fuseops.DirOffset(i + 1),
	}
}

// Serve a ReadDir request.
//
// REQUIRES: in.isDir()
func (in *inode) ReadDir(p []byte, offset int) int {
	if !in.isDir() {
		panic("ReadDir called on non-directory.")
	}

	var n int
	for i := offset; i < len(in.entries); i++ {
		e := in.entries[i]

		// Skip unused entries.
		if e.Type == fuseutil.DT_Unknown {
			continue
		}

		tmp := fuseutil.WriteDirent(p[n:], in.entries[i])
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
func (in *inode) ReadAt(p []byte, off int64) (int, error) {
	if !in.isFile() {
		panic("ReadAt called on non-file.")
	}

	// Ensure the offset is in range.
	if off > int64(len(in.contents)) {
		return 0, io.EOF
	}

	// Read what we can.
	n := copy(p, in.contents[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// Write to the file's contents. See documentation for ioutil.WriterAt.
//
// REQUIRES: in.isFile()
func (in *inode) WriteAt(p []byte, off int64) (int, error) {
	if !in.isFile() {
		panic("WriteAt called on non-file.")
	}

	// Update the modification time.
	in.attrs.Mtime = time.Now()

	// Ensure that the contents slice is long enough.
	newLen := int(off) + len(p)
	if len(in.contents) < newLen {
		padding := make([]byte, newLen-len(in.contents))
		in.contents = append(in.contents, padding...)
		in.attrs.Size = uint64(newLen)
	}

	// Copy in the data.
	n := copy(in.contents[off:], p)

	// Sanity check.
	if n != len(p) {
		panic(fmt.Sprintf("Unexpected short copy: %v", n))
	}

	return n, nil
}
*/
// Update attributes from non-nil parameters.
func (in *Inode) SetAttributes(
	size *uint64,
	mode *os.FileMode,
	mtime *time.Time) {
	// Update the modification time.
	in.Mtime = time.Now()

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

/*
func (in *inode) Fallocate(mode uint32, offset uint64, length uint64) error {
	if mode != 0 {
		return fuse.ENOSYS
	}
	newSize := int(offset + length)
	if newSize > len(in.contents) {
		padding := make([]byte, newSize-len(in.contents))
		in.contents = append(in.contents, padding...)
		in.attrs.Size = offset + length
	}
	return nil
}
*/
