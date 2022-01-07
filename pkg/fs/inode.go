package fs

import (
	"sync"
	"time"
)

type Inode struct {
	inumber int64
	name    int64
	size    int64
	nlink   int64
	mode    int64
	atime   *time.Time
	mtime   *time.Time
	ctime   *time.Time
	crtime  *time.Time
	uid     int64
	gid     int64
	parent  int64
	m       sync.Mutex // NOTE: we could use atomic types and avoid the mutex usage
}

// Getters
func (in *Inode) Inumber() int64 {
	in.m.Lock()
	defer in.m.Unlock()
	return in.inumber
}
func (in *Inode) Name() int64 {
	in.m.Lock()
	defer in.m.Unlock()
	return in.name
}
func (in *Inode) Size() int64 {
	in.m.Lock()
	defer in.m.Unlock()
	return in.size
}
func (in *Inode) NLink() int64 {
	in.m.Lock()
	defer in.m.Unlock()
	return in.nlink
}
func (in *Inode) Mode() int64 {
	in.m.Lock()
	defer in.m.Unlock()
	return in.mode
}
func (in *Inode) ATime() *time.Time {
	in.m.Lock()
	defer in.m.Unlock()
	return in.atime
}
func (in *Inode) MTime() *time.Time {
	in.m.Lock()
	defer in.m.Unlock()
	return in.mtime
}
func (in *Inode) CTime() *time.Time {
	in.m.Lock()
	defer in.m.Unlock()
	return in.ctime
}
func (in *Inode) CrTime() *time.Time {
	in.m.Lock()
	defer in.m.Unlock()
	return in.crtime
}
func (in *Inode) Uid() int64 {
	in.m.Lock()
	defer in.m.Unlock()
	return in.uid
}
func (in *Inode) Gid() int64 {
	in.m.Lock()
	defer in.m.Unlock()
	return in.gid
}
