package fs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"immufs/pkg/config"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/stdlib"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
)

type ImmuDbClient struct {
	cl  *sql.DB
	log *logrus.Entry
}

// Helpers
func marshalDirents(dirent []fuseutil.Dirent) ([]byte, error) {
	return json.Marshal(dirent)
}

func unmarshalDirents(data []byte) ([]fuseutil.Dirent, error) {
	var ret []fuseutil.Dirent
	err := json.Unmarshal(data, &ret)

	return ret, err
}

// Instantiate and connect the Immudb client
func NewImmuDbClient(ctx context.Context, cfg *config.Config, log *logrus.Logger) (*ImmuDbClient, error) {
	opts := client.DefaultOptions()
	opts.Address = cfg.Immudb
	opts.Username = cfg.User
	opts.Password = cfg.Password
	db := stdlib.OpenDB(opts)
	return &ImmuDbClient{
		cl:  db,
		log: log.WithFields(logrus.Fields{"component": "immudb client"}),
	}, nil
}

func (idb *ImmuDbClient) Destroy(ctx context.Context) error {
	err := idb.cl.Close()
	if err != nil {
		idb.log.Errorf("could not close session: %s", err)

		return err
	}

	return nil
}

func (idb *ImmuDbClient) GetInode(ctx context.Context, inumber int64) (*Inode, error) {
	res, err := idb.cl.QueryContext(ctx, "SELECT * from inode WHERE inumber=?", inumber)
	if err != nil {
		idb.log.Errorf("could not get inode %d: %s", inumber, err)

		return nil, err
	}

	var inode Inode

	defer res.Close()
	if found := res.Next(); !found {
		idb.log.Errorf("Inode %d not found", inumber)

		return nil, fmt.Errorf("Inode %d not found", inumber)
	}

	err = res.Scan(
		&inode.Inumber,
		&inode.Size,
		&inode.Nlink,
		&inode.Mode,
		&inode.Atime,
		&inode.Mtime,
		&inode.Ctime,
		&inode.Crtime,
		&inode.Uid,
		&inode.Gid,
	)
	inode.cl = idb
	if err != nil {
		idb.log.Errorf("could not scan inode %d: %s", inumber, err)

		return nil, err
	}

	return &inode, nil
}

func (idb *ImmuDbClient) GetChildren(ctx context.Context, parent int64) ([]fuseutil.Dirent, error) {
	res, err := idb.cl.QueryContext(ctx, "SELECT content from content WHERE inumber=?", parent)
	if err != nil {
		idb.log.Errorf("could not get directory %d content: %s", parent, err)

		return nil, err
	}

	var content []byte

	defer res.Close()
	if found := res.Next(); !found {
		idb.log.Errorf("Directory %d content not found", parent)

		return nil, fmt.Errorf("Inode %d not found", parent)
	}

	err = res.Scan(content)
	if err != nil {
		idb.log.Errorf("could not read directory %d content: %s", parent, err)

		return nil, err
	}

	dirents, err := unmarshalDirents(content)
	if err != nil {
		idb.log.Errorf("could not unmarshal dirents of inode %d: %s", parent, err)

		return nil, err
	}

	return dirents, err
}

func (idb *ImmuDbClient) WriteChildren(ctx context.Context, parentInumber int64, children []fuseutil.Dirent) error {
	content, err := marshalDirents(children)
	if err != nil {
		idb.log.Errorf("could not marshal directory entries: %s", children)

		return err
	}

	err = idb.WriteContent(ctx, parentInumber, content)
	if err != nil {
		idb.log.Errorf("could not write directory content: %s", err)

		return err
	}

	return nil
}

// File content can be read as a whole
func (idb *ImmuDbClient) ReadContent(ctx context.Context, inumber int64) ([]byte, error) {
	res, err := idb.cl.QueryContext(ctx, "SELECT content from content WHERE inumber=?", inumber)
	if err != nil {
		idb.log.Errorf("could not get file %d content: %s", inumber, err)

		return nil, err
	}

	var content []byte

	defer res.Close()
	if found := res.Next(); !found {
		idb.log.Errorf("File %d content not found", inumber)

		return nil, fmt.Errorf("Inode %d not found", inumber)
	}

	err = res.Scan(content)
	if err != nil {
		idb.log.Errorf("could not read file %d content: %s", inumber, err)

		return nil, err
	}

	return content, err
}

// File content can be written as a whole
func (idb *ImmuDbClient) WriteContent(ctx context.Context, inumber int64, data []byte) error {
	_, err := idb.cl.ExecContext(ctx, "UPSERT INTO content(inumber, content) VALUES(?, ?)", inumber, data)
	if err != nil {
		idb.log.Errorf("could not write file %d content: %s", inumber, err)
	}

	return err
}

func (idb *ImmuDbClient) WriteInode(ctx context.Context, inode *Inode) error {
	_, err := idb.cl.ExecContext(ctx, "UPSERT INTO inode(inumber, size, nlink, mode, atime, mtime, ctime, crtime, uid, gid) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
		inode.Inumber, inode.Size, inode.Nlink, inode.Mode, inode.Atime, inode.Mtime, inode.Ctime, inode.Crtime, inode.Uid, inode.Gid)
	if err != nil {
		idb.log.Errorf("could not write file %s", err)
	}

	return err
}

func (idb *ImmuDbClient) NextInumber(ctx context.Context) (int64, error) {
	res, err := idb.cl.QueryContext(ctx, "SELECT MAX(inumber) from inode")
	if err != nil {
		return -1, err
	}

	var inumber int64

	defer res.Close()
	if found := res.Next(); !found {
		return 0, nil
	}

	err = res.Scan(
		&inumber,
	)

	return inumber, nil
}
