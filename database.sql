CREATE TABLE inode (inumber INTEGER, size INTEGER NOT NULL, nlink INTEGER NOT NULL, mode INTEGER NOT NULL, atime TIMESTAMP NULL, mtime TIMESTAMP NULL, ctime TIMESTAMP NULL, crtime TIMESTAMP NULL, uid INTEGER NOT NULL, gid INTEGER NOT NULL, to_be_deleted BOOLEAN, PRIMARY KEY(inumber));

CREATE TABLE content(inumber INTEGER, content BLOB, PRIMARY KEY(inumber));
