CREATE TABLE inode (inumber INTEGER AUTO_INCREMENT, size INTEGER NOT NULL, nlink INTEGER NOT NULL, mode INTEGER NOT NULL, atime TIMESTAMP NULL, mtime TIMESTAMP NULL, ctime TIMESTAMP NULL, crtime TIMESTAMP NULL, uid INTEGER NOT NULL, gid INTEGER NOT NULL, PRIMARY KEY(inumber));

CREATE TABLE content(inumber INTEGER, content BLOB, PRIMARY KEY(inumber));
