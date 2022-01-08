CREATE TABLE inode (inumber INTEGER AUTO_INCREMENT, size INTEGER NOT NULL, nlink INTEGER NOT NULL, mode INTEGER NOT NULL, atime TIMESTAMP NULL, mtime TIMESTAMP NULL, ctime TIMESTAMP NULL, crtime TIMESTAMP NULL, uid INTEGER NOT NULL, gid INTEGER NOT NULL, parent INTEGER, PRIMARY KEY(inumber));
CREATE INDEX name ON inode (name);
CREATE INDEX parent ON inode (parent);


CREATE TABLE content(inumber INTEGER, content BLOB, PRIMARY KEY(inumber));
