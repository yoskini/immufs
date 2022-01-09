# ImmuFS

Immudb based FUSE filesystem, written in Go.

## How to build

```bash
$> go build
```

## How to run

```bash
$> ./immufs -s defaultdb -m mnt -u immudb -p immudb
```

## BUGS AND LIMITATIONS

ImmuFS implementation is not complete and has some defetcs:

- refcnt should be improved. Temporarily patched with a flag in the database which mark a file to be deleted.
- hard links and symlinks are not implemented.
- timestamp management should be improved.
- Rename API has a bug (used by `mv` command).
- File handles are not implemented.
- Unknown performance. Given the amount of db accesses, it's likely that performance are not excellent.
