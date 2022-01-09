# ImmuFS

<img align="right" src="icon.png" width="256px"/>

Immudb based FUSE filesystem, written in Go.
The project is base on the [https://github.com/jacobsa/fuse](https://github.com/jacobsa/fuse) library, and in particular it has been inspired by the `memFS` implementation.

## How to build

Simply enter into the project directory and type:

```bash
$> go build
```

## How to run

Immudb supports a set of command line options, but those options can also be specified through a configuration file.
To start the application, create a mountpoint and run:

```bash
$> mkdir mnt
$> ./immufs -s defaultdb -m mnt -u immudb -p immudb
```

or, in alternative, use:

```bash
$> ./immufs -c config.yaml 
```

An example of usage is as follows:

```bash
$> cd mnt
mnt $> touch abc
mnt $> mkdir 1234
mnt $> ls            
1234 abc
mnt $> echo hello > world.txt
mnt $> ls
1234      abc       world.txt
mnt $> cat world.txt 
hello
mnt $> echo -n 123 > xxx
mnt $> echo 456 >> xxx  
mnt $> cat xxx          
123456
```

## BUGS AND LIMITATIONS

ImmuFS implementation is not complete and has some defetcs:

- refcnt should be improved. Temporarily patched with a flag in the database which mark a file to be deleted.
- hard links and symlinks are not implemented.
- timestamp management should be improved.
- Rename API has a bug (used by `mv` command).
- File handles are not implemented.
- Unknown performance. Given the amount of db accesses, it's likely that performance are not excellent.
- Inumbers are never reused.
- Immufs does not support extended attributes.
