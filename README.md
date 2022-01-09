# ImmuFS

<img align="right" src="icon.png" width="256px"/>

Immudb based FUSE filesystem, written in Go.
The project is base on the [https://github.com/jacobsa/fuse](https://github.com/jacobsa/fuse) library, and in particular it has been inspired by the `memFS` implementation.
All tests have been performed using macos 12.1 and macFUSE 4.2.1.
Linux works too, provided that the Immufs is mounted as root and the PID used to write files is not 0 (comment the check in case).

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
$> ./immufs -s 127.0.0.1 -m mnt -u immudb -p immudb
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

## Time-machine

It is possible to use the test tool to inspect how file content changed during time. The tool enables the user to retrieve the content of a file at a specified time point, using the Transaction identifier.
To build the tool, run the following command:

```bash
$> cd time-machine
$> go build
```

The tool has two different kinds of output, depending on the file type: binary and string. To activate the string output (only for text files), use the `-s` option.
Examples:

```bash
$> ./time-machine -c ../config.yaml -t 380 -i 1 -s
INFO[0000] Before TX=380 the file content was:
[{"Offset":1,"Inode":2,"Name":"abc","Type":4},{"Offset":2,"Inode":3,"Name":"1234","Type":8}]
$> ./time-machine -c ../config.yaml -t 980 -i 1 -s
INFO[0000] Before TX=980 the file content was:
[{"Offset":1,"Inode":2,"Name":"abc","Type":4},{"Offset":2,"Inode":3,"Name":"pippo","Type":8},{"Offset":3,"Inode":4,"Name":"aaa","Type":8}]
$> ./time-machine -c ../config.yaml -t 1000000 -i 1 
INFO[0000] Before TX=1000000 the file content was:
[91 123 34 79 102 102 115 101 116 34 58 49 44 34 73 110 111 100 101 34 58 50 44 34 78 97 109 101 34 58 34 97 98 99 34 44 34 84 121 112 101 34 58 56 125 44 123 34 79 102 102 115 101 116 34 58 50 44 34 73 110 111 100 101 34 58 51 44 34 78 97 109 101 34 58 34 49 50 51 52 34 44 34 84 121 112 101 34 58 52 125 44 123 34 79 102 102 115 101 116 34 58 51 44 34 73 110 111 100 101 34 58 52 44 34 78 97 109 101 34 58 34 119 111 114 108 100 46 116 120 116 34 44 34 84 121 112 101 34 58 56 125 44 123 34 79 102 102 115 101 116 34 58 52 44 34 73 110 111 100 101 34 58 53 44 34 78 97 109 101 34 58 34 120 120 120 34 44 34 84 121 112 101 34 58 56 125 93] 
```

## BUGS AND LIMITATIONS

ImmuFS implementation is not complete and has some defetcs:

- refcnt should be improved. Temporarily patched with a flag in the database which mark a file to be deleted.
- hard links and symlinks are not implemented.
- timestamp management should be improved.
- Rename API has a bug (used by `mv` command). It works under Linux btw.
- File handles are not implemented.
- Unknown performance. Given the amount of db accesses, it's likely that performance are not excellent.
- Inumbers are never reused.
- Immufs does not support extended attributes.
