package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"immufs/pkg/config"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/stdlib"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "This tool is intended for performing file content verification, through a sort of time machine.\nThe provided inumber is used to retrieve the file content at the given transaction.\n")
		flag.PrintDefaults()
	}
	configFile := flag.String("c", "config.yaml", "config file")
	inumber := flag.Int64("i", 1, "inumber of the inode to check")
	tx := flag.Int64("t", 1, "transaction to check")
	str := flag.Bool("s", false, "Interpret content as string")
	flag.Parse()

	var cfg config.Config

	cfgContent, err := os.ReadFile(*configFile)
	if err != nil {
		logrus.Fatalf("Could not read config file: %v", err)
	}

	yaml.Unmarshal(cfgContent, &cfg)

	////////////////////////////////////////////////////////////////

	opts := client.DefaultOptions()
	opts.Address = cfg.Immudb
	opts.Username = cfg.User
	opts.Password = cfg.Password
	opts.Database = cfg.Database

	db := stdlib.OpenDB(opts)
	defer db.Close()

	rows, err := db.QueryContext(context.TODO(), fmt.Sprintf("SELECT content FROM content BEFORE TX %d WHERE inumber = %d", *tx, *inumber))
	if err != nil {
		logrus.Fatalf("Could not execute query context: %v", err)
	}

	var content []byte

	defer rows.Close()

	found := false
	for rows.Next() {
		found = true
		err = rows.Scan(&content)
		if err != nil {
			panic(err)
		}

		if *str {
			logrus.Infof("Before TX=%d the file content was:\n%s", *tx, string(content))
		} else {
			logrus.Infof("Before TX=%d the file content was:\n%v", *tx, content)
		}
	}
	if !found {
		logrus.Infof("No entries found for file %d at TX=%d", *inumber, *tx)
	}
}
