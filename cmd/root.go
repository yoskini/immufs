package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"immufs/pkg/config"
	"immufs/pkg/fs"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	flagConfig     = "config"
	flagServerAddr = "immudb-addr"
	flagUser       = "user"
	flagPassword   = "password"
	flagDatabase   = "database"
	flagMountpoint = "mountpoint"
	flagLogFile    = "logfile"
	flagUid        = "uid"
	flagGid        = "gid"
)

var (
	cfgFile string
	cfg     config.Config
	rootCmd = &cobra.Command{
		Use:   "immufs",
		Short: "immufs",
		Long:  `fuse filesystem backed with immudb`,
		Run: func(cmd *cobra.Command, args []string) {
			// Main program entry point
			readFlags(cmd.PersistentFlags())
			logger := logrus.New()

			logger.Infof("%+v", cfg)
			// Adjust the logger
			if cfg.LogFile != "" {
				if fh, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_APPEND, 0644); err != nil {
					logger.Fatalf("could not open log file %s: %s", cfg.LogFile, err)
				} else {
					logger.SetOutput(fh)
					defer fh.Close()
				}
			}

			// Mount the filesystem
			immufs, err := fs.NewImmufs(context.Background(), &cfg, logger)
			if err != nil {
				logger.Fatalf("failed to build Immufs: %s", err)
			}
			server := fuseutil.NewFileSystemServer(immufs)
			mountCfg := &fuse.MountConfig{
				FSName: "immufs",
			}
			mfs, err := fuse.Mount(cfg.Mountpoint, server, mountCfg)
			if err != nil {
				logger.Fatalf("could not mount immufs: %s", err)
			}
			logger.Info("immufs mounted")

			// Handle ctrl-c
			c := make(chan os.Signal)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			//go func() {
			func() {
				<-c
				// Unmount fs
				select {
				case <-time.After(time.Second * 3):
					logger.Fatalf("could not Join immufs for unmounting: %s. Remember to run umount immufs manually.", err)
				default:
					fuse.Unmount(cfg.Mountpoint)
					err := mfs.Join(context.Background())
					if err != nil {
						logger.Fatalf("could not Join immufs for unmounting: %s", err)
					}
					logger.Info("immufs unmounted")
					os.Exit(1)
				}
			}()
		},
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVarP(&cfgFile, flagConfig, "c", "config.yaml", "config file")
	rootCmd.PersistentFlags().StringP(flagServerAddr, "s", "127.0.0.1:3322", "immudb server address")
	rootCmd.PersistentFlags().StringP(flagUser, "u", "immudb", "immudb user")
	rootCmd.PersistentFlags().StringP(flagPassword, "p", "immudb", "immudb password")
	rootCmd.PersistentFlags().StringP(flagDatabase, "d", "defaultdb", "immudb database name")
	rootCmd.PersistentFlags().StringP(flagMountpoint, "m", "", "mountpoint")
	rootCmd.PersistentFlags().StringP(flagLogFile, "f", "", "logfile")
	rootCmd.PersistentFlags().Int32P(flagUid, "i", int32(os.Getuid()), "uid to use when mounting immufs")
	rootCmd.PersistentFlags().Int32P(flagGid, "g", int32(os.Getgid()), "gid to use when mounting immufs")

	// Bind all flags
	err := viper.BindPFlags(rootCmd.PersistentFlags())
	if err != nil {
		logrus.Fatal(err)
	}
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigType("yaml")
		viper.SetConfigName("config.yaml")
	}

	if err := viper.ReadInConfig(); err == nil {
		logrus.Infoln("Using config file:", viper.ConfigFileUsed())
	}
}

// Move pflags into the config structure that will be passed to the application
func readFlags(flag *pflag.FlagSet) {
	cfg.Immudb = viper.GetString(flagServerAddr)
	cfg.User = viper.GetString(flagUser)
	cfg.Password = viper.GetString(flagPassword)
	cfg.Database = viper.GetString(flagDatabase)
	cfg.Mountpoint = viper.GetString(flagMountpoint)
	cfg.LogFile = viper.GetString(flagLogFile)
	cfg.Uid = viper.GetUint32(flagUid)
	cfg.Gid = viper.GetUint32(flagGid)
}
