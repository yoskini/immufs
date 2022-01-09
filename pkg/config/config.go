package config

type Config struct {
	Immudb     string `yaml:"immudb"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	Database   string `yaml:"database"`
	Mountpoint string `yaml:"mountpoint"`
	LogFile    string `yaml:"logfile"`
	Uid        uint32 `yaml:"uid"`
	Gid        uint32 `yaml:"gid"`
}
