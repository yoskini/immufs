package config

type Config struct {
	Immudb     string `yaml:"immudb"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	Mountpoint string `yaml:"mountpoint"`
	LogFile    string `yaml:"logFile"`
}
