package config

import (
	"go/build"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type TASCConfig struct {
	StorageType string `yaml:"storageType"`
	AnnaELB     string `yaml:"annaELB"`
	IpAddress   string `yaml:"privateIP"`
	PublicIP    string `yaml:"publicIP"`
	RoutingILB  string `yaml:"routingILB"`
	MonitorIP   string `yaml:"monitorIP"`
}

func ParseConfig() *TASCConfig {
	home := os.Getenv("GOPATH")
	if home == "" {
		home = build.Default.GOPATH
	}
	confPath := filepath.Join(home, "src", "github.com", "saurav-c", "tasc", "config", "tasc-config.yml")
	bts, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatal("Unable to read config file.\n")
		os.Exit(1)
	}
	var config TASCConfig
	err = yaml.Unmarshal(bts, &config)
	if err != nil {
		log.Fatal("Unable to correctly parse yaml file.\n")
		os.Exit(1)
	}
	return &config
}
