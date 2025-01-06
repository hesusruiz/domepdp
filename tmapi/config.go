package tmapi

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Listen is the address to listen on, e.g. ":443".
	Listen string
}

func LoadConfig() *Config {
	yml, err := os.ReadFile("server.yaml")
	if err != nil {
		log.Fatal("failed to read config file", "err", err)
	}
	config := &Config{}
	if err := yaml.Unmarshal(yml, config); err != nil {
		log.Fatal("failed to parse config file", "err", err)
	}

	return config
}
