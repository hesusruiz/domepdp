package mitm

import (
	"log"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Listen is the address to listen on, e.g. ":443".
	Listen string

	CaCertFile    string
	CaKeyFile     string
	ProxyPassword string

	PDPAddress string

	HostTargets []string
}

func NewConfig(environment string, mitmAddress string, caCertFile string, caKeyFile string, proxyPassword string, pdpServer string) *Config {

	c := &Config{
		Listen:        mitmAddress,
		CaCertFile:    caCertFile,
		CaKeyFile:     caKeyFile,
		ProxyPassword: proxyPassword,
		PDPAddress:    pdpServer,
	}

	switch environment {
	case "DOME_PRO":
		c.HostTargets = []string{
			"dome-marketplace.eu",
			"dome-marketplace-prd.eu",
			"dome-marketplace-prd.org",
			"dome-marketplace.org",
		}
	case "DOME_DEV2":
		c.HostTargets = []string{
			"dome-marketplace-dev2.org",
		}
	case "DOME_SBX":
		c.HostTargets = []string{
			"dome-marketplace-dev2.org",
		}
	case "DOME_LCL":
		c.HostTargets = []string{
			"dome-marketplace-lcl.org",
		}
	default:
		slog.Error("unknown environment", "env", environment)
		panic("unknown DOME environment")
	}

	return c
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
