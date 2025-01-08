package tmfsync

import (
	"log"
	"os"

	"github.com/hesusruiz/domeproxy/constants"
	"gopkg.in/yaml.v3"
)

const DEV2_DOMEServer = "https://dome-marketplace-dev2.org"
const PRO_DOMEServer = "https://dome-marketplace.eu"

const DEV2_dbname = "./tmf-dev2.db"
const PRO_dbname = "./tmf.db"

type Config struct {
	environment constants.Environment
	CaCertFile  string
	CaKeyFile   string

	HostTargets []string
	domeServer  string
	dbname      string
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

// These are the hosts that we will really intercept and inspect request/replies. Any other host will be just forwarded transparently.
var PROHostTargets = []string{
	"dome-marketplace.eu",
	"dome-marketplace-prd.eu",
	"dome-marketplace-prd.org",
	"dome-marketplace.org",
}

var DEV2HostTargets = []string{
	"dome-marketplace-dev2.org",
}

func DefaultConfig(where constants.Environment) *Config {
	conf := &Config{environment: where}

	if where == constants.DOME_DEV2 {

		conf.HostTargets = DEV2HostTargets
		conf.domeServer = DEV2_DOMEServer
		conf.dbname = DEV2_dbname

	} else if where == constants.DOME_PRO {

		conf.HostTargets = PROHostTargets
		conf.domeServer = PRO_DOMEServer
		conf.dbname = PRO_dbname

	} else {

		panic("unknown DOME environment")
	}

	return conf
}
