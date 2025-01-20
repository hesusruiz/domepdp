// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Environment int

const DOME_PRO Environment = 0
const DOME_DEV2 Environment = 1

const DEV2_DOMEServer = "https://dome-marketplace-dev2.org"
const PRO_DOMEServer = "https://dome-marketplace.eu"

const DEV2_dbname = "./tmf-dev2.db"
const PRO_dbname = "./tmf.db"

type Config struct {
	Environment Environment
	RootDir     string
	CaCertFile  string
	CaKeyFile   string

	HostTargets []string
	DomeServer  string
	Dbname      string
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

func DefaultConfig(where Environment) *Config {
	conf := &Config{Environment: where}

	if where == DOME_DEV2 {

		conf.HostTargets = DEV2HostTargets
		conf.DomeServer = DEV2_DOMEServer
		conf.Dbname = DEV2_dbname

	} else if where == DOME_PRO {

		conf.HostTargets = PROHostTargets
		conf.DomeServer = PRO_DOMEServer
		conf.Dbname = PRO_dbname

	} else {

		panic("unknown DOME environment")
	}

	return conf
}
