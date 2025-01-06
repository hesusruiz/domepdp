package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/tmfsync"
)

func main() {
	var err error

	var refreshTime = flag.Int("refresh", 3600, "refresh time in seconds, to update all objects older than this time")
	var dump = flag.String("dump", "", "display an object by identifier")
	var delete = flag.Bool("delete", false, "delete the database before performing a new synchronization")
	var production = flag.Bool("production", false, "operate in PRODUCTION. Otherwise, use DEV2")

	flag.Parse()

	var server tmfsync.Environment
	if *production {
		server = tmfsync.DOME_PRO
		if *delete {
			os.Remove(tmfsync.PRO_dbname)
			os.Remove("./tmf.db-shm")
			os.Remove("./tmf.db-wal")
		}
	} else {
		server = tmfsync.DOME_DEV2
		if *delete {
			os.Remove(tmfsync.DEV2_dbname)
			os.Remove("./tmf-dev2.db-shm")
			os.Remove("./tmf-dev2.db-wal")
		}
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	tmf, err := tmfsync.New(server)
	if err != nil {
		log.Fatal(err)
	}
	defer tmf.Close()

	if *refreshTime > 0 {
		tmf.Maxfreshness = *refreshTime
	}

	if len(*dump) > 0 {
		po, _, err := tmf.RetrieveLocalTMFObject(nil, *dump, "")
		if err != nil {
			panic(err)
		}
		out, err := json.MarshalIndent(po.ContentMap, "", "   ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(out))
		return
	}

	// Retrieve the product offerings
	_, err = tmf.CloneRemoteProductOfferings()
	if err != nil {
		panic(err)
	}

	// Retrieve the product offerings
	_, err = tmf.CloneRemoteCatalogues()
	if err != nil {
		panic(err)
	}

	fmt.Println("Refreshed objects", tmf.RefreshCounter)

}
