package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/constants"
	"github.com/hesusruiz/domeproxy/tmfsync"
	"gitlab.com/greyxor/slogor"
)

func main() {

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

	var err error

	var refreshTime = flag.Int("refresh", 3600, "refresh time in seconds, to update all objects older than this time")
	var dump = flag.String("dump", "", "display an object by identifier")
	var delete = flag.Bool("delete", false, "delete the database before performing a new synchronization")
	var production = flag.Bool("production", false, "operate in PRODUCTION. Otherwise, use DEV2")

	flag.Parse()

	var server constants.Environment
	if *production {
		server = constants.DOME_PRO
		if *delete {
			os.Remove(tmfsync.PRO_dbname)
			os.Remove(tmfsync.PRO_dbname + "-shm")
			os.Remove(tmfsync.PRO_dbname + "-wal")
		}
	} else {
		server = constants.DOME_DEV2
		if *delete {
			os.Remove(tmfsync.DEV2_dbname)
			os.Remove(tmfsync.DEV2_dbname + "-shm")
			os.Remove(tmfsync.DEV2_dbname + "-wal")
		}
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	tmfConfig := tmfsync.DefaultConfig(server)

	tmf, err := tmfsync.New(tmfConfig)
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
	_, visitedObjects, err := tmf.CloneRemoteProductOfferings()
	if err != nil {
		panic(err)
	}

	// Write some stats
	fmt.Println("############################################")

	var differentTypes = make(map[string]bool)

	fmt.Println("Visited objects:")
	for id := range visitedObjects {
		parts := strings.Split(id, ":")
		differentTypes[parts[2]] = true
		fmt.Println(id)
	}
	fmt.Println("############################################")

	fmt.Println("Different types:")
	for t := range differentTypes {
		fmt.Println(t)
	}

	// Retrieve the product offerings
	_, visitedObjects, err = tmf.CloneRemoteCatalogues()
	if err != nil {
		panic(err)
	}

	// Write some stats
	fmt.Println("############################################")

	differentTypes = make(map[string]bool)

	fmt.Println("Visited objects:")
	for id := range visitedObjects {
		parts := strings.Split(id, ":")
		differentTypes[parts[2]] = true
		fmt.Println(id)
	}
	fmt.Println("############################################")

	fmt.Println("Different types:")
	for t := range differentTypes {
		fmt.Println(t)
	}

	fmt.Println("Refreshed objects", tmf.RefreshCounter)

}
