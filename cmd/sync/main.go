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
	"github.com/hesusruiz/domeproxy/pdp"
	"gitlab.com/greyxor/slogor"
)

func main() {

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

	var err error

	var refreshTime = flag.Int("refresh", 3600, "refresh time in seconds, to update all objects older than this time")
	var dump = flag.String("dump", "", "display an object by identifier")
	var delete = flag.Bool("delete", false, "delete the database before performing a new synchronization")
	var envir = flag.String("env", "lcl", "environment, one of lcl, dev2 or pro.")

	flag.Parse()

	var server pdp.Environment

	switch *envir {
	case "pro":
		server = pdp.DOME_PRO
		if *delete {
			os.Remove(pdp.PRO_dbname)
			os.Remove(pdp.PRO_dbname + "-shm")
			os.Remove(pdp.PRO_dbname + "-wal")
		}
	case "dev2":
		server = pdp.DOME_DEV2
		if *delete {
			os.Remove(pdp.DEV2_dbname)
			os.Remove(pdp.DEV2_dbname + "-shm")
			os.Remove(pdp.DEV2_dbname + "-wal")
		}
	case "lcl":
		server = pdp.DOME_LCL
		if *delete {
			os.Remove(pdp.LCL_dbname)
			os.Remove(pdp.LCL_dbname + "-shm")
			os.Remove(pdp.LCL_dbname + "-wal")
		}
	default:
		fmt.Printf("unknown environment: %v. Must be one of lcl, dev2 or pro\n", *envir)
		os.Exit(1)
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	tmfConfig := pdp.DefaultConfig(server)

	tmf, err := pdp.New(tmfConfig)
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

	fmt.Println("Cloning", *envir)

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
