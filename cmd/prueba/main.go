package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/hesusruiz/domeproxy/tmfcache"
	"gitlab.com/greyxor/slogor"
)

func main() {

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelInfo), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var o *tmfcache.TMFObject

	err := o.FromMap(
		map[string]any{
			"ID": "urn:ngsi-ld:ProductOffering",
		},
	)
	if err != nil {
		fmt.Println(err.Error())
	}

}
