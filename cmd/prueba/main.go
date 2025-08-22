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

	_, err := tmfcache.TMFObjectFromMap(
		map[string]any{
			"id": "urn:ngsi-ld:ProductOffering",
		},
		"productOffering",
	)
	if err != nil {
		fmt.Println(err.Error())
	}

}
