// Implements a tunneling forward proxy for CONNECT requests, while also
// MITM-ing the connection and dumping the HTTPs requests/responses that cross
// the tunnel.
//
// Requires a certificate/key for a CA trusted by clients in order to generate
// and sign fake TLS certificates.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
//
// (JRM) Replace panics for error handling, to avoid the proxy server exiting.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/hesusruiz/domeproxy/internal/run"
	"github.com/hesusruiz/domeproxy/mitm"
	"github.com/hesusruiz/domeproxy/tmapi"
	"github.com/hesusruiz/domeproxy/tmfsync"
	"gitlab.com/greyxor/slogor"
)

func main() {

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

	pdpAddress := flag.String("pdp", ":9991", "address of the PDP server implementing the TMForum APIs")
	proxyAddress := flag.String("proxy", ":8888", "address of the PROXY server intercepting requests to/from the Marketplace")
	caCertFile := flag.String("cacertfile", "secrets/rootCA.pem", "certificate .pem file for trusted CA")
	caKeyFile := flag.String("cakeyfile", "secrets/rootCA-key.pem", "key .pem file for trusted CA")
	prod := flag.Bool("pro", false, "use the PRODUCTION environment")
	flag.Parse()

	var environment = tmfsync.DOME_DEV2
	if *prod {
		environment = tmfsync.DOME_PRO
		fmt.Println("Using the PRODUCTION environment")
	} else {
		fmt.Println("Using the DEV2 environment")
	}

	// Group collects actors (functions) and runs them concurrently. When one actor (function) returns, all actors are interrupted.
	var gr run.Group

	// Configure the PDP server to receive/authorize intercepted requests
	tmfConfig, execute, interrupt, err := tmapi.HttpServerHandler(environment, *pdpAddress)
	if err != nil {
		panic(err)
	}

	// Add to the monitoring group
	gr.Add(execute, interrupt)

	// Start a MITM server to intercept the requests to the TMF APIs
	mitmConfig := &mitm.Config{
		Listen:      *proxyAddress,
		CaCertFile:  *caCertFile,
		CaKeyFile:   *caKeyFile,
		HostTargets: tmfConfig.HostTargets,
	}
	pdpServer := "http://localhost" + *pdpAddress
	gr.Add(mitm.MITMServerHandler(mitmConfig, pdpServer))

	// The management of the interrupt signal (ctrl-c)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	gr.Add(func() error {
		<-ctx.Done()
		return fmt.Errorf("interrupt signal has been received")
	}, func(error) {
		stop()
	})

	// Start all actors and wait for interrupt signal to gracefully shut down the server.
	log.Fatal(gr.Run())
}
