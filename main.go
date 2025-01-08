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
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/hesusruiz/domeproxy/constants"
	"github.com/hesusruiz/domeproxy/internal/run"
	"github.com/hesusruiz/domeproxy/mitm"

	"github.com/hesusruiz/domeproxy/tmapi"
	"gitlab.com/greyxor/slogor"
)

func main() {

	pdpAddress := flag.String("pdp", ":9991", "address of the PDP server implementing the TMForum APIs")
	proxyAddress := flag.String("proxy", ":8888", "address of the PROXY server intercepting requests to/from the Marketplace")
	caCertFile := flag.String("cacertfile", "secrets/rootCA.pem", "certificate .pem file for trusted CA")
	caKeyFile := flag.String("cakeyfile", "secrets/rootCA-key.pem", "key .pem file for trusted CA")
	prod := flag.Bool("pro", false, "use the PRODUCTION environment")
	debug := flag.Bool("debug", false, "run in debug mode with more logs enabled")

	flag.Parse()

	logLevel := new(slog.LevelVar)

	slogor.SetLevel(logLevel)

	if *debug {
		logLevel.Set(slog.LevelDebug)
	}

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(logLevel), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

	http.HandleFunc("/debug/logson", func(w http.ResponseWriter, r *http.Request) {
		logLevel.Set(slog.LevelDebug)
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/debug/logsoff", func(w http.ResponseWriter, r *http.Request) {
		logLevel.Set(slog.LevelInfo)
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		ln, err := net.Listen("tcp", "localhost:")
		if err != nil {
			slog.Error("failed to start debug server", "err", err)
		} else {
			slog.Info("debug server listening", "addr", ln.Addr())
			err := http.Serve(ln, nil)
			slog.Error("debug server exited", "err", err)
		}
	}()

	var environment = constants.DOME_DEV2
	if *prod {
		environment = constants.DOME_PRO
		fmt.Println("Using the PRODUCTION environment")
	} else {
		fmt.Println("Using the DEV2 environment")
	}

	// Group collects actors (functions) and runs them concurrently. When one actor (function) returns, all actors are interrupted.
	var gr run.Group

	// Configure the PDP server to receive/authorize intercepted requests
	tmfConfig, execute, interrupt, err := tmapi.HttpServerHandler(environment, *pdpAddress, *debug)
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
