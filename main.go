// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

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

	"github.com/hesusruiz/domeproxy/internal/run"
	"github.com/hesusruiz/domeproxy/mitm"
	"github.com/hesusruiz/domeproxy/pdp"

	"github.com/hesusruiz/domeproxy/tmfapi"
	"gitlab.com/greyxor/slogor"
)

func main() {

	pdpAddress := flag.String("pdp", ":9991", "address of the PDP server implementing the TMForum APIs")
	mitmAddress := flag.String("mitm", ":8888", "address of the Man-In-The-Middle proxy server intercepting requests to/from the Marketplace")
	caCertFile := flag.String("cacertfile", "secrets/rootCA.pem", "certificate .pem file for trusted CA for the MITM proxy")
	caKeyFile := flag.String("cakeyfile", "secrets/rootCA-key.pem", "key .pem file for trusted CA for the MITM proxy")
	proxyPassword := flag.String("password", "secrets/proxy-password.txt", "the password file for proxy authentication of the MITM proxy")
	debug := flag.Bool("debug", false, "run in debug mode with more logs enabled")
	var envir = flag.String("env", "lcl", "environment, one of lcl, dev2 or pro.")

	flag.Parse()

	logLevel := new(slog.LevelVar)

	slogor.SetLevel(logLevel)

	if *debug {
		logLevel.Set(slog.LevelDebug)
	}

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(logLevel), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

	// Start a debug server on a random port, enabling control of log level.
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

	// By default we will operate in the DEV2 environment
	var environment = pdp.DOME_DEV2

	switch *envir {
	case "pro":
		environment = pdp.DOME_PRO
		fmt.Println("Using the PRODUCTION environment")
	case "dev2":
		environment = pdp.DOME_DEV2
		fmt.Println("Using the DEV2 environment")
	case "lcl":
		environment = pdp.DOME_LCL
		fmt.Println("Using the LCL environment")
	default:
		fmt.Printf("unknown environment: %v. Must be one of lcl, dev2 or pro\n", *envir)
		os.Exit(1)
	}

	// Group collects actors (functions) and runs them concurrently. When one actor (function) returns,
	// all actors are interrupted.
	var gr run.Group

	// Configure the PDP server to receive/authorize intercepted requests
	tmfConfig, execute, interrupt, err := tmfapi.TMFServerHandler(environment, *pdpAddress, *debug)
	if err != nil {
		panic(err)
	}

	// Add to the monitoring group
	gr.Add(execute, interrupt)

	// Start a MITM server to intercept the requests to the TMF APIs
	mitmConfig := &mitm.Config{
		Listen:        *mitmAddress,
		CaCertFile:    *caCertFile,
		CaKeyFile:     *caKeyFile,
		ProxyPassword: *proxyPassword,
		HostTargets:   tmfConfig.HostTargets,
	}
	pdpServer := "http://localhost" + *pdpAddress
	execute, interrupt, err = mitm.MITMServerHandler(mitmConfig, pdpServer)
	if err != nil {
		panic(err)
	}
	gr.Add(execute, interrupt)

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
