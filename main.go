// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/run"
	"github.com/hesusruiz/domeproxy/mitm"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

	"github.com/hesusruiz/domeproxy/tmfproxy"
)

func main() {

	processArgs()

	// PDP and general command line flags
	pdpAddress := flag.String("pdp", ":9991", "address of the PDP server implementing the TMForum APIs")
	debug := flag.Bool("debug", false, "run in debug mode with more logs enabled")
	nocolor := flag.Bool("nocolor", false, "disable color output for the logs to stdout")
	internal := flag.Bool("internal", false, "true if must use internal upstream hosts")
	usingBAEProxy := flag.Bool("bae", true, "use the BAE Proxy for external access to TMForum")
	domeenvir := flag.String("env", "sbx", "environment, one of lcl, sbx, dev2 or pro.")

	// Man-In-The-Middle proxy flags
	enableMITM := flag.Bool("enablemitm", false, "enable the Man-In-The-Middle proxy server")
	mitmAddress := flag.String("mitm", ":8888", "address of the Man-In-The-Middle proxy server intercepting requests to/from the Marketplace")
	caCertFile := flag.String("cacertfile", "secrets/rootCA.pem", "certificate .pem file for trusted CA for the MITM proxy")
	caKeyFile := flag.String("cakeyfile", "secrets/rootCA-key.pem", "key .pem file for trusted CA for the MITM proxy")
	proxyPassword := flag.String("password", "secrets/proxy-password.txt", "the password file for proxy authentication of the MITM proxy")

	flag.Parse()

	// concurrentGroup collects actors (functions) and runs them concurrently. When one actor (function) returns,
	// all actors are interrupted by calling to their stop function for a graceful shutdown.
	var concurrentGroup run.Group

	tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, *nocolor)
	if err != nil {
		panic(err)
	}

	// Make sure to close the database associated to the log
	defer tmfConfig.LogHandler.Close()

	// Configure the PDP server to receive/authorize intercepted requests
	tmfRun, tmfStop, err := tmfproxy.TMFServerHandler(tmfConfig)
	if err != nil {
		panic(err)
	}

	// Add to the monitoring group
	concurrentGroup.Add(tmfRun, tmfStop)

	if *enableMITM {

		// Start a MITM server to intercept the requests to the TMF APIs
		mitmConfig := mitm.NewConfig(
			*domeenvir,
			*mitmAddress,
			*caCertFile,
			*caKeyFile,
			*proxyPassword,
			*pdpAddress,
		)

		mitmRun, mitmStop, err := mitm.MITMServerHandler(mitmConfig)
		if err != nil {
			panic(err)
		}
		concurrentGroup.Add(mitmRun, mitmStop)

	}

	startDebugServer(tmfConfig.LogLevel)

	// The management of the interrupt signal (ctrl-c)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	concurrentGroup.Add(func() error {
		<-ctx.Done()
		return fmt.Errorf("interrupt signal has been received")
	}, func(error) {
		stop()
	})

	// Start all actors and wait for interrupt signal to gracefully shut down the server.
	err = concurrentGroup.Run()
	if err != nil {
		slog.Error("error running concurrent group", "err", err)
	}
	slog.Info("shutting down gracefully")
}

// startDebugServer allows remote setting of the log level
func startDebugServer(logLevel *slog.LevelVar) {
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
}

func processArgs() {

	rootFlags := ff.NewFlagSet("globalflags")
	verbose := rootFlags.Bool('v', "verbose", "increase log verbosity")

	pdpAddress := rootFlags.String('p', "pdp", ":9991", "address of the PDP server implementing the TMForum APIs")
	debug := rootFlags.Bool('d', "debug", "run in debug mode with more logs enabled")
	nocolor := rootFlags.Bool('n', "nocolor", "disable color output for the logs to stdout")
	internal := rootFlags.Bool('i', "internal", "true if must use internal upstream hosts")
	usingBAEProxy := rootFlags.BoolDefault('b', "bae", true, "use the BAE Proxy for external access to TMForum")
	domeenvir := rootFlags.StringEnum('e', "env", "runtime environment [lcl, sbx, dev2 or pro]", "sbx", "lcl", "dev2", "pro")

	rootCmd := &ff.Command{
		Name:  "domepdp",
		Usage: "domepdp [FLAGS] [SUBCOMMAND]",
		Flags: rootFlags,
		Exec: func(ctx context.Context, args []string) error {

			config.SetLogger(*debug, *nocolor)

			fmt.Println("running MAIN domepdp command")
			if len(args) > 0 {
				fmt.Println("invalid subcommand:", args[0])
				os.Exit(-1)
			}

			// concurrentGroup collects actors (functions) and runs them concurrently. When one actor (function) returns,
			// all actors are interrupted by calling to their stop function for a graceful shutdown.
			var concurrentGroup run.Group

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, *nocolor)
			if err != nil {
				panic(err)
			}

			// Make sure to close the database associated to the log
			defer tmfConfig.LogHandler.Close()

			// Configure the PDP server to receive/authorize intercepted requests
			tmfRun, tmfStop, err := tmfproxy.TMFServerHandler(tmfConfig)
			if err != nil {
				panic(err)
			}

			// Add to the monitoring group
			concurrentGroup.Add(tmfRun, tmfStop)

			startDebugServer(tmfConfig.LogLevel)

			// The management of the interrupt signal (ctrl-c)
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
			concurrentGroup.Add(func() error {
				<-ctx.Done()
				return fmt.Errorf("interrupt signal has been received")
			}, func(error) {
				stop()
			})

			// Start all actors and wait for interrupt signal to gracefully shut down the server.
			err = concurrentGroup.Run()
			if err != nil {
				slog.Error("error running concurrent group", "err", err)
			}
			slog.Info("shutting down gracefully")

			return nil
		},
	}

	syncFlags := ff.NewFlagSet("sync").SetParent(rootFlags)

	var fressness = syncFlags.Int('f', "freshness", 3600, "refresh time in seconds, to update all objects older than this time")
	var delete = syncFlags.BoolLong("delete", "delete the database before performing a new synchronization")
	var resources = syncFlags.StringList('r', "resources", "TMForum resource type to synchronize. Can be repeated to specify more than one")

	repeatCmd := &ff.Command{
		Name:      "sync",
		Usage:     "domepdp [GLOBALFLAGS] sync [-f DURATION] [--delete] [-r RESOURCE1 [-r RESOURCE2]]",
		ShortHelp: "perform a one-time syncronization of the TMForum objects into the local database",
		Flags:     syncFlags,
		Exec: func(ctx context.Context, args []string) error { // defining Exec inline allows it to access the e.g. verbose flag, above

			logger := config.SetLogger(*debug, *nocolor)
			defer logger.Close()

			if *delete {
				slog.Info("deleting database")
			}

			if len(*resources) > 0 {
				fmt.Printf("%d resources to sync: %s\n", len(*resources), *resources)
			} else {
				slog.Info("synchronizing ALL resources")
			}

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, *nocolor)
			if err != nil {
				panic(err)
			}

			// Make sure to close the database associated to the log
			defer tmfConfig.LogHandler.Close()

			tmf, err := pdp.NewTMFCache(tmfConfig)
			if err != nil {
				log.Fatal(err)
				fmt.Println("error calling NewTMFdb", err.Error())
				os.Exit(-1)
			}
			defer tmf.Close()

			if *fressness > 0 {
				tmf.Maxfreshness = *fressness
			}

			// Retrieve the product offerings
			_, visitedObjects, err := tmf.CloneAllRemoteBAEResources()
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

			return nil
		},
	}
	rootCmd.Subcommands = append(rootCmd.Subcommands, repeatCmd)

	countCmd := &ff.Command{
		Name:      "count",
		Usage:     "textctl count [<ARG> ...]",
		ShortHelp: "count the number of bytes in the arguments",
		Flags:     ff.NewFlagSet("count").SetParent(rootFlags), // count has no flags itself, but it should still be able to parse root flags
		Exec: func(ctx context.Context, args []string) error {
			if *verbose {
				fmt.Fprintf(os.Stderr, "count: nargs=%d\n", len(args))
			}
			var count int
			for _, arg := range args {
				count += len(arg)
			}
			fmt.Fprintf(os.Stdout, "%d\n", count)
			return nil
		},
	}
	rootCmd.Subcommands = append(rootCmd.Subcommands, countCmd) // add the count command underneath the root command

	if err := rootCmd.ParseAndRun(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", ffhelp.Command(rootCmd))

		if errors.Is(err, ff.ErrHelp) {
			fmt.Println("HELP is requested")
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(-1)
		}

	}

	os.Exit(0)
}
