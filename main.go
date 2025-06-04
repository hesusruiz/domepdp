// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/hesusruiz/domeproxy/tmfcache"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"

	"github.com/hesusruiz/domeproxy/tmfproxy"
)

func main() {

	startServices(os.Args[1:])

}

func startServices(args []string) {

	rootFlags := ff.NewFlagSet("globalflags")

	verbose := rootFlags.Bool('v', "verbose", "increase log verbosity")

	// PDP and general command line flags
	pdpAddress := rootFlags.String('p', "pdp", ":9991", "address of the PDP server implementing the TMForum APIs")
	debug := rootFlags.Bool('d', "debug", "run in debug mode with more logs enabled")
	nocolor := rootFlags.Bool('n', "nocolor", "disable color output for the logs to stdout")
	internal := rootFlags.Bool('i', "internal", "true if must use internal upstream hosts")
	usingBAEProxy := rootFlags.BoolDefault('b', "bae", false, "use the BAE Proxy for external access to TMForum")
	domeenvir := rootFlags.StringEnum('e', "env", "runtime environment [lcl, sbx, dev2 or pro]", "sbx", "lcl", "dev2", "pro")

	// Man-In-The-Middle proxy flags
	enableMITM := rootFlags.BoolLong("mitmenable", "enable the Man-In-The-Middle proxy server")
	mitmAddress := rootFlags.StringLong("mitmaddress", ":8888", "address of the Man-In-The-Middle proxy server intercepting requests to/from the Marketplace")
	caCertFile := rootFlags.StringLong("mitmcacertfile", "secrets/rootCA.pem", "certificate .pem file for trusted CA for the MITM proxy")
	caKeyFile := rootFlags.StringLong("mitmcakeyfile", "secrets/rootCA-key.pem", "key .pem file for trusted CA for the MITM proxy")
	proxyPassword := rootFlags.StringLong("mitmpassword", "secrets/proxy-password.txt", "the password file for proxy authentication of the MITM proxy")

	rootCmd := &ff.Command{
		Name:  "domepdp",
		Usage: "domepdp [FLAGS] [SUBCOMMAND]",
		Flags: rootFlags,
		Exec: func(ctx context.Context, args []string) error {

			fmt.Println("running MAIN domepdp command")
			if len(args) > 0 {
				return fmt.Errorf("invalid subcommand: '%s'", args[0])
			}

			config.SetLogger(*debug, *nocolor)

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

	syncCmd := &ff.Command{
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

			tmf, err := tmfcache.NewTMFCache(tmfConfig)
			if err != nil {
				log.Fatal(err)
				fmt.Println("error calling NewTMFCache", err.Error())
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

			var differentTypes = make(map[string]int)

			fmt.Println("Visited objects:")
			for id := range visitedObjects {
				parts := strings.Split(id, ":")
				count := differentTypes[parts[2]]
				count++
				differentTypes[parts[2]] = count
				fmt.Println(id)
			}
			fmt.Println("############################################")

			fmt.Println("Total objects:", len(visitedObjects))
			fmt.Println("Different types:")
			for t, count := range differentTypes {
				fmt.Println(t, count)
			}

			return nil
		},
	}
	rootCmd.Subcommands = append(rootCmd.Subcommands, syncCmd)

	countCmd := &ff.Command{
		Name:      "get",
		Usage:     "domepdp get TMF_ID",
		ShortHelp: "retrieve a single object by its ID",
		Flags:     ff.NewFlagSet("get").SetParent(rootFlags),
		Exec: func(ctx context.Context, args []string) error {
			if *verbose {
				fmt.Fprintf(os.Stderr, "get: nargs=%d\n", len(args))
			}

			logger := config.SetLogger(*debug, *nocolor)
			defer logger.Close()

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, *nocolor)
			if err != nil {
				panic(err)
			}

			// Make sure to close the database associated to the log
			defer tmfConfig.LogHandler.Close()

			tmf, err := tmfcache.NewTMFCache(tmfConfig)
			if err != nil {
				log.Fatal(err)
				fmt.Println("error calling NewTMFCache", err.Error())
				os.Exit(-1)
			}
			defer tmf.Close()

			if *fressness > 0 {
				tmf.Maxfreshness = *fressness
			}

			for _, arg := range args {
				if len(arg) == 0 {
					continue
				}

				po, found, err := tmf.LocalRetrieveTMFObject(nil, arg, "")
				if err != nil {
					panic(err)
				}
				if !found {
					fmt.Println("object not found:", arg)
					continue
				}
				out, err := json.MarshalIndent(po.ContentAsMap, "", "   ")
				if err != nil {
					panic(err)
				}
				fmt.Println("Object", arg)
				fmt.Println(string(out))

			}
			return nil
		},
	}
	rootCmd.Subcommands = append(rootCmd.Subcommands, countCmd)

	// Parse the arguments and flags and select the proper command to execute
	if err := rootCmd.Parse(args, ff.WithEnvVarPrefix("PDP")); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", ffhelp.Command(rootCmd))

		if errors.Is(err, ff.ErrHelp) {
			fmt.Println("HELP is requested")
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(1)
		}

	}

	// At this moment, the flags have the values either from the environment or from the command line
	if err := rootCmd.Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", ffhelp.Command(rootCmd))

		if errors.Is(err, ff.ErrHelp) {
			fmt.Println("HELP is requested")
			os.Exit(0)
		} else {
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
			os.Exit(1)
		}

	}

	os.Exit(0)
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
