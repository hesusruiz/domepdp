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
	"github.com/hesusruiz/domeproxy/internal/errl"
	"github.com/hesusruiz/domeproxy/internal/run"
	"github.com/hesusruiz/domeproxy/mitm"
	"github.com/hesusruiz/domeproxy/tmfcache"
	"github.com/peterbourgon/ff/v4"
	"github.com/peterbourgon/ff/v4/ffhelp"
	"gitlab.com/greyxor/slogor"

	"github.com/hesusruiz/domeproxy/tmfproxy"
)

func main() {

	startServices(os.Args[1:])

}

func startServices(args []string) {

	rootFlags := ff.NewFlagSet("globalflags")

	// *************************************************************************************************
	// This is the main command and its flags, which are also available to the subcommands
	// *************************************************************************************************

	verbose := rootFlags.Bool('v', "verbose", "increase log verbosity")

	// PDP and general command line flags
	pdpAddress := rootFlags.String('p', "pdp", ":9991", "address of the PDP server implementing the TMForum APIs")
	debug := rootFlags.Bool('d', "debug", "run in debug mode with more logs enabled")
	nocolor := rootFlags.Bool('n', "nocolor", "disable color output for the logs to stdout")
	internal := rootFlags.Bool('i', "internal", "true if must use internal upstream hosts")
	usingBAEProxy := rootFlags.BoolDefault('b', "bae", false, "use the BAE Proxy for external access to TMForum")
	domeenvir := rootFlags.StringEnum('e', "env", "runtime environment [lcl, sbx, dev2 or pro]", "isbe", "sbx", "lcl", "dev2", "pro")

	// Man-In-The-Middle proxy flags
	enableMITM := rootFlags.BoolLong("mitmenable", "enable the Man-In-The-Middle proxy server")
	mitmAddress := rootFlags.StringLong("mitmaddress", ":8888", "address of the Man-In-The-Middle proxy server intercepting requests to/from the Marketplace")
	caCertFile := rootFlags.StringLong("mitmcacertfile", "secrets/rootCA.pem", "certificate .pem file for trusted CA for the MITM proxy")
	caKeyFile := rootFlags.StringLong("mitmcakeyfile", "secrets/rootCA-key.pem", "key .pem file for trusted CA for the MITM proxy")
	proxyPassword := rootFlags.StringLong("mitmpassword", "secrets/proxy-password.txt", "the password file for proxy authentication of the MITM proxy")

	rootCmd := &ff.Command{
		Name:  "domepdp",
		Usage: "domepdp [flags] [subcommand]",
		Flags: rootFlags,
		Exec: func(ctx context.Context, args []string) error {

			fmt.Println("running MAIN domepdp command")
			if len(args) > 0 {
				return errl.Errorf("invalid subcommand: '%s'", args[0])
			}

			logger := config.SetLogger(*debug, *nocolor)
			defer logger.Close()

			// concurrentGroup collects actors (functions) and runs them concurrently. When one actor (function) returns,
			// all actors are interrupted by calling to their stop function for a graceful shutdown.
			var concurrentGroup run.Group

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, logger)
			if err != nil {
				return errl.Error(err)
			}

			// Configure the PDP server to receive/authorize intercepted requests
			tmfRun, tmfStop, err := tmfproxy.TMFServerHandler(tmfConfig)
			if err != nil {
				return errl.Errorf("error starting TMF server: %w", err)
			}

			// Add to the monitoring group
			concurrentGroup.Add(tmfRun, tmfStop)

			// Start a debug server to manage some internal settings
			startDebugServer(logger.Level())

			// The management of the interrupt signal (ctrl-c)
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)

			concurrentGroup.Add(func() error {
				<-ctx.Done()
				return fmt.Errorf("interrupt signal has been received")
			}, func(error) {
				stop()
			})

			// If the MITM (Man-In-The-Middle) proxy is enabled, start it
			// It will intercept the requests to the TMF APIs and allow to inspect them
			// This must be used only for debugging purposes, as it will not work in production environments
			if *enableMITM {

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
					return errl.Errorf("error starting MITM server: %w", err)
				}
				concurrentGroup.Add(mitmRun, mitmStop)

			}

			// Everything is ready, start all actors and wait for interrupt signal to gracefully shut down the server.
			err = concurrentGroup.Run()
			if err != nil {
				return errl.Errorf("error running concurrent group: %w", err)
			}
			slog.Info("server stopped, shutting down gracefully")

			return nil
		},
	}

	// *************************************************************************************************
	// sync command, to synchronize only once
	// *************************************************************************************************

	syncFlags := ff.NewFlagSet("sync").SetParent(rootFlags)

	var fressness = syncFlags.Int('f', "freshness", 3600, "refresh time in seconds, to update all objects older than this time")
	var delete = syncFlags.BoolLong("delete", "delete the database before performing a new synchronization")
	var resources = syncFlags.StringList('r', "resource", "TMForum resource type to synchronize. Can be repeated to specify more than one")

	syncCmd := &ff.Command{
		Name:      "sync",
		Usage:     "domepdp [globalflags] sync [-f DURATION] [--delete] [-r RESOURCE1 [-r RESOURCE2]]",
		ShortHelp: "perform a one-time syncronization of the TMForum objects into the local database",
		Flags:     syncFlags,
		Exec: func(ctx context.Context, args []string) error {

			logger := config.SetLogger(*debug, *nocolor)
			defer logger.Close()
			if *verbose {
				tmfcache.Verbose = true
			}

			if len(args) > 0 {
				return errl.Errorf("invalid subcommand: '%s'", args[0])
			}

			if *delete {
				slog.Info("deleting database")
			}

			if len(*resources) > 0 {
				fmt.Printf("%d resources to sync: %s\n", len(*resources), *resources)
			} else {
				slog.Info("synchronizing ALL resources")
			}

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, logger)
			if err != nil {
				slog.Error("loading configuration", slogor.Err(err))
				os.Exit(1)
			}

			// Make sure to close the database associated to the log
			defer tmfConfig.LogHandler.Close()

			tmf, err := tmfcache.NewTMFCache(tmfConfig, *delete)
			if err != nil {
				slog.Error("error calling NewTMFCache", slogor.Err(err))
				os.Exit(1)
			}
			defer tmf.Close()

			if *fressness > 0 {
				tmf.Maxfreshness = *fressness
			}

			tmf.Dump = false

			tmf.MustFixInBackend = tmfcache.FixHigh

			visitedObjects := make(map[string]bool)
			if len(*resources) > 0 {

				if strings.HasPrefix((*resources)[0], "urn:") {
					object := (*resources)[0]

					tmf.Dump = false

					_, err = tmf.CloneRemoteObject(nil, object, visitedObjects)
					if err != nil {
						slog.Error("error calling CloneRemoteObject", slogor.Err(err))
						os.Exit(1)
					}

				} else {

					_, visitedObjects, err = tmf.CloneRemoteResources(*resources)

				}

			} else {
				_, visitedObjects, err = tmf.CloneAllRemoteBAEResources()
			}
			if err != nil {
				slog.Error("error calling CloneRemoteResource", slogor.Err(err))
				os.Exit(1)
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

	// *************************************************************************************************
	// get command, to retrieve one or more individual objects
	// *************************************************************************************************

	getFlags := ff.NewFlagSet("get")

	getCmd := &ff.Command{
		Name:      "get",
		Usage:     "domepdp get TMF_ID",
		ShortHelp: "retrieve a single object by its ID",
		Flags:     getFlags.SetParent(rootFlags),
		Exec: func(ctx context.Context, args []string) error {
			if *verbose {
				fmt.Fprintf(os.Stderr, "get: nargs=%d\n", len(args))
			}

			logger := config.SetLogger(*debug, *nocolor)
			defer logger.Close()

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, logger)
			if err != nil {
				slog.Error("error loading configuration", slogor.Err(err))
				os.Exit(1)
			}

			// Make sure to close the database associated to the log
			defer tmfConfig.LogHandler.Close()

			tmf, err := tmfcache.NewTMFCache(tmfConfig, false)
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

				po, local, err := tmf.RetrieveOrUpdateObject(nil, arg, "", "", "", tmfcache.LocalOrRemote)
				if err != nil {
					fmt.Println("error:", err.Error())
					continue
				}
				if !local {
					fmt.Println("object retrieved remotely:", arg)
				} else {
					fmt.Println("object retrieved locally:", arg)
				}
				out, err := json.MarshalIndent(po.GetContentAsMap(), "", "   ")
				if err != nil {
					panic(err)
				}
				fmt.Println("Object", arg)
				fmt.Println(string(out))

			}
			return nil
		},
	}
	rootCmd.Subcommands = append(rootCmd.Subcommands, getCmd)

	// *************************************************************************************************
	// fix command, to try to fix an object or colletion of objects
	// *************************************************************************************************

	fixFlags := ff.NewFlagSet("fix")
	var deleteFix = fixFlags.BoolLong("delete", "delete the database before performing a new synchronization")

	fixCmd := &ff.Command{
		Name:      "fix",
		Usage:     "domepdp fix RESOURCE [RESOURCE...]",
		ShortHelp: "try to fix a single object by its ID",
		Flags:     fixFlags.SetParent(rootFlags),
		Exec: func(ctx context.Context, resources []string) error {
			if *verbose {
				fmt.Fprintf(os.Stderr, "fix: nargs=%d\n", len(resources))
			}

			if len(resources) == 0 {
				return fmt.Errorf("no resource specified")
			}

			logger := config.SetLogger(*debug, *nocolor)
			defer logger.Close()

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, logger)
			if err != nil {
				slog.Error("error loading configuration", slogor.Err(err))
				os.Exit(1)
			}

			// Make sure to close the database associated to the log
			defer tmfConfig.LogHandler.Close()

			cache, err := tmfcache.NewTMFCache(tmfConfig, *deleteFix)
			if err != nil {
				log.Fatal(err)
				fmt.Println("error calling NewTMFCache", err.Error())
				os.Exit(1)
			}
			defer cache.Close()

			if *fressness > 0 {
				cache.Maxfreshness = *fressness
			}

			cache.MustFixInBackend = tmfcache.FixNone

			for _, resource := range resources {
				if len(resource) == 0 {
					continue
				}

				visitedObjects := make(map[string]bool)

				oList, err := cache.CloneRemoteResource(resource, visitedObjects)
				if err != nil {
					fmt.Println("error:", err.Error())
					continue
				}

				fmt.Println("############################################")
				fmt.Println("Number of", resource, "objects:", len(oList))
				fmt.Println("############################################")
				for _, pepe := range oList {

					org := &tmfcache.TMFOrganization{}
					err := org.FromMap(pepe.GetContentAsMap())
					if err != nil {
						panic(err)
					}

					// id, _ := org.GetIDMID()
					// org.SetOrganizationIdentification(id)

					fmt.Println(org)
				}

				// out, err := json.MarshalIndent(oList.ContentAsMap, "", "   ")
				// if err != nil {
				// 	panic(err)
				// }
				// fmt.Println("Object", resource)
				// fmt.Println(string(out))

			}
			return nil
		},
	}
	rootCmd.Subcommands = append(rootCmd.Subcommands, fixCmd)

	// *************************************************************************************************
	// dump command, to retrieve one or more individual objects
	// *************************************************************************************************

	dumpFlags := ff.NewFlagSet("dump")

	dumpCmd := &ff.Command{
		Name:      "dump",
		Usage:     "domepdp dump TMF_ID",
		ShortHelp: "retrieve a local object by its ID and display it",
		Flags:     dumpFlags.SetParent(rootFlags),
		Exec: func(ctx context.Context, args []string) error {
			if *verbose {
				fmt.Fprintf(os.Stderr, "dump: nargs=%d\n", len(args))
			}

			logger := config.SetLogger(*debug, *nocolor)
			defer logger.Close()

			tmfConfig, err := config.LoadConfig(*domeenvir, *pdpAddress, *internal, *usingBAEProxy, *debug, logger)
			if err != nil {
				slog.Error("error loading configuration", slogor.Err(err))
				os.Exit(1)
			}

			// Make sure to close the database associated to the log
			defer tmfConfig.LogHandler.Close()

			tmf, err := tmfcache.NewTMFCache(tmfConfig, false)
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

				visitedObjects := make(map[string]bool)
				visitedStack := tmfcache.Stack{}
				tmf.Dump = true

				_, visitedStack, err := tmf.LocalProductOfferings(nil, arg, visitedObjects, visitedStack)
				// _, visitedStack, err := tmf.VisitRemoteObject(nil, arg, visitedObjects, visitedStack)
				if err != nil {
					return err
				}
				// for _, oo := range visitedStack {
				// 	fmt.Println(oo.OrigHref, "-->", oo.DestHref)
				// }

			}
			return nil
		},
	}
	rootCmd.Subcommands = append(rootCmd.Subcommands, dumpCmd)

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
