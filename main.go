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

	"github.com/hesusruiz/domeproxy/cmd/mitm"
	"github.com/hesusruiz/domeproxy/internal/run"
	"github.com/hesusruiz/domeproxy/tmapi"
	"github.com/hesusruiz/domeproxy/tmfsync"
	"gitlab.com/greyxor/slogor"
)

// These are the hosts that we will really intercept and inspect request/replies. Any otehr host will be just forwarded transparently.
var targets = []string{
	"dome-marketplace.eu",
	"dome-marketplace-prd.eu",
	"dome-marketplace-prd.org",
	"dome-marketplace.org",
}

func main() {

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

	var _ = flag.String("addr", ":9991", "proxy address")
	caCertFile := flag.String("cacertfile", "", "certificate .pem file for trusted CA")
	caKeyFile := flag.String("cakeyfile", "", "key .pem file for trusted CA")
	flag.Parse()

	if *caCertFile == "" {
		*caCertFile = "rootCA.pem"
	}
	if *caKeyFile == "" {
		*caKeyFile = "rootCA-key.pem"
	}

	var gr run.Group

	tmf, err := tmfsync.New(tmfsync.DOME_PRO)
	if err != nil {
		log.Fatal(err)
	}
	defer tmf.Close()

	// Start a normal http server to receive intercepted requests
	httpConfig := &tmapi.Config{Listen: ":9991"}
	gr.Add(tmapi.HttpServerHandler(context.Background(), httpConfig, tmf, os.Stdout, os.Args))

	// Start a MITM server to intercept the requests to the TMF APIs
	mitmConfig := &mitm.Config{
		Listen:     ":8888",
		CaCertFile: *caCertFile,
		CaKeyFile:  *caKeyFile,
	}
	gr.Add(mitm.MITMServerHandler(context.Background(), mitmConfig, tmf, os.Stdout, os.Args))

	// The management of the interrupt signal (ctrl-c)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	gr.Add(func() error {
		<-ctx.Done()
		return fmt.Errorf("interrupt signal has been received")
	}, func(error) {
		stop()
	})

	// Wait for interrupt signal to gracefully shut down the server.
	log.Fatal(gr.Run())
}
