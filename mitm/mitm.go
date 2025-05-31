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
package mitm

import (
	"bufio"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/hesusruiz/domeproxy/tmfproxy"
	"github.com/smarty/cproxy/v2"
	"gitlab.com/greyxor/slogor"
)

func MITMServerHandler(
	config *Config,
) (execute func() error, interrupt func(error), err error) {

	// Read the passworf file
	p, err := os.ReadFile(config.ProxyPassword)
	if err != nil {
		return nil, nil, fmt.Errorf("reading proxy password file: %w", err)
	}

	password := strings.TrimSpace(string(p))

	// This is the man-in-the-middle proxy that will intercept the requests to the TMF APIs, so they can be served locally from the
	// local database, or updated if the local copy is not fresh anymore.
	// In addition, we can test the authorization mechanism for each API
	pdpServer := "http://localhost" + config.PDPAddress
	mitmProxy := createMitmProxy(config.CaCertFile, config.CaKeyFile, pdpServer)

	// This proxy just connects the client with the server and does not look inside the requests/responses, because they are encrypted.
	// We need this for all the requests that we are not interested in, and because we can not (do not want to) tell the client browser which are the requests
	// on which we are interested.
	transparentProxy := cproxy.New(
		cproxy.Options.Logger(log.New(os.Stderr, "", log.LstdFlags)),
		cproxy.Options.Filter(NewFilter()),
		cproxy.Options.LogConnections(true),
	)

	// This HTTP handler will receive all requests and each one will be dispatched to the appropriate type of proxy.
	rh := &roothandler{
		mitmProxy:        mitmProxy,
		proxyPassword:    password,
		transparentProxy: transparentProxy,
		hostTargets:      config.HostTargets,
	}

	// The main server will accept requests without TLS, using port 80
	s := &http.Server{
		Addr:           config.Listen,
		Handler:        rh,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	return func() error {
			slog.Info("Starting MITM proxy server", "addr", s.Addr)
			return s.ListenAndServe()
		}, func(error) {
			slog.Info("Cancelling MITM proxy server")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s.Shutdown(ctx)
			fmt.Println("MITM proxy cancelled")
		},
		nil
}

// The main handler for the proxy server.

type roothandler struct {
	mitmProxy        *mitmProxy
	proxyPassword    string
	transparentProxy http.Handler
	hostTargets      []string
}

func (h *roothandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// The proxy is authenticated and we do not allow any other request to pass through
	// Check for proxy authentication in the Proxy-Authorization request header
	proxyAuthHeader := r.Header.Get("Proxy-Authorization")
	if len(proxyAuthHeader) == 0 {
		// We do not log here due to the Internet background noise ...
		w.Header().Set("Proxy-Authenticate", "Basic realm=\"Proxy Authentication Required\"")
		http.Error(w, "Proxy Authentication Required", http.StatusProxyAuthRequired)
		return
	}

	// Decode username and password from the Proxy-Authorization header
	authComponents := strings.SplitN(proxyAuthHeader, " ", 2)
	if len(authComponents) != 2 || authComponents[0] != "Basic" {
		slog.Error("REJECTED (bad proxy auth header)", "Method", r.Method, "Host", r.Host, "URL", r.URL, "IP", r.RemoteAddr)
		http.Error(w, "error decoding Proxy-Authorization header", http.StatusInternalServerError)
		return
	}
	credentials, err := base64.StdEncoding.DecodeString(authComponents[1])
	if err != nil {
		slog.Error("REJECTED (error decoding Proxy-Authorization header)", "Method", r.Method, "Host", r.Host, "URL", r.URL, "IP", r.RemoteAddr)
		http.Error(w, "error decoding Proxy-Authorization header", http.StatusInternalServerError)
		return
	}
	usernamePassword := strings.SplitN(string(credentials), ":", 2)
	if len(usernamePassword) != 2 {
		slog.Error("REJECTED (splitting username/password)", "Method", r.Method, "Host", r.Host, "URL", r.URL, "IP", r.RemoteAddr)
		http.Error(w, "error splitting username/password", http.StatusInternalServerError)
		return
	}
	username := usernamePassword[0]
	password := usernamePassword[1]
	if username != "warped" || password != h.proxyPassword {
		slog.Error("REJECTED (invalid username/password)", "Method", r.Method, "Host", r.Host, "URL", r.URL, "IP", r.RemoteAddr)
		http.Error(w, "Invalid username/password", http.StatusUnauthorized)
		return
	}

	// Parse the target host without the optional port
	host := r.Host
	if strings.Contains(r.Host, ":") {
		host, _, err = net.SplitHostPort(r.Host)
	}
	if err != nil {
		slog.Error("error splitting host/port", slogor.Err(err))
		http.Error(w, "error splitting host/port", http.StatusInternalServerError)
		return
	}

	// For the moment, we only support TLS connections (https).
	// TODO: add support for http connections. Or maybe not, as they must be forbidden from the Internet!

	// The first request before a TLS session is stablished is a CONNECT request
	if r.Method == http.MethodConnect {

		// If the client wants to connect to one of the hosts that we are interested in, then we use the MITM proxy to intercept traffic.
		// Otherwise, we will just use the transparent proxy because we are not interested in the data exchanged.
		if slices.Contains(h.hostTargets, host) {
			h.mitmProxy.ServeHTTP(w, r)
		} else {
			h.transparentProxy.ServeHTTP(w, r)
		}

	} else {

		// We received a different HTTP method (GET, POST, ...), indicating that the client wants to use non-TLS session.
		http.Error(w, "this proxy only supports CONNECT", http.StatusMethodNotAllowed)

	}

}

// mitmProxy is a type implementing http.Handler that serves as a MITM proxy
// for CONNECT tunnels. Create new instances of mitmProxy using createMitmProxy.
type mitmProxy struct {
	caCert    *x509.Certificate
	caKey     any
	pdpServer string
}

// createMitmProxy creates a new MITM proxy. It should be passed the filenames
// for the certificate and private key of a certificate authority trusted by the
// client's machine.
func createMitmProxy(caCertFile, caKeyFile string, pdpServer string) *mitmProxy {
	caCert, caKey, err := loadX509KeyPair(caCertFile, caKeyFile)
	if err != nil {
		slog.Error("Error loading CA certificate/key", slogor.Err(err))
		os.Exit(1)
	}
	slog.Info("loaded CA certificate and key;", "IsCA", caCert.IsCA)

	return &mitmProxy{
		caCert:    caCert,
		caKey:     caKey,
		pdpServer: pdpServer,
	}
}

func (p *mitmProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodConnect {
		p.proxyConnect(w, req)
	} else {
		http.Error(w, "this proxy only supports CONNECT", http.StatusMethodNotAllowed)
	}
}

// proxyConnect implements the MITM proxy for CONNECT tunnels.
func (p *mitmProxy) proxyConnect(w http.ResponseWriter, proxyReq *http.Request) {
	slog.Debug("CONNECT requested", "to", proxyReq.Host, "from", proxyReq.RemoteAddr)

	// "Hijack" the client connection to get a TCP (or TLS) socket we can read
	// and write arbitrary data to/from.
	hj, ok := w.(http.Hijacker)
	if !ok {
		slog.Error("http server doesn't support hijacking connection")
		http.Error(w, "http server doesn't support hijacking connection", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hj.Hijack()
	if err != nil {
		slog.Error("http hijacking failed", slogor.Err(err))
		http.Error(w, "http hijacking failed", http.StatusInternalServerError)
		return
	}

	// proxyReq.Host will hold the CONNECT target host, which will typically have
	// a port - e.g. example.org:443
	// To generate a fake certificate for example.org, we have to first split off
	// the host from the port.
	host := proxyReq.Host
	if strings.Contains(proxyReq.Host, ":") {
		host, _, err = net.SplitHostPort(proxyReq.Host)
	}
	if err != nil {
		slog.Error("error splitting host/port", slogor.Err(err))
		http.Error(w, "error splitting host/port", http.StatusInternalServerError)
		return
	}

	// Create a fake TLS certificate for the target host, signed by our CA. The
	// certificate will be valid for 10 days (240 hours) - this number can be changed.
	pemCert, pemKey := createCert([]string{host}, p.caCert, p.caKey, 240)
	tlsCert, err := tls.X509KeyPair(pemCert, pemKey)
	if err != nil {
		slog.Error("creating certificate", slogor.Err(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send an HTTP OK response back to the client; this initiates the CONNECT
	// tunnel. From this point on the client will assume it's connected directly
	// to the target.
	if _, err := clientConn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n")); err != nil {
		slog.Error("writing status to client:", slogor.Err(err))
		// TODO: we can not use the HTTP status codes here.
		clientConn.Close()
		return
	}

	// Configure a new TLS server, pointing it at the client connection, using
	// our certificate. This server will now pretend being the target.
	tlsConfig := &tls.Config{
		PreferServerCipherSuites: true,
		CurvePreferences:         []tls.CurveID{tls.X25519, tls.CurveP256},
		MinVersion:               tls.VersionTLS13,
		Certificates:             []tls.Certificate{tlsCert},
	}

	tlsConn := tls.Server(clientConn, tlsConfig)
	defer tlsConn.Close()

	// Create a buffered reader for the client connection; this is required to
	// use http package functions with this connection.
	connReader := bufio.NewReader(tlsConn)

	// Run the proxy in a loop until the client closes the connection.
	for {
		// Read an HTTP request from the client; the request is sent over TLS that
		// connReader is configured to serve. The read will run a TLS handshake in
		// the first invocation (we could also call tlsConn.Handshake explicitly
		// before the loop, but this isn't necessary).
		// Note that while the client believes it's talking across an encrypted
		// channel with the target, the proxy gets these requests in "plain text"
		// because of the MITM setup.
		r, err := http.ReadRequest(connReader)
		if err == io.EOF {
			break
		} else if err != nil {
			slog.Error("MITM", slogor.Err(err))
			tlsConn.Close()
			return
		}

		var resp *http.Response

		// We will intercept the requests to the TMForum APIs
		if tmfproxy.PrefixInRequest(r.URL.Path) {

			// Take the original request and change its destination to be forwarded
			// to the PDP server.
			targetUrl, err := url.Parse(p.pdpServer)
			if err != nil {
				slog.Error("## MITM LOCAL", slogor.Err(err), "URL", r.URL)
				tlsConn.Close()
				return
			}
			targetUrl.Path = r.URL.Path
			targetUrl.RawQuery = r.URL.RawQuery
			r.URL = targetUrl

			// Make sure this is unset for sending the request through a client
			r.RequestURI = ""

			// Send the request to the target server and log the response.
			resp, err = http.DefaultClient.Do(r)
			if err != nil {
				slog.Error("## MITM LOCAL", slogor.Err(err), "URL", r.URL)
				tlsConn.Close()
				return
			}

			slog.Debug("## MITM LOCAL", "URL", r.URL, "status", resp.Status)
			sendReply(tlsConn, resp)

		} else {

			// Take the original request and change its destination to be forwarded
			// to the target server.
			err = changeRequestToTarget(r, proxyReq.Host)
			if err != nil {
				slog.Error(">> MITM FORWARDED changeRequestToTarget", "host", proxyReq.Host, "URL", r.URL, slogor.Err(err))
				// TODO: modify the error sent back
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Disable following redirects, because we want to see the 302 responses
			http.DefaultClient.CheckRedirect = func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }

			// Send the request to the target server.
			resp, err = http.DefaultClient.Do(r)
			if err != nil {
				slog.Error(">> MITM FORWARDED sending request", "host", proxyReq.Host, "URL", r.URL, slogor.Err(err))
				tlsConn.Close()
				return
			}

			// Reply to the client with the response from the target server.
			slog.Info(">> MITM FORWARDED", "URL", r.URL, "status", resp.Status)
			sendReply(tlsConn, resp)

		}

	}
}

func sendReply(tlsConn *tls.Conn, resp *http.Response) {
	defer resp.Body.Close()
	if err := resp.Write(tlsConn); err != nil {
		slog.Error("writing response back:", slogor.Err(err))
	}
}

// createCert creates a new certificate/private key pair for the given domains,
// signed by the parent/parentKey certificate. hoursValid is the duration of
// the new certificate's validity.
func createCert(dnsNames []string, parent *x509.Certificate, parentKey crypto.PrivateKey, hoursValid int) (cert []byte, priv []byte) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		slog.Error("generating private key", slogor.Err(err))
		os.Exit(1)
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		slog.Error("generating serial number", slogor.Err(err))
		os.Exit(1)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"JRM MITM proxy"},
		},
		DNSNames:  dnsNames,
		NotBefore: time.Now().AddDate(0, 0, -1), // Valid from yesterday, to make sure time zones do not affect validity
		NotAfter:  time.Now().Add(time.Duration(hoursValid) * time.Hour),

		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, parent, &privateKey.PublicKey, parentKey)
	if err != nil {
		slog.Error("creating certificate", slogor.Err(err))
		os.Exit(1)
	}
	pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if pemCert == nil {
		slog.Error("encode certificate to PEM", slogor.Err(err))
		os.Exit(1)
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		slog.Error("marshal private key", slogor.Err(err))
		os.Exit(1)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	if pemCert == nil {
		slog.Error("encode key to PEM")
		os.Exit(1)
	}

	return pemCert, pemKey
}

// loadX509KeyPair loads a certificate/key pair from files, and unmarshals them
// into data structures from the x509 package. Note that private key types in Go
// don't have a shared named interface and use `any` (for backwards
// compatibility reasons).
func loadX509KeyPair(certFile, keyFile string) (cert *x509.Certificate, key any, err error) {
	cf, err := os.ReadFile(certFile)
	if err != nil {
		return nil, nil, err
	}

	kf, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, nil, err
	}
	certBlock, _ := pem.Decode(cf)
	cert, err = x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}

	keyBlock, _ := pem.Decode(kf)
	key, err = x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}

	return cert, key, nil
}

// changeRequestToTarget modifies req to be re-routed to the given target;
// the target should be taken from the Host of the original tunnel (CONNECT)
// request.
func changeRequestToTarget(req *http.Request, targetHost string) error {
	targetUrl, err := addrToUrl(targetHost)
	if err != nil {
		return err
	}
	targetUrl.Path = req.URL.Path
	targetUrl.RawQuery = req.URL.RawQuery
	req.URL = targetUrl
	// Make sure this is unset for sending the request through a client
	req.RequestURI = ""
	return nil
}

func addrToUrl(addr string) (*url.URL, error) {
	if !strings.HasPrefix(addr, "https") {
		addr = "https://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	return u, nil
}

type myFilter struct {
}

func NewFilter() cproxy.Filter {
	return &myFilter{}
}

func (this myFilter) IsAuthorized(_ http.ResponseWriter, request *http.Request) bool {

	slog.Debug("Foreign request", "URL", request.URL)

	return true
}

func processTMFAPI() {

}
