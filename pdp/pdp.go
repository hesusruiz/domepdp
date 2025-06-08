// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/hesusruiz/domeproxy/config"
	conf "github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/tmfcache"

	"gitlab.com/greyxor/slogor"
	starjson "go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	sttime "go.starlark.net/lib/time"
	"go.starlark.net/repl"
	st "go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"
)

func init() {
	// Add our built-ins to the Starlark Universe dictionary before any evaluation begins.
	// All values here must be immutable and shared among all instances.
	// See here for the standard Starlark entities:
	// https://github.com/google/starlark-go/blob/master/doc/spec.md#built-in-constants-and-functions

	// Create a StarLark module with our own utility functions
	var Module = &starlarkstruct.Module{
		Name: "star",
		Members: st.StringDict{
			"getinput": st.NewBuiltin("getinput", getInputElement),
			"getbody":  st.NewBuiltin("getbody", getRequestBody),
		},
	}

	// Set the global Starlark environment with required modules, including our own
	st.Universe["json"] = starjson.Module
	st.Universe["time"] = sttime.Module
	st.Universe["math"] = math.Module
	st.Universe["star"] = Module

}

// We may request decisions to Authenticate or to Authorize
type Decision int

const Authenticate Decision = 1
const Authorize Decision = 2

func (d Decision) String() string {
	if d == Authenticate {
		return "Authenticate"
	} else {
		return "Authorize"
	}
}

// PDP implements a simple Policy Decision Point in Starlark, for use in the DOME project.
//
// There can be several instances simultaneously, and each instance is safe for concurrent
// use by different goroutines.
type PDP struct {

	// environment is the run-time environment (production or development) where the PDP executes.
	// Some things behave differently by default, like logging level.
	// environment Environment
	config *conf.Config

	// The name of the file where the policy rules reside.
	scriptname string

	// The function used at runtime to read the files.
	// If not specified by the caller, a default implementation is used, which uses the file system.
	// readFileFun func(fileName string) (entry *FileEntry, err error)

	// The public key used to verify the Access Tokens. In DOME they belong to the Verifier,
	// and the PDP retrieves it dynamically depending on the environment.
	// The caller is able to provide a function to retrieve the key from a different place.
	verifierJWK        *jose.JSONWebKey
	verificationKeyFun func(config *conf.Config) (*jose.JSONWebKey, error)

	debug bool

	// The file cache to read the policy and other files. Modifications to the original file
	// are picked up automatically according to a freshness policy.
	// fileCache    sync.Map
	fileCache *conf.SimpleFileCache

	// The pool of instances of the policy execution engines, to minimize startup
	// and teardown overheads.
	// Every goroutine uses its own instance from the pool, so they are goroutine safe.
	// If the file with the policies change, the associated Starlark thread is updated,
	// Goroutines using old versions will run until completion, and new ones will pick
	// the new version of the policies
	threadPool sync.Pool

	// The http Client to retrieve the policies from a remote server if configured to do so.
	httpClient *http.Client
}

// NewPDP creates a new PDP instance.
//   - environment is the runtime environment (development or production)
//   - fileName specifies the file with the policies
//   - debug enables more logging information
//   - readFileFun is a user-provided function to read the file from where it is stored. If not
//     provided, the default function is used, which reads from the disk and using a cache
//     to improve performance.
//   - verificationKeyFunc is a user-provided function to supply the verification key for
//     access tokens. If not provided, the default function is used which queries the Verifier
//     JWKS endpoint for the public key of the Verifier.
func NewPDP(
	config *conf.Config,
	readFileFun func(fileName string) (fentry *conf.FileEntry, err error),
	verificationKeyFunc func(config *conf.Config) (*jose.JSONWebKey, error),
) (*PDP, error) {

	m := &PDP{}
	m.config = config
	m.scriptname = config.PolicyFileName
	// m.fileCache = sync.Map{}

	// Create the file cache and initialize it with the policy file.
	m.fileCache = conf.NewSimpleFileCache(nil)
	m.fileCache.Get(config.PolicyFileName)

	// Set either the user-supplied key retrieval function or the default one.
	if verificationKeyFunc == nil {
		m.verificationKeyFun = m.defaultVerificationKey
	} else {
		m.verificationKeyFun = verificationKeyFunc
	}

	// Retrieve the key at initialization time, to discover any possible
	// error in environment configuration as early as possible (eg, the Verifier is not running).
	// TODO: provide for refresh of the key without restarting the PDP
	var err error
	m.verifierJWK, err = m.verificationKeyFun(config)
	if err != nil {
		return nil, fmt.Errorf("error retrieving verification key: %w", err)
	}

	// Create the pool of parsed and compiled Starlark policy rules.
	m.threadPool = sync.Pool{
		New: func() any {
			return m.BufferedParseAndCompileFile(m.scriptname)
		},
	}

	m.debug = config.Debug

	// We use an http.Client with a timeout of 10 seconds and no redirects.
	m.httpClient = &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return tmfcache.ErrorRedirectsNotAllowed
		},
	}

	return m, nil
}

// defaultVerificationKey returns the verification key for Access Tokens, in JWK format.
//
// It receives the runtime environment, enabling a different mechanism depending on it.
func (m *PDP) defaultVerificationKey(config *conf.Config) (*jose.JSONWebKey, error) {

	// Retrieve the OpenID configuration from the Verifier
	oid, err := NewOpenIDConfig(config)
	if err != nil {
		return nil, err
	}

	// Use the link in the OpenID config to retrieve the key from the Verifier.
	// In DOME, we use the first key from the keyset retrieved.
	m.verifierJWK, err = oid.VerificationJWK()
	if err != nil {
		return nil, err
	}

	return m.verifierJWK, nil

}

func (m *PDP) VerificationJWK() (key *jose.JSONWebKey, err error) {
	if m.verifierJWK == nil {
		m.verifierJWK, err = m.verificationKeyFun(m.config)
		return m.verifierJWK, err
	}
	return m.verifierJWK, nil
}

// threadEntry represents the pool of Starlark threads for policy rules execution.
// All instances are normally the same, using the same compiled version of the same file.
// The pool increases concurrency because a given Starlark thread can be reused
// among goroutines, but not used concurrently by the same goroutine.
// Another benefit is that it facilitates the dynamic update of policy files without
// affecting concurrency.
type threadEntry struct {
	globals              st.StringDict
	predeclared          st.StringDict
	thread               *st.Thread
	authenticateFunction *st.Function
	authorizeFunction    *st.Function
	scriptname           string
	scriptHash           uint64
}

// BufferedParseAndCompileFile reads a file with Starlark code and compiles it, storing the resulting global
// dictionary for later usage. In particular, the compiled module should define two functions,
// one for athentication and the second for authorisation.
// BufferedParseAndCompileFile can be called several times and will perform a new compilation every time,
// creating a new Thread and so the old ones will never be called again and eventually will be disposed.
func (m *PDP) BufferedParseAndCompileFile(scriptname string) *threadEntry {
	slog.Debug("BufferedParseAndCompileFile Start")
	var err error

	// The Starlark thread will be created for each invocation of the PDP in a local variable
	// to avoid concurrency problems. The thread is not goroutine safe, but the globals are.
	te := &threadEntry{}
	te.scriptname = scriptname

	logger := slog.Default()

	// The compiled program context will be stored in a new Starlark thread for each invocation
	te.thread = &st.Thread{
		Load: repl.MakeLoadOptions(&syntax.FileOptions{}),
		Print: func(_ *st.Thread, msg string) {
			logger.Info("rules => " + msg)
		},
		Name: "exec " + scriptname,
	}

	// Create a predeclared environment holding the 'input' object.
	// For the moment it is empty, but it will be mutated for each request for authentication.
	te.predeclared = st.StringDict{}
	te.predeclared["input"] = StarTMFMap{}

	// Get the file from the cache, which will be up to date according to its freshness policy.
	entry, err := m.fileCache.Get(scriptname)
	if err != nil {
		slog.Error("reading script", slogor.Err(err), "file", scriptname)
		return nil
	}

	// entry, err := m.readFileFun(scriptname)
	// if err != nil {
	// 	slog.Error("reading script", slogor.Err(err), "file", scriptname)
	// 	return nil
	// }

	te.scriptHash = entry.FileHash
	src := entry.Content

	// Parse and execute the top-level commands in the script file
	// The globals are thread-local and not process-global
	te.globals, err = st.ExecFileOptions(&syntax.FileOptions{}, te.thread, scriptname, src, te.predeclared)
	if err != nil {
		slog.Error("error compiling Starlark program", slogor.Err(err))
		return nil
	}

	// Make sure that the global environment is frozen so the Startlark script cannot
	// modify it. This is important for security and to avoid concurrency problems.
	te.globals.Freeze()

	// The module has to define a function called 'authorize', which will be invoked
	// for each request to access protected resources.
	te.authorizeFunction, err = getGlobalFunction(te.globals, "authorize")
	if err != nil {
		return nil
	}

	slog.Debug("BufferedParseAndCompileFile End")
	return te

}

func (m *PDP) Reset(te *threadEntry) error {

	// Read the file again, to check if it has changed.
	// entry, err := m.readFileFun(te.scriptname)
	entry, err := m.fileCache.Get(te.scriptname)
	if err != nil {
		slog.Error("reading script", slogor.Err(err), "file", te.scriptname)
		return nil
	}

	// If hashes are the same, we do not need to recompile the file.
	if entry.FileHash == te.scriptHash {
		return nil
	}

	// The file has changed, so we recompile it.
	src := entry.Content

	// Parse and execute the top-level commands in the script file
	// The globals are thread-local and not process-global
	te.globals, err = st.ExecFileOptions(&syntax.FileOptions{}, te.thread, te.scriptname, src, te.predeclared)
	if err != nil {
		slog.Error("error compiling Starlark program", slogor.Err(err))
		return err
	}

	// Make sure that the global environment is frozen so the Startlark script cannot
	// modify it. This is important for security and to avoid concurrency problems.
	te.globals.Freeze()

	// The module has to define a function called 'authorize', which will be invoked
	// for each request to access protected resources.
	te.authorizeFunction, err = getGlobalFunction(te.globals, "authorize")
	if err != nil {
		return err
	}

	return nil
}

// getGlobalFunction retrieves a Callable from the supplied globals dictionary.
func getGlobalFunction(globals st.StringDict, funcName string) (*st.Function, error) {

	// Check that we have the function
	f, ok := globals[funcName]
	if !ok {
		err := fmt.Errorf("missing definition of %s", funcName)
		log.Println(err.Error())
		return nil, err
	}

	// Check that is is a Callable
	starFunction, ok := f.(*st.Function)
	if !ok {
		err := fmt.Errorf("expected a Callable but got %v", f.Type())
		log.Println(err.Error())
		return nil, err
	}

	return starFunction, nil
}

// // defaultReadDiskFileFun reads the given file from disk, using a sync.Map to store it
// // It is safe for concurrent use.
// func (m *PDP) defaultReadDiskFileFun(fileName string) (*FileEntry, error) {

// 	now := time.Now()

// 	// Try to get the file from the cache
// 	fe, found := m.fileCache.Load(fileName)
// 	if found {
// 		entry := fe.(*FileEntry)

// 		// Return the entry if it is fresh enough.
// 		if now.Sub(entry.EntryUpdated) < freshnessForDiskFiles {
// 			slog.Debug("readFileIfNew", "file", fileName, "msg", "found and cache entry is fresh")
// 			return entry, nil
// 		}
// 		slog.Debug("readFileIfNew", "file", fileName, "msg", "found but entry is NOT fresh")
// 	}

// 	// We are here because either the entry was not found or is not fresh.
// 	// We get the file info, to check if it was modified.
// 	fileInfo, err := os.Stat(fileName)
// 	if err != nil {
// 		return nil, fmt.Errorf("readFileIfNew: error checking file %s: %w", fileName, err)
// 	} else if fileInfo.Mode().IsDir() {
// 		// We cannot read a directory
// 		return nil, fmt.Errorf("readFileIfNew: file %s is a directory, not a file", fileName)
// 	}

// 	// Check if the size is "reasonable" to be loaded in the cache. Default is 1MB, enogh for many policies.
// 	if fileInfo.Size() > maxFileSize {
// 		return nil, fmt.Errorf("readFileIfNew: file %s is too big", fileName)
// 	}

// 	modifiedAt := fileInfo.ModTime()

// 	// If not found, read the file, set in the cache and return the file.
// 	if !found {
// 		slog.Debug("readFileIfNew", "file", fileName, "msg", "entry not found in cache")
// 		content, err := os.ReadFile(fileName)
// 		if err != nil {
// 			return nil, err
// 		}

// 		// Add or replace the content of the file cache
// 		entry := &FileEntry{
// 			Name:         fileName,
// 			EntryUpdated: now,
// 			FileModTime:  modifiedAt,
// 			Content:      content,
// 			FileHash:     maphash.Bytes(seed, content),
// 		}

// 		m.fileCache.Store(fileName, entry)

// 		return entry, nil

// 	}

// 	// The entry was found in the cache, but it may be stale.
// 	entry := fe.(*FileEntry)

// 	if entry.FileModTime.Before(modifiedAt) {

// 		// The entry in the cache is old, so we read again the file.
// 		content, err := os.ReadFile(fileName)
// 		if err != nil {
// 			return nil, fmt.Errorf("readFileIfNew: error reading file %s: %w", fileName, err)
// 		}

// 		// Add to the cache. There is only one instance of each file in the cache.
// 		entry := &FileEntry{
// 			Name:         fileName,
// 			EntryUpdated: now,
// 			FileModTime:  modifiedAt,
// 			Content:      content,
// 			FileHash:     maphash.Bytes(seed, content),
// 		}

// 		slog.Debug("readFileIfNew", "file", fileName, "msg", "file modification is later than in entry")
// 		m.fileCache.Store(fileName, entry)
// 		return entry, nil

// 	} else {

// 		// The entry in the cache is still valid, update the timestamp and return the file.
// 		// Updating the timestamp extends the TTL of the entry.
// 		slog.Debug("readFileIfNew", "file", fileName, "msg", "entry was not fresh but still valid")
// 		entry.EntryUpdated = now

// 		// And return contents
// 		return entry, nil
// 	}

// }

// // defaultReadServerFileFun implements a simple file cache to read files from an http server.
// // It is intended for a small number of well-known files, like the configuration or policy files.
// // In this use case, we do not need an eviction system, as the number of files is small and
// // the cache is not expected to grow indefinitely.
// // It is also used mostly for reads with very few writes, which happen only when the file is modified.
// // The cache can be used concurrently, so we use a sync.Map, which is very good for reads and acceptable for writes.
// func (m *PDP) defaultReadServerFileFun(fileName string) (*FileEntry, error) {

// 	now := time.Now()

// 	// Try to get the file from the cache
// 	fe, found := m.fileCache.Load(fileName)
// 	if found {

// 		// Return the entry if it is fresh enough, or read it again if it is not.

// 		entry := fe.(*FileEntry)
// 		if now.Before(entry.expires) {

// 			// We return directly the entry in the cache if it is fresh enough.
// 			slog.Debug("readFileIfNew", "file", fileName, "msg", "found and cache entry is fresh")
// 			return entry, nil

// 		} else {

// 			// The entry in the cache is old, so we read again the file.
// 			slog.Debug("readFileIfNew", "file", fileName, "msg", "found but entry is NOT fresh")
// 			req, err := http.NewRequest("GET", fileName, nil)
// 			if err != nil {
// 				// An error creating the request is strange, but we return the entry in the cache
// 				// and log the error, so the system can continue working, even with stale data.
// 				slog.Error("readFileIfNew", "file", fileName, "msg", "error creating request", slogor.Err(err))
// 				return entry, nil
// 			}

// 			// Add to the request the If-None-Match header if Etag was present in the previous response from the server
// 			if entry.Etag != "" {
// 				req.Header.Add("If-None-Match", entry.Etag)
// 			}

// 			resp, err := m.httpClient.Do(req)
// 			if err != nil {
// 				// Log the error and return the entry in the cache, so the system can continue working, even with stale data.
// 				slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
// 				return entry, nil
// 			}
// 			defer resp.Body.Close()

// 			// If the server returns a 304 Not Modified, we return the entry in the cache
// 			if resp.StatusCode == http.StatusNotModified {
// 				slog.Debug("readFileIfNew", "file", fileName, "msg", "file not modified")

// 				// Set the Etag header if present in the response from the server
// 				if etag := resp.Header.Get("Etag"); etag != "" {
// 					entry.Etag = etag
// 				}

// 				// Refresh the expires header if present in the response from the server
// 				if expires := resp.Header.Get("Expires"); expires != "" {
// 					entry.expires, err = time.Parse(time.RFC1123, expires)
// 					if err != nil {
// 						// If we cannot parse the Expires header, we just log the error and return the entry in the cache,
// 						// so the system can continue working, even with stale data.
// 						slog.Error("readFileIfNew", "file", fileName, "msg", "error parsing Expires header", slogor.Err(err))
// 					}
// 				}

// 				// Update the entry in the cache with the new Etag and Expires headers
// 				// We do not update the content of the file, as it was not modified.
// 				// For the caller, the content is the same as before so we return found = true
// 				m.fileCache.Store(fileName, entry)
// 				return entry, nil
// 			}

// 			// Other status codes are errors, so we return the entry in the cache, logging the error
// 			// and the status code.
// 			if resp.StatusCode != http.StatusOK {
// 				slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slog.Int("status", resp.StatusCode))
// 				return entry, nil
// 			}

// 			content, err := io.ReadAll(resp.Body)
// 			if err != nil {
// 				// If we cannot read the file, we return the entry in the cache, logging the error
// 				// so the system can continue working, even with stale data.
// 				slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
// 				return entry, nil
// 			}

// 			// Check the size of the file
// 			if len(content) > maxFileSize {
// 				// If the file is too big, we return the entry in the cache, logging the error
// 				// so the system can continue working, even with stale data.
// 				slog.Error("readFileIfNew", "file", fileName, "msg", "file is too big", slog.Int("size", len(content)))
// 				// We do not return the content of the file, as it is too big.
// 				return entry, nil
// 			}

// 			// Store the new entry with the content of the file
// 			entry := &FileEntry{
// 				Name:         fileName,
// 				EntryUpdated: now,
// 				Content:      content,
// 				FileHash:     maphash.Bytes(seed, content),
// 			}

// 			// Add the Etag header if present in the response from the server
// 			if etag := resp.Header.Get("Etag"); etag != "" {
// 				entry.Etag = etag
// 			}
// 			// Set the expires header if present in the response from the server
// 			if expires := resp.Header.Get("Expires"); expires != "" {
// 				entry.expires, err = time.Parse(time.RFC1123, expires)
// 				if err != nil {
// 					// If we cannot parse the Expires header, set the default freshness
// 					entry.expires = time.Now().Add(freshnessForServerFiles)
// 				}
// 			} else {
// 				// If the Expires header is not present, set the default freshness
// 				entry.expires = time.Now().Add(freshnessForServerFiles)
// 			}

// 			slog.Debug("readFileIfNew", "file", fileName, "msg", "file refreshed from the server")

// 			// Store the entry in the cache and return the content, with found = false because the contents changed
// 			m.fileCache.Store(fileName, entry)
// 			return entry, nil

// 		}

// 	} else {

// 		// The entry was not found, read the file from the server, set in the cache and return the file.
// 		// In we found any error, we can not do anything except log it and return an error.
// 		slog.Debug("readFileIfNew", "file", fileName, "msg", "entry not found in cache")

// 		// Request the file from the server.
// 		req, err := http.NewRequest("GET", fileName, nil)
// 		resp, err := m.httpClient.Do(req)
// 		if err != nil {
// 			// If we cannot read the file, we return an error after logging it
// 			slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
// 			return nil, err
// 		}
// 		defer resp.Body.Close()

// 		if resp.StatusCode != http.StatusOK {
// 			// Other status codes are errors, and we can not do anything except log an error
// 			// and return the error.
// 			slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slog.Int("status", resp.StatusCode))
// 			return nil, fmt.Errorf("readFileIfNew: error reading file %s: %w", fileName, err)
// 		}

// 		content, err := io.ReadAll(resp.Body)
// 		if err != nil {
// 			// If we cannot read the file, we return an error after logging it
// 			slog.Error("readFileIfNew", "file", fileName, "msg", "error reading file", slogor.Err(err))
// 			return nil, err
// 		}

// 		// Check the size of the file
// 		if len(content) > maxFileSize {
// 			// If the file is too big, we return an error after logging it
// 			slog.Error("readFileIfNew", "file", fileName, "msg", "file is too big", slog.Int("size", len(content)))
// 			return nil, fmt.Errorf("readFileIfNew: file %s is too big", fileName)
// 		}

// 		// Store the entry with the content of the file
// 		entry := &FileEntry{
// 			Name:         fileName,
// 			EntryUpdated: now,
// 			Content:      content,
// 			FileHash:     maphash.Bytes(seed, content),
// 		}

// 		// Add the Etag header if present in the response from the server
// 		if etag := resp.Header.Get("Etag"); etag != "" {
// 			entry.Etag = etag
// 		}

// 		// Set the expires header if present in the response from the server
// 		if expires := resp.Header.Get("Expires"); expires != "" {
// 			entry.expires, err = time.Parse(time.RFC1123, expires)
// 			if err != nil {
// 				// If we cannot parse the Expires header, set the default freshness
// 				entry.expires = time.Now().Add(freshnessForServerFiles)
// 			}
// 		} else {
// 			entry.expires = time.Now().Add(freshnessForServerFiles)
// 		}

// 		// Store the entry in the cache and return the content, with found = false because the contents changed
// 		m.fileCache.Store(fileName, entry)
// 		return entry, nil

// 	}

// }

// TakeAuthnDecision is called when a decision should be taken for either Authentication or Authorization.
// The type of decision to evaluate is passed in the Decision argument. The rest of the arguments contain the information required
// for the decision. They are:
// - the Verifiable Credential with the information from the caller needed for the decision
// - the protected resource that the caller identified in the Credential wants to access

func (m *PDP) TakeAuthnDecision(decision Decision, input StarTMFMap) (bool, error) {
	var err error

	// Get a Starlark Thread from the pool to evaluate the policies.
	ent := m.threadPool.Get()
	if ent == nil {
		return false, fmt.Errorf("getting a thread entry from pool")
	}
	defer m.threadPool.Put(ent)

	te := ent.(*threadEntry)
	if te == nil {
		return false, fmt.Errorf("invalid entry type in the pool")
	}

	// Check if the thread is still valid. If not, we need to recompile the file.
	err = m.Reset(te)
	if err != nil {
		return false, err
	}

	// We mutate the predeclared identifier, so the policy can access the data for this request.
	// We can also service possible callbacks from the rules engine.
	te.predeclared["input"] = input

	// Build the arguments to the StarLark function, which is empty.
	var args st.Tuple

	// Call the corresponding function in the Starlark Thread
	var result st.Value
	if decision == Authenticate {
		// Call the 'authenticate' funcion
		result, err = st.Call(te.thread, te.authenticateFunction, args, nil)
	} else {
		// Call the 'authorize' function
		result, err = st.Call(te.thread, te.authorizeFunction, args, nil)
	}

	if err != nil {
		fmt.Printf("rules ERROR: %s\n", err.(*st.EvalError).Backtrace())
		return false, fmt.Errorf("error calling function: %w", err)
	}

	// Check that the value returned is of the correct type (boolean)
	resultType := result.Type()
	if resultType != "bool" {
		err := fmt.Errorf("function returned wrong type: %v", resultType)
		return false, err
	}

	// Return the value as a Go boolean
	return bool(result.(st.Bool).Truth()), nil

}

func (m *PDP) GetFile(filename string) (*conf.FileEntry, error) {

	entry, err := m.fileCache.MustExist(filename)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (m *PDP) PutFile(filename string, content []byte) error {
	return m.fileCache.Set(filename, content, 0)
}

func getInputElement(thread *st.Thread, _ *st.Builtin, args st.Tuple, kwargs []st.Tuple) (st.Value, error) {

	// Get the current input structure being processed
	r := thread.Local("inputrequest")
	input, ok := r.(StarTMFMap)
	if !ok {
		return st.None, fmt.Errorf("no request found in thread locals")
	}

	// Get the element
	var elemPath string
	err := st.UnpackPositionalArgs("input2", args, kwargs, 1, &elemPath)
	if err != nil {
		return nil, err
	}

	elem, err := GetValue(input, elemPath)
	if err != nil {
		return st.None, nil
	}
	return elem, nil
}

func getRequestBody(thread *st.Thread, _ *st.Builtin, args st.Tuple, kwargs []st.Tuple) (st.Value, error) {

	// Get the current HTTP request being processed
	r := thread.Local("httprequest")
	request, ok := r.(*http.Request)
	if !ok {
		return st.None, fmt.Errorf("no request found in thread locals")
	}

	// Read the body from the request and store in thread locals in case we need it later
	bytes, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	thread.SetLocal("requestbody", bytes)

	// Return string for the Starlark script
	body := st.String(bytes)

	return body, nil
}

func mapToStarlark(mapClaims map[string]any) (*st.Dict, error) {
	dd := &st.Dict{}

	for k, v := range mapClaims {
		switch v := v.(type) {
		case string:
			dd.SetKey(st.String(k), st.String(v))
		case bool:
			dd.SetKey(st.String(k), st.Bool(v))
		case float64:
			dd.SetKey(st.String(k), st.Float(v))
		case int:
			dd.SetKey(st.String(k), st.MakeInt(v))
		case map[string]any:
			stdic, err := mapToStarlark(v)
			if err != nil {
				return nil, err
			}
			dd.SetKey(st.String(k), stdic)
		case []any:
			stlist, err := listToStarlark(v)
			if err != nil {
				return nil, err
			}
			dd.SetKey(st.String(k), stlist)
		default:
			//
		}
	}

	return dd, nil
}

func listToStarlark(list []any) (*st.List, error) {
	ll := &st.List{}

	for _, v := range list {
		switch v := v.(type) {
		case string:
			ll.Append(st.String(v))
		case map[string]any:
			stmap, err := mapToStarlark(v)
			if err != nil {
				return nil, err
			}
			ll.Append(stmap)
		case bool:
			ll.Append(st.Bool(v))
		case float64:
			ll.Append(st.Float(v))
		case int:
			ll.Append(st.MakeInt(v))
		default:
			//
		}
	}

	return ll, nil
}

func StarDictFromHttpRequest(request *http.Request) (*st.Dict, error) {

	dd := &st.Dict{}

	dd.SetKey(st.String("method"), st.String(request.Method))
	dd.SetKey(st.String("url"), st.String(request.URL.String()))
	dd.SetKey(st.String("path"), st.String(request.URL.Path))
	dd.SetKey(st.String("query"), getDictFromValues(request.URL.Query()))

	dd.SetKey(st.String("host"), st.String(request.Host))
	dd.SetKey(st.String("content_length"), st.MakeInt(int(request.ContentLength)))
	dd.SetKey(st.String("headers"), getDictFromHeaders(request.Header))

	return dd, nil
}

func getDictFromValues(values map[string][]string) *st.Dict {
	dict := &st.Dict{}
	for key, values := range values {
		dict.SetKey(st.String(key), getSkylarkList(values))
	}
	return dict
}

func getDictFromHeaders(headers http.Header) *st.Dict {
	dict := &st.Dict{}
	for key, values := range headers {
		dict.SetKey(st.String(key), getSkylarkList(values))
	}
	return dict
}

func getSkylarkList(values []string) *st.List {
	list := &st.List{}
	for _, v := range values {
		list.Append(st.String(v))
	}
	return list
}

type StarTMFMap map[string]any

// Value interface
func (s StarTMFMap) String() string {
	out := new(strings.Builder)

	out.WriteByte('{')
	sep := ""
	for k, v := range s {
		out.WriteString(sep)
		s := fmt.Sprintf("%v", k)
		out.WriteString(s)
		out.WriteString(": ")

		val := anyToValue(v)
		s = fmt.Sprintf("%v", val.String())
		out.WriteString(s)
		sep = ", "
	}
	out.WriteByte('}')
	return out.String()

}
func (s StarTMFMap) GoString() string      { return s["id"].(string) }
func (s StarTMFMap) Type() string          { return "tmfmap" }
func (s StarTMFMap) Freeze()               {} // immutable
func (s StarTMFMap) Truth() st.Bool        { return len(s) > 0 }
func (s StarTMFMap) Hash() (uint32, error) { return hashString(s["id"].(string)), nil }

// Indexable interface
func (s StarTMFMap) Len() int { return len(s) } // number of entries
// Index(i int) Value // requires 0 <= i < Len()

// Mapping interface
func (s StarTMFMap) Get(name st.Value) (v st.Value, found bool, err error) {

	path := string(name.(st.String))

	// We need at least one name
	if path == "" {
		return s, false, nil
	}

	// This is a special case, where we assume the meaning of "this object".
	if path == "." {
		return s, true, nil
	}

	// Two consecutive dots is an error
	if strings.Contains(path, "..") {
		return nil, false, fmt.Errorf("invalid path %q: contains '..'", path)
	}

	// vv, err := jpath.Get(s, string(name.(st.String)))
	vv, err := GetValue(s, string(name.(st.String)))
	if err != nil {
		return nil, false, err
	}
	v = anyToValue(vv)
	return v, true, nil

	// value, err := s.Attr(string(string(name.(st.String))))
	// if err != nil {
	// 	return nil, false, err
	// }
	// return value, true, nil
}

// Get returns a child of the given value according to a dotted path.
// The source data must be either map[string]any or []any
func GetValue(a StarTMFMap, path string) (st.Value, error) {

	parts := strings.Split(path, ".")

	var src st.Value = a

	// Get the value.
	for pos, pathComponent := range parts {

		switch src.Type() {

		case "tmfmap":
			c := src.(StarTMFMap)

			if value, ok := c[pathComponent]; ok {
				src = anyToValue(value)
				continue
			} else {
				return st.None, nil
				// return nil, fmt.Errorf("jpath.Get: nonexistent map key at %q",
				// 	strings.Join(parts[:pos+1], "."))
			}

		case "tmflist":
			c := src.(StarTMFList)

			// If data is an array, the path component must be an integer (base 10) to index the array
			index, err := strconv.ParseInt(pathComponent, 10, 0)
			if err != nil {
				return nil, fmt.Errorf("jpath.Get: invalid list index at %q",
					strings.Join(parts[:pos+1], "."))
			}
			if int(index) < len(c) {
				// Update src to be the indexed element of the array
				value := c[index]
				src = anyToValue(value)
				continue
			} else {
				return nil, fmt.Errorf(
					"jpath.Get: index out of range at %q: list has only %v items",
					strings.Join(parts[:pos+1], "."), len(c))
			}

		default:

			return nil, fmt.Errorf(
				"jpath.Get: invalid type at %q: expected []any or map[string]any; got %T",
				strings.Join(parts[:pos+1], "."), src)
		}
	}

	return src, nil
}

func anyToValue(value any) st.Value {
	switch v := value.(type) {
	case StarTMFMap:
		return StarTMFMap(v)
	case StarTMFList:
		return StarTMFList(v)
	case string:
		return st.String(v)
	case st.String:
		return st.String(v)
	case map[string]any:
		return StarTMFMap(v)
	case []any:
		var l []st.Value
		for _, elem := range v {
			l = append(l, anyToValue(elem))
		}
		return StarTMFList(l)
	case bool:
		return st.Bool(v)
	case float64:
		return st.Float(v)
	case int:
		return st.MakeInt(v)
	default:
		return st.None
	}
}

// HasAttrs interface
// Attr(name string) (st.Value, error)
// AttrNames() []string

func (s StarTMFMap) Attr(name string) (st.Value, error) {
	value, ok := s[name]
	if !ok {
		return nil, nil
	}

	return anyToValue(value), nil

}

func (s StarTMFMap) AttrNames() []string {
	var keys []string
	for key := range s {
		keys = append(keys, key)
	}
	return keys
}

type StarTMFList []st.Value

// Value interface
func (s StarTMFList) String() string {
	out := new(strings.Builder)

	out.WriteByte('[')
	for i, elem := range s {
		if i > 0 {
			out.WriteString(", ")
		}
		s := fmt.Sprintf("%v", elem)
		out.WriteString(s)
	}
	out.WriteByte(']')

	return out.String()
}
func (s StarTMFList) Type() string          { return "tmflist" }
func (s StarTMFList) Freeze()               {} // immutable
func (s StarTMFList) Truth() st.Bool        { return len(s) > 0 }
func (s StarTMFList) Hash() (uint32, error) { return hashString("tmflist"), nil }

// Indexable interface
func (s StarTMFList) Len() int { return len(s) } // number of entries
func (s StarTMFList) Index(i int) st.Value {
	value := s[i]
	return anyToValue(value)
}

var seed = maphash.MakeSeed()

// hashString computes the hash of s.
func hashString(s string) uint32 {
	if len(s) >= 12 {
		// Call the Go runtime's optimized hash implementation,
		// which uses the AES instructions on amd64 and arm64 machines.
		h := maphash.String(seed, s)
		return uint32(h>>32) | uint32(h)
	}
	return softHashString(s)
}

// softHashString computes the 32-bit FNV-1a hash of s in software.
func softHashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// getClaimsFromToken verifies the Access Token received with the request, and extracts the claims in its payload.
// The most important claim in the payload is the LEARCredential that was used for authentication.
func (m *PDP) getClaimsFromToken(tokString string) (claims map[string]any, found bool, err error) {
	var token *jwt.Token
	var theClaims = MapClaims{}

	if tokString == "" {
		return nil, false, nil
	}

	verifierPublicKeyFunc := func(*jwt.Token) (any, error) {
		vk, err := m.VerificationJWK()
		if err != nil {
			return nil, config.Error(err)
		}
		slog.Debug("publicKeyFunc", "key", vk)
		return vk.Key, nil
	}

	// Validate and verify the token
	token, err = jwt.NewParser().ParseWithClaims(tokString, &theClaims, verifierPublicKeyFunc)
	if err != nil {
		return nil, false, config.Errorf("error parsing token: %w", err)
	}

	jwtmapClaims := token.Claims.(*MapClaims)

	return *jwtmapClaims, true, nil
}
