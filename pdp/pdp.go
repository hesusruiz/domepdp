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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/hesusruiz/domeproxy/internal/jpath"

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
	// All values here must be immutable and shared among all instances of the
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

// Decision can be Authenticate or Authorize
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
	environment Environment

	// The name of the file where the policy rules resides.
	scriptname string

	// The function used at runtime to read the files. If not specified by the caller,
	// a default implementation is used, which uses the file system.
	readFileFun func(fileName string) ([]byte, bool, error)

	// The public key used to verify the Access Tokens. In DOME they belong to the Verifier,
	// and the PDP retrieves it dynamically depending on the environment.
	// The caller is able to provide a function to retrieve the key from a different place.
	verifierJWK        *jose.JSONWebKey
	verificationKeyFun func(environment Environment) (*jose.JSONWebKey, error)

	debug bool

	// The file cache to read the policy files. Modifications to the original file
	// are picked automatically according to a freshness policy.
	fileCache sync.Map

	// The pool of instances of the policy execution engines, to minimize startup
	// and teardown overheads.
	threadPool sync.Pool
}

func NewPDP(environment Environment,
	fileName string,
	debug bool,
	readFileFun func(fileName string) ([]byte, bool, error),
	verificationKeyFunc func(environment Environment) (*jose.JSONWebKey, error),
) (*PDP, error) {

	m := &PDP{}
	m.environment = environment
	m.scriptname = fileName
	m.fileCache = sync.Map{}

	if readFileFun == nil {
		m.readFileFun = m.readFileIfNew
	} else {
		m.readFileFun = readFileFun
	}

	if verificationKeyFunc == nil {
		m.verificationKeyFun = defaultVerificationKey
	} else {
		m.verificationKeyFun = verificationKeyFunc
	}

	// Retrieve the key at initialization time, to discover any possible
	// error in environment configuration as early as possible (eg, the Verifier is not running).
	var err error
	m.verifierJWK, err = m.verificationKeyFun(environment)
	if err != nil {
		return nil, err
	}

	// Create the pool of parsed and compiled Starlark policy rules.
	m.threadPool = sync.Pool{
		New: func() any {
			// The Pool's New function should generally only return pointer
			// types, since a pointer can be put into the return interface
			// value without an allocation:
			return m.BufferedParseAndCompileFile(m.scriptname)
		},
	}

	m.debug = debug

	return m, nil
}

// defaultVerificationKey returns the verification key for Access Tokens, in JWK format.
//
// It receives the runtime environment, enabling a different mechanism depending on it.
func defaultVerificationKey(environment Environment) (*jose.JSONWebKey, error) {

	// Retrieve the OpenID configuration from the Verifier
	oid, err := NewOpenIDConfig(environment)
	if err != nil {
		return nil, err
	}

	// Use the link in the OpenID config to retrieve the key from the Verifier.
	// In DOME, we use the first key from the keyset retrieved.
	verifierJWK, err := oid.VerificationJWK()
	if err != nil {
		return nil, err
	}

	return verifierJWK, nil

}

// threadEntry represents the pool of Starlark threads for policy rules execution.
// All instances are normally the same, using the same compiled version of the same file.
// The pool increases concurrency because a given Starlark thread can be reused
// among goroutines, but not used concurrently.
// Another benefit is that it facilitates the dynamic update of policy files without
// affecting concurrency.
type threadEntry struct {
	globals              st.StringDict
	predeclared          st.StringDict
	thread               *st.Thread
	authenticateFunction *st.Function
	authorizeFunction    *st.Function
	scriptname           string
}

// BufferedParseAndCompileFile reads a file with Starlark code and compiles it, storing the resulting global
// dictionary for later usage. In particular, the compiled module should define two functions,
// one for athentication and the second for athorisation.
// ParseAndCompileFile can be called several times and will perform a new compilation every time,
// creating a new Thread and so the old ones will never be called again and eventually will be disposed.
func (m *PDP) BufferedParseAndCompileFile(scriptname string) *threadEntry {
	slog.Debug("===> BufferedParseAndCompileFile")
	var err error

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

	src, _, err := m.readFileFun(scriptname)
	if err != nil {
		slog.Error("reading script", slogor.Err(err), "file", scriptname)
		return nil
	}

	// Parse and execute the top-level commands in the script file
	te.globals, err = st.ExecFileOptions(&syntax.FileOptions{}, te.thread, scriptname, src, te.predeclared)
	if err != nil {
		slog.Error("error compiling Starlark program", slogor.Err(err))
		return nil
	}

	// Make sure that the global environment is frozen so the Startlark
	te.globals.Freeze()

	// The module has to define a function called 'authorize', which will be invoked
	// for each request to access protected resources.
	te.authorizeFunction, err = getGlobalFunction(te.globals, "authorize")
	if err != nil {
		return nil
	}

	slog.Debug("<=== BufferedParseAndCompileFile")
	return te

}

// getGlobalFunction retrieves a Callable from the globals dictionary.
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

// fileEntry implements a file cache to improve performance while being able to pick newer
// versions of the rules in a reasonable time (20 secods by default).
type fileEntry struct {
	name         string
	entryUpdated time.Time
	fileModTime  time.Time
	content      []byte
}

const maxFileSize = 1024 * 1024
const freshness = 20 * time.Second

func (m *PDP) readFileIfNew(fileName string) ([]byte, bool, error) {

	now := time.Now()

	// Try to get the file from the cache
	fe, found := m.fileCache.Load(fileName)
	if found {
		entry := fe.(*fileEntry)

		// Return the entry if it is fresh enough.
		if now.Sub(entry.entryUpdated) < freshness {
			slog.Debug("found and cache entry is fresh")
			return entry.content, true, nil
		}
		slog.Debug("found but entry is NOT fresh")
	}

	// We are here because either the entry was not found or is not fresh.
	// Get the file info, to check if it was modified.
	// We discard directories.
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return nil, false, err
	} else if fileInfo.Mode().IsDir() {
		return nil, false, fmt.Errorf("is a directory, not a file")
	}

	// Check if the size is "reasonable" to be loaded in the cache
	if fileInfo.Size() > maxFileSize {
		return nil, false, fmt.Errorf("file too big")
	}

	modifiedAt := fileInfo.ModTime()

	// If not found, read the file, set in the cache and return the file.
	if !found {
		slog.Debug("entry not found in cache")
		content, err := os.ReadFile(fileName)
		if err != nil {
			return nil, false, err
		}

		// Add to the cache
		entry := &fileEntry{
			name:         fileName,
			entryUpdated: now,
			fileModTime:  modifiedAt,
			content:      content,
		}

		m.fileCache.Store(fileName, entry)

		return content, false, nil

	}

	// The entry was found in the cache, but it may be stale.
	entry := fe.(*fileEntry)

	if entry.fileModTime.Before(modifiedAt) {

		// Read the file
		content, err := os.ReadFile(fileName)
		if err != nil {
			return nil, false, err
		}

		// Add to the cache
		entry := &fileEntry{
			name:         fileName,
			entryUpdated: now,
			fileModTime:  modifiedAt,
			content:      content,
		}

		slog.Debug("file modification is later than in entry")
		m.fileCache.Store(fileName, entry)
		return content, false, nil

	} else {

		// The entry in the cache is still valid, update the timestamp and return the file.
		slog.Debug("entry was not fresh but still valid")
		entry.entryUpdated = now

		// And return contents
		return entry.content, true, nil
	}

}

// TakeAuthnDecision is called when a decision should be taken for either Athentication or Authorization.
// The type of decision to evaluate is passed in the Decision argument. The rest of the arguments contain the information required
// for the decision. They are:
// - the Verifiable Credential with the information from the caller needed for the decision
// - the protected resource that the caller identified in the Credential wants to access

func (m *PDP) TakeAuthnDecisionOPAStyle(decision Decision, r *http.Request, tokString string, tmfObject *TMFObject) (bool, error) {
	var err error

	// Verify the token and extract the claims.
	// A verification error stops processing. But an empy claim string '{}' is valid, unless the policies say otherwise later.
	mapClaims, _, err := m.getClaimsFromToken(tokString)
	if err != nil {
		return false, err
	}

	// The first argument to the rule engine will be the Request object, which will be composed of:
	//   - Some relevant fields of the received http.Request
	//   - Some fields of the Access Token
	requestArgument, err := StarDictFromHttpRequest(r)
	if err != nil {
		return false, err
	}

	// Add the type of TMF object and its id as top-level properties of the Request object
	tmfType := st.String(r.PathValue(("type")))
	tmfId := st.String(r.PathValue("id"))

	requestArgument.SetKey(st.String("tmf_entity"), st.String(tmfType))
	requestArgument.SetKey(st.String("tmf_id"), st.String(tmfId))

	// Enrich the http variable with other fields to make easier write rules
	var action string
	switch r.Method {
	case "GET":
		action = "READ"
	case "POST":
		action = "CREATE"
	case "PUT":
		action = "UPDATE"
	case "DELETE":
		action = "DELETE"
	}

	requestArgument.SetKey(st.String("action"), st.String(action))

	// Setup some fields about the remote User
	userOI := jpath.GetString(mapClaims, "vc.credentialSubject.mandate.mandator.organizationIdentifier")
	userDict := &st.Dict{}
	userDict.SetKey(st.String("organizationIdentifier"), st.String(userOI))

	if userOI == "" || tmfObject == nil {
		userDict.SetKey(st.String("isOwner"), st.Bool(false))
	} else {
		userDict.SetKey(st.String("isOwner"), st.Bool(userOI == tmfObject.OrganizationIdentifier))
	}

	requestArgument.SetKey(st.String("user"), userDict)

	// The second argument will be the Access Token, representing the User and his/her powers
	tokenArgument, err := mapToStarlark(mapClaims)
	if err != nil {
		return false, err
	}

	// The third argument will be the TMF object that the User wants to access
	var oMap map[string]any
	if tmfObject == nil {
		oMap = map[string]any{}
		oMap["type"] = "unknown"
		oMap["organizationIdentifier"] = "unknown"
	} else {
		oMap = tmfObject.ContentMap
		oMap["type"] = tmfObject.Type
		oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier
	}

	// tmfArgument, err := mapToStarlark(oMap)
	// if err != nil {
	// 	return false, err
	// }

	tmfArgument := StarTMFMap(oMap)

	// Get a Starlark Thread to execute the evaluation of the policies
	ent := m.threadPool.Get()
	if ent == nil {
		return false, fmt.Errorf("getting a thread entry from pool")
	}
	defer m.threadPool.Put(ent)

	te := ent.(*threadEntry)
	if te == nil {
		return false, fmt.Errorf("getting a thread entry from pool")
	}

	te.thread.SetLocal("httprequest", r)

	// Create the input arguments

	// Build the arguments to the StarLark function
	var args st.Tuple
	args = append(args, requestArgument) // The http.Request
	args = append(args, tokenArgument)   // The LEARCredential inside the Access Token
	args = append(args, tmfArgument)     // The TMForum object, on GETs. Nothing in other requests

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
		return false, fmt.Errorf("getting a thread entry from pool")
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

// getClaimsFromRequest verifies the Access Token received with the request, and extracts the claims in its payload.
// The most important claim in the payload is the LEARCredential that was used for authentication.
func (m *PDP) getClaimsFromToken(tokString string) (claims map[string]any, found bool, err error) {
	var token *jwt.Token
	var theClaims = MapClaims{}

	if tokString == "" {
		return nil, false, nil
	}

	verifierPublicKeyFunc := func(*jwt.Token) (interface{}, error) {
		if m.verifierJWK == nil {
			slog.Error("verifierJWK not initialized")
			return nil, fmt.Errorf("verifierJWK not initialized")
		}
		slog.Debug("publicKeyFunc", "key", m.verifierJWK)
		return m.verifierJWK.Key, nil
	}

	// Validate and verify the token
	token, err = jwt.NewParser().ParseWithClaims(tokString, &theClaims, verifierPublicKeyFunc)
	if err != nil {
		return nil, false, err
	}

	jwtmapClaims := token.Claims.(*MapClaims)

	return *jwtmapClaims, true, nil
}
