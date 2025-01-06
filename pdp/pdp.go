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

	"github.com/hesusruiz/domeproxy/tmfsync"
	starjson "go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	"go.starlark.net/lib/time"
	"go.starlark.net/repl"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"
)

// X-Original-URI $request_uri;
// X-Original-Method $request_method
// X-Original-Remote-Addr $remote_addr;
// X-Original-Host $host;

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

// PDP implements a simple Policy Decision Point in Starlark
type PDP struct {

	// The globals for the Starlark program
	globals              starlark.StringDict
	thread               *starlark.Thread
	authenticateFunction *starlark.Function
	authorizeFunction    *starlark.Function

	// The name of the Starlark script file.
	scriptname string
}

func NewPDP(fileName string) (*PDP, error) {

	// Create a StarLark module with our own utility functions
	var Module = &starlarkstruct.Module{
		Name: "star",
		Members: starlark.StringDict{
			"getbody": starlark.NewBuiltin("getbody", getRequestBody),
		},
	}

	// Set the global Starlark environment with required modules, including our own
	starlark.Universe["json"] = starjson.Module
	starlark.Universe["time"] = time.Module
	starlark.Universe["math"] = math.Module
	starlark.Universe["star"] = Module

	m := &PDP{}
	m.scriptname = fileName
	err := m.ParseAndCompileFile()
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ParseAndCompileFile reads a file with Starlark code and compiles it, storing the resulting global
// dictionary for later usage. In particular, the compiled module should define two functions,
// one for athentication and the second for athorisation.
// ParseAndCompileFile can be called several times and will perform a new compilation every time,
// creating a new Thread and so the old ones will never be called again and eventually will be disposed.
func (m *PDP) ParseAndCompileFile() error {
	var err error

	// The compiled program context will be stored in a new Starlark thread for each invocation
	m.thread = &starlark.Thread{
		Load:  repl.MakeLoadOptions(&syntax.FileOptions{}),
		Print: func(_ *starlark.Thread, msg string) { slog.Info("rules => " + msg) },
		Name:  "exec " + m.scriptname,
	}

	// Create a predeclared environment specific for this module (empy for the moment)
	predeclared := make(starlark.StringDict)

	// Parse and execute the top-level commands in the script file
	m.globals, err = starlark.ExecFileOptions(&syntax.FileOptions{}, m.thread, m.scriptname, nil, predeclared)
	if err != nil {
		log.Println("error compiling Starlark program")
		return err
	}

	// There should be two functions: 'authenticate' and 'authorize', called at the proper moments

	m.authenticateFunction, err = m.getGlobalFunction("authenticate")
	if err != nil {
		return err
	}

	m.authorizeFunction, err = m.getGlobalFunction("authorize")
	if err != nil {
		return err
	}

	return nil

}

// getGlobalFunction retrieves a global with the specified name, requiring it to be a Callable
func (m PDP) getGlobalFunction(funcName string) (*starlark.Function, error) {

	// Check that we have the function
	f, ok := m.globals[funcName]
	if !ok {
		err := fmt.Errorf("missing definition of %s", funcName)
		log.Println(err.Error())
		return nil, err
	}

	// Check that is is a Callable
	starFunction, ok := f.(*starlark.Function)
	if !ok {
		err := fmt.Errorf("expected a Callable but got %v", f.Type())
		log.Println(err.Error())
		return nil, err
	}

	return starFunction, nil
}

// TakeAuthnDecision is called when a decision should be taken for either Athentication or Authorization.
// The type of decision to evaluate is passed in the Decision argument. The rest of the arguments contain the information required
// for the decision. They are:
// - the Verifiable Credential with the information from the caller needed for the decision
// - the protected resource that the caller identified in the Credential wants to access
func (m PDP) TakeAuthnDecision(decision Decision, r *http.Request, claimsString string, tmfObject *tmfsync.TMFObject) (bool, error) {
	var err error
	debug := true

	claimsArgument, err := JsonToStarlark(claimsString, nil)
	if err != nil {
		return false, err
	}

	// tmfArgument := &starlark.Dict{}
	// for key, value := range tmfObject {
	// 	tmfArgument.SetKey(starlark.String(key), starlark.Value(value))
	// }

	oMap := tmfObject.ContentMap
	oMap["type"] = tmfObject.Type
	oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier

	tmfArgument := StarTMF(oMap)

	// In development, parse and compile the script on every request
	if debug {
		err := m.ParseAndCompileFile()
		if err != nil {
			return false, err
		}
	}

	m.thread.SetLocal("httprequest", r)

	// Create the input arguments

	tmfType := starlark.String(r.PathValue(("type")))
	id := starlark.String(r.PathValue("id"))

	httpRequest, err := StarDictFromHttpRequest(r)
	if err != nil {
		return false, err
	}

	// Add the type of TMF object and its id
	httpRequest.SetKey(starlark.String("tmf_type"), starlark.String(tmfType))
	httpRequest.SetKey(starlark.String("id"), starlark.String(id))

	// Build the arguments to the StarLark function
	var args starlark.Tuple
	args = append(args, httpRequest)
	args = append(args, claimsArgument)
	args = append(args, tmfArgument)

	// Call the corresponding function in the Starlark Thread
	var result starlark.Value
	if decision == Authenticate {
		// Call the 'authenticate' funcion
		result, err = starlark.Call(m.thread, m.authenticateFunction, args, nil)
	} else {
		// Call the 'authorize' function
		result, err = starlark.Call(m.thread, m.authorizeFunction, args, nil)
	}

	if err != nil {
		fmt.Printf("rules ERROR: %s\n", err.(*starlark.EvalError).Backtrace())
		return false, fmt.Errorf("error calling function: %w", err)
	}

	// Check that the value returned is of the correct type (boolean)
	resultType := result.Type()
	if resultType != "bool" {
		err := fmt.Errorf("function returned wrong type: %v", resultType)
		return false, err
	}

	// Return the value as a Go boolean
	return bool(result.(starlark.Bool).Truth()), nil

}

func getRequestBody(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {

	// Get the current HTTP request being processed
	r := thread.Local("httprequest")
	request, ok := r.(*http.Request)
	if !ok {
		return starlark.None, fmt.Errorf("no request found in thread locals")
	}

	// Read the body from the request and store in thread locals in case we need it later
	bytes, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	thread.SetLocal("requestbody", bytes)

	// Return string for the Starlark script
	body := starlark.String(bytes)

	return body, nil
}

func StarDictFromHttpRequest(request *http.Request) (*starlark.Dict, error) {

	dd := &starlark.Dict{}

	dd.SetKey(starlark.String("method"), starlark.String(request.Method))
	dd.SetKey(starlark.String("url"), starlark.String(request.URL.String()))
	dd.SetKey(starlark.String("path"), starlark.String(request.URL.Path))
	dd.SetKey(starlark.String("query"), getDictFromValues(request.URL.Query()))

	dd.SetKey(starlark.String("host"), starlark.String(request.Host))
	dd.SetKey(starlark.String("content_length"), starlark.MakeInt(int(request.ContentLength)))
	dd.SetKey(starlark.String("headers"), getDictFromHeaders(request.Header))

	return dd, nil
}

func getDictFromValues(values map[string][]string) *starlark.Dict {
	dict := &starlark.Dict{}
	for key, values := range values {
		dict.SetKey(starlark.String(key), getSkylarkList(values))
	}
	return dict
}

func getDictFromHeaders(headers http.Header) *starlark.Dict {
	dict := &starlark.Dict{}
	for key, values := range headers {
		dict.SetKey(starlark.String(key), getSkylarkList(values))
	}
	return dict
}

func getSkylarkList(values []string) *starlark.List {
	list := &starlark.List{}
	for _, v := range values {
		list.Append(starlark.String(v))
	}
	return list
}

type StarTMF map[string]any

func (s StarTMF) String() string        { return s["id"].(string) }
func (s StarTMF) GoString() string      { return s["id"].(string) }
func (s StarTMF) Type() string          { return "tmfobject" }
func (s StarTMF) Freeze()               {} // immutable
func (s StarTMF) Truth() starlark.Bool  { return len(s) > 0 }
func (s StarTMF) Hash() (uint32, error) { return hashString(s["id"].(string)), nil }
func (s StarTMF) Len() int              { return len(s) } // number of entries

func (s StarTMF) Attr(name string) (starlark.Value, error) {
	value, ok := s[name]
	if ok {
		return starlark.String(value.(string)), nil
	} else {
		return nil, nil
	}
}

func (s StarTMF) AttrNames() []string {
	var keys []string
	for key := range s {
		keys = append(keys, key)
	}
	return keys
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
