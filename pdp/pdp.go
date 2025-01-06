// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.
package pdp

import (
	"encoding/json"
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"log/slog"
	"net/http"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	"github.com/hesusruiz/domeproxy/tmfsync"
	starjson "go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	"go.starlark.net/lib/time"
	"go.starlark.net/repl"
	st "go.starlark.net/starlark"
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
	globals              st.StringDict
	thread               *st.Thread
	authenticateFunction *st.Function
	authorizeFunction    *st.Function
	scriptname           string
	verifierJWK          jose.JSONWebKey
	debug                bool
}

func NewPDP(environment tmfsync.Environment, fileName string) (*PDP, error) {

	// Create a StarLark module with our own utility functions
	var Module = &starlarkstruct.Module{
		Name: "star",
		Members: st.StringDict{
			"getbody": st.NewBuiltin("getbody", getRequestBody),
		},
	}

	// Set the global Starlark environment with required modules, including our own
	st.Universe["json"] = starjson.Module
	st.Universe["time"] = time.Module
	st.Universe["math"] = math.Module
	st.Universe["star"] = Module

	m := &PDP{}
	m.scriptname = fileName

	if err := m.ParseAndCompileFile(); err != nil {
		return nil, err
	}

	m.debug = true

	verifierConfig, err := NewOpenIDConfig(environment)
	if err != nil {
		return nil, err
	}

	m.verifierJWK, err = verifierConfig.VerificationJWK()
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
	m.thread = &st.Thread{
		Load:  repl.MakeLoadOptions(&syntax.FileOptions{}),
		Print: func(_ *st.Thread, msg string) { slog.Info("rules => " + msg) },
		Name:  "exec " + m.scriptname,
	}

	// Create a predeclared environment specific for this module (empy for the moment)
	predeclared := make(st.StringDict)

	// Parse and execute the top-level commands in the script file
	m.globals, err = st.ExecFileOptions(&syntax.FileOptions{}, m.thread, m.scriptname, nil, predeclared)
	if err != nil {
		log.Println("error compiling Starlark program")
		return err
	}

	m.globals.Freeze()
	// There should be two functions: 'authenticate' and 'authorize', called at the proper moments

	// m.authenticateFunction, err = m.getGlobalFunction("authenticate")
	// The authentication function is optional, for the moment
	// if err != nil {
	// 	return err
	// }

	m.authorizeFunction, err = m.getGlobalFunction("authorize")
	if err != nil {
		return err
	}

	return nil

}

// getGlobalFunction retrieves a global with the specified name, requiring it to be a Callable
func (m PDP) getGlobalFunction(funcName string) (*st.Function, error) {

	// Check that we have the function
	f, ok := m.globals[funcName]
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

// TakeAuthnDecision is called when a decision should be taken for either Athentication or Authorization.
// The type of decision to evaluate is passed in the Decision argument. The rest of the arguments contain the information required
// for the decision. They are:
// - the Verifiable Credential with the information from the caller needed for the decision
// - the protected resource that the caller identified in the Credential wants to access
func (m PDP) TakeAuthnDecision(decision Decision, r *http.Request, tokString string, tmfObject *tmfsync.TMFObject) (bool, error) {
	var err error

	// Verify the token and extract the claims.
	// A verification error stops processing. But an empy claim string '{}' is valid, unless the policies say otherwise later.
	mapClaims, _, _, err := m.getClaimsFromToken(tokString)
	if err != nil {
		return false, err
	}

	nativeMapClaims := map[string]any{}
	for k, v := range mapClaims {
		nativeMapClaims[k] = v
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
	userOI := jpath.GetString(nativeMapClaims, "vc.credentialSubject.mandate.mandator.organizationIdentifier")
	userDict := &st.Dict{}
	userDict.SetKey(st.String("organizationIdentifier"), st.String(userOI))
	userDict.SetKey(st.String("isOwner"), st.Bool(userOI == tmfObject.OrganizationIdentifier))

	requestArgument.SetKey(st.String("user"), userDict)

	// The second argument will be the Access Token, representing the User and his/her powers
	tokenArgument, err := mapToStarlark(mapClaims)
	if err != nil {
		return false, err
	}

	// // Convert the string to a native Starlark object
	// claimsArgument, err := JsonToStarlark(stringClaims, nil)
	// if err != nil {
	// 	return false, err
	// }

	// The third argument will be the TMF object that the User wants to access
	oMap := tmfObject.ContentMap
	oMap["type"] = tmfObject.Type
	oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier

	tmfArgument, err := mapToStarlark(oMap)
	if err != nil {
		return false, err
	}

	// tmfArgument := StarTMF(oMap)

	// In development, parse and compile the script on every request
	if m.debug {
		err := m.ParseAndCompileFile()
		if err != nil {
			return false, err
		}
	}

	m.thread.SetLocal("httprequest", r)

	// Create the input arguments

	// Build the arguments to the StarLark function
	var args st.Tuple
	args = append(args, requestArgument)
	args = append(args, tokenArgument)
	args = append(args, tmfArgument)

	// Call the corresponding function in the Starlark Thread
	var result st.Value
	if decision == Authenticate {
		// Call the 'authenticate' funcion
		result, err = st.Call(m.thread, m.authenticateFunction, args, nil)
	} else {
		// Call the 'authorize' function
		result, err = st.Call(m.thread, m.authorizeFunction, args, nil)
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

type StarTMF map[string]any

func (s StarTMF) String() string        { return s["id"].(string) }
func (s StarTMF) GoString() string      { return s["id"].(string) }
func (s StarTMF) Type() string          { return "tmfobject" }
func (s StarTMF) Freeze()               {} // immutable
func (s StarTMF) Truth() st.Bool        { return len(s) > 0 }
func (s StarTMF) Hash() (uint32, error) { return hashString(s["id"].(string)), nil }
func (s StarTMF) Len() int              { return len(s) } // number of entries

func (s StarTMF) Attr(name string) (st.Value, error) {
	value, ok := s[name]
	if ok {
		return st.String(value.(string)), nil
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

// getClaimsFromRequest verifies the Access Token received with the request, and extracts the claims in its payload.
// The most important claim in the payload is the LEARCredential that was used for authentication.
func (m *PDP) getClaimsFromToken(tokString string) (mapClaims jwt.MapClaims, stringClaims string, found bool, err error) {
	var token *jwt.Token
	stringClaims = "{}"

	if tokString == "" {
		return mapClaims, stringClaims, false, fmt.Errorf("no token")
	}

	publicKeyFunc := func(*jwt.Token) (interface{}, error) {
		return m.verifierJWK.Key, nil
	}

	token, err = jwt.NewParser().ParseWithClaims(tokString, jwt.MapClaims{}, publicKeyFunc)
	if err != nil {
		return mapClaims, stringClaims, false, err
	}

	mapClaims = token.Claims.(jwt.MapClaims)

	cl, err := json.Marshal(token.Claims)
	if err != nil {
		return mapClaims, stringClaims, false, err
	}
	stringClaims = string(cl)

	return mapClaims, stringClaims, true, nil

}
