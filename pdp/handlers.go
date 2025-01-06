package pdp

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"go.starlark.net/starlark"
)

type jsonrpcMessage struct {
	Version string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func (m PDP) JSONRPCHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	debug := true

	log.Println("in JSONRPC handler")

	// Check that this is a JSON request
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		http.Error(w, "invalid content type", http.StatusForbidden)
	}

	// Read and decode the body from the request and store in thread locals in case we need it later
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	var msg jsonrpcMessage
	err = json.Unmarshal(bytes, &msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	if msg.Version != "2.0" {
		http.Error(w, "invalid JSON-RPC version", http.StatusBadRequest)
	}
	if len(msg.Method) == 0 {
		http.Error(w, "JSON-RPC method not specified", http.StatusBadRequest)
	}
	m.thread.SetLocal("jsonmessage", m)

	// In development, parse and compile the script on every request
	if debug {
		err := m.ParseAndCompileFile()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	m.thread.SetLocal("httprequest", r)

	// Create the input argument
	req, err := StarDictFromHttpRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	req.SetKey(starlark.String("jsonrpc_method"), starlark.String(msg.Method))

	// Call the already compiled 'authenticate' funcion
	var args starlark.Tuple
	args = append(args, req)
	res, err := starlark.Call(m.thread, m.authenticateFunction, args, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Check that the value returned is of the correct type
	resultType := res.Type()
	if resultType != "string" {
		err := fmt.Errorf("authenticate function returned wrong type: %v", resultType)
		log.Println(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Return the value
	user := res.(starlark.String).GoString()

	if len(user) > 0 {
		w.Write([]byte("Authenticated"))
	} else {
		w.Write([]byte("Forbidden"))
	}

}

func (m PDP) HttpHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	debug := true

	log.Println("in HTTP handler")

	// Check that this is a JSON request
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		http.Error(w, "invalid content type", http.StatusForbidden)
		return
	}

	// Read and decode the body from the request and store in thread locals in case we need it later
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Decode the body as JSON to a Starlark Value
	tmfObject, err := JsonToStarlark(string(bytes), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var msg jsonrpcMessage
	err = json.Unmarshal(bytes, &msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if msg.Version != "2.0" {
		http.Error(w, "invalid JSON-RPC version", http.StatusBadRequest)
		return
	}
	if len(msg.Method) == 0 {
		http.Error(w, "JSON-RPC method not specified", http.StatusBadRequest)
		return
	}
	m.thread.SetLocal("jsonmessage", m)

	// In development, parse and compile the script on every request
	if debug {
		err := m.ParseAndCompileFile()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	m.thread.SetLocal("httprequest", r)

	// Create the input argument
	req, err := StarDictFromHttpRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	req.SetKey(starlark.String("body"), tmfObject)
	req.SetKey(starlark.String("jsonrpc_method"), starlark.String(msg.Method))

	// Call the already compiled 'authenticate' funcion
	var args starlark.Tuple
	args = append(args, req)
	res, err := starlark.Call(m.thread, m.authenticateFunction, args, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Check that the value returned is of the correct type
	resultType := res.Type()
	if resultType != "string" {
		err := fmt.Errorf("authenticate function returned wrong type: %v", resultType)
		log.Println(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return the value
	user := res.(starlark.String).GoString()

	if len(user) > 0 {
		w.Write([]byte("Authenticated"))
	} else {
		w.Write([]byte("Forbidden"))
	}

}
