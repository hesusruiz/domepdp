// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.
package pdp

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"gitlab.com/greyxor/slogor"
)

var testJWK = `{
	"kty": "EC",
	"use": "sig",
	"crv": "P-256",
	"kid": "did:key:zDnaeVYnWTZu5nbrH1qmBVMvNwSrtKnkRbCZ4xH5h2LQPnzdr",
	"x": "TAmV5htgfwIOjgaDENCqSKUOsYvmIW_dHPXtYNpa-GU",
	"y": "OOxoUKEbvt-GZqc2296Kdxr6Ez4osae77J6T-JllKkA"
}
`

func verificationKeyFunc(environment Environment) (*jose.JSONWebKey, error) {
	k := &jose.JSONWebKey{}
	err := k.UnmarshalJSON([]byte(testJWK))
	if err != nil {
		return nil, err
	}
	return k, nil
}

func TestPDP_Handler(t *testing.T) {
	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

	// Change to the root directory of the project
	cd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	cd = strings.TrimSuffix(cd, "/pdp")
	err = os.Chdir(cd)
	if err != nil {
		panic(err)
	}
	cd, _ = os.Getwd()
	print(cd)

	tmfConfig := DefaultConfig(
		DOME_PRO,
	)

	tmf, err := NewTMFdb(tmfConfig)
	if err != nil {
		panic(err)
	}
	defer tmf.Close()

	type args struct {
		url         string
		readFileFun func(name string) ([]byte, bool, error)
		setHeaders  bool
	}
	tests := []struct {
		name           string
		args           args
		wantStatusCode int
		wantErr        bool
	}{
		{
			name: "ERROR_X-Original-URI missing",
			args: args{
				setHeaders: false,
				url:        "https://dome-marketplace.org/catalog/productSpecification/urn:ngsi-ld:product-specification:7e7b7dea-b79c-49d4-9b3f-0f5126460534",
				readFileFun: func(name string) ([]byte, bool, error) {
					return []byte(`
def authorize(input):
    a = input["request"]
    print(a)
    return True     `), true, nil
				},
			},
			wantStatusCode: http.StatusForbidden,
			wantErr:        true,
		},
		{
			name: "OK acceso via path",
			args: args{
				setHeaders: true,
				url:        "https://dome-marketplace.org/catalog/productSpecification/urn:ngsi-ld:product-specification:7e7b7dea-b79c-49d4-9b3f-0f5126460534",
				readFileFun: func(name string) ([]byte, bool, error) {
					return []byte(`
def authorize():
    print("Inside Authorize")
    print("input.request", input.request)
    print("input.request.path", input.request.path)
    print("input.request.path[1]", input.request.path[1])

    return True     `), true, nil
				},
			},
			wantStatusCode: http.StatusOK,
			wantErr:        true,
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			logger := slog.Default()

			rulesEngine, err := NewPDP(
				DOME_PRO,
				tt.name,
				true,
				tt.args.readFileFun,
				verificationKeyFunc,
			)
			if err != nil {
				panic(err)
			}

			handler := HandleGETAuthorization(logger, tmf, rulesEngine)
			req := httptest.NewRequest("GET", tt.args.url, nil)

			if tt.args.setHeaders {
				req.Header.Set("X-Original-URI", tt.args.url)
				req.Header.Set("X-Original-Method", "GET")
			}

			w := httptest.NewRecorder()
			handler(w, req)
			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)

			fmt.Println(resp.StatusCode)
			fmt.Println(resp.Header.Get("Content-Type"))
			fmt.Println(string(body))

			if resp.StatusCode != tt.wantStatusCode {
				t.Errorf("TakeAuthnDecision status = %v, wantStatus %v", resp.StatusCode, tt.wantStatusCode)
				return
			}

		})
	}
}

func BenchmarkPDPhandler(b *testing.B) {

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelInfo), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))
	logger := slog.Default()

	// Change to the root directory of the project
	cd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	cd = strings.TrimSuffix(cd, "/pdp")
	err = os.Chdir(cd)
	if err != nil {
		panic(err)
	}
	cd, _ = os.Getwd()
	print(cd)

	tmfConfig := DefaultConfig(
		DOME_PRO,
	)

	tmf, err := NewTMFdb(tmfConfig)
	if err != nil {
		panic(err)
	}
	defer tmf.Close()

	readFileFun := func(name string) ([]byte, bool, error) {
		return []byte(`
def authorize():
    a = input.request
    b = input.request.path
    c = input.request.path[1]
    if c == "segundo":
        return True

    return True     `), true, nil
	}

	rulesEngine, err := NewPDP(
		DOME_PRO,
		"test1",
		false,
		readFileFun,
		verificationKeyFunc,
	)
	if err != nil {
		panic(err)
	}

	url := "https://dome-marketplace.org/catalog/productSpecification/urn:ngsi-ld:product-specification:7e7b7dea-b79c-49d4-9b3f-0f5126460534"

	for i := 0; i < b.N; i++ {

		handler := HandleGETAuthorization(logger, tmf, rulesEngine)
		req := httptest.NewRequest("GET", url, nil)

		req.Header.Set("X-Original-URI", url)
		req.Header.Set("X-Original-Method", "GET")

		w := httptest.NewRecorder()
		handler(w, req)
		resp := w.Result()
		body, _ := io.ReadAll(resp.Body)

		fmt.Println(resp.StatusCode)
		fmt.Println(resp.Header.Get("Content-Type"))
		fmt.Println(string(body))

		if resp.StatusCode != http.StatusOK {
			panic("status is error")
		}

	}

}

func Test_getRestrictionElements(t *testing.T) {
	type args struct {
		objectName string
		concept    string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "official DOME example",
			args: args{
				objectName: "testdata/restrictions/example_DOME.json",
				concept:    "permittedLegalRegion",
			},
			want: []string{"FR"},
		},
		{
			name: "several permitted countries",
			args: args{
				objectName: "testdata/restrictions/several_permitted_countries.json",
				concept:    "permittedLegalRegion",
			},
			want: []string{"FR", "ES", "RU", "IT"},
		},
		{
			name: "several forbidden countries",
			args: args{
				objectName: "testdata/restrictions/several_forbidden_countries.json",
				concept:    "forbiddenLegalRegion",
			},
			want: []string{"FR", "ES", "RU", "IT"},
		},
		{
			name: "no permitted countries",
			args: args{
				objectName: "testdata/restrictions/no_permitted_countries.json",
				concept:    "permittedLegalRegion",
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			object := readTest(tt.args.objectName)
			got := getRestrictionElements(object, tt.args.concept)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getRestrictionElements() = %v, want %v", got, tt.want)
			}
		})
	}
}

func readTest(fileName string) map[string]any {
	buf, err := os.ReadFile(fileName)
	if err != nil {
		return nil
	}
	var object map[string]any
	err = json.Unmarshal(buf, &object)
	if err != nil {
		return nil
	}
	return object
}
