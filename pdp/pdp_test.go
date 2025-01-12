// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hesusruiz/domeproxy/constants"
	"github.com/hesusruiz/domeproxy/tmfsync"
	"gitlab.com/greyxor/slogor"
	st "go.starlark.net/starlark"
)

func TestPDP_TakeAuthnDecision(t *testing.T) {
	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelInfo), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

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

	tmfConfig := tmfsync.DefaultConfig(
		constants.DOME_PRO,
	)

	tmf, err := tmfsync.New(tmfConfig)
	if err != nil {
		panic(err)
	}
	defer tmf.Close()

	type args struct {
		decision        Decision
		requestArgument StarTMFMap
		tokenArgument   StarTMFMap
		tmfArgument     StarTMFMap
		readFileFun     func(name string) ([]byte, bool, error)
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "OK acceso via path",
			args: args{
				readFileFun: func(name string) ([]byte, bool, error) {
					return []byte(`
def authorize():
    print("Inside Authorize")
    print("input.request", input.request)
    print("input.request.path", input.request.path)
    print("input.request.path[1]", input.request.path[1])

    return True     `), true, nil
				},
				requestArgument: StarTMFMap{
					"path": []any{
						st.String("primero"),
						st.String("segundo"),
						st.String("tercero"),
					},
				},
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			m, err := NewPDP(
				constants.DOME_PRO,
				tt.name,
				true,
				tt.args.readFileFun,
				verificationKeyFunc,
			)
			if err != nil {
				panic(err)
			}

			got, err := m.TakeAuthnDecision(tt.args.decision, tt.args.requestArgument, tt.args.tokenArgument, tt.args.tmfArgument)
			if (err != nil) != tt.wantErr {
				t.Errorf("PDP.TakeAuthnDecision() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PDP.TakeAuthnDecision() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkAuthDecision(b *testing.B) {

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelInfo), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))

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

	tmfConfig := tmfsync.DefaultConfig(
		constants.DOME_PRO,
	)

	tmf, err := tmfsync.New(tmfConfig)
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

	m, err := NewPDP(
		constants.DOME_PRO,
		"test1",
		false,
		readFileFun,
		verificationKeyFunc,
	)
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		requestArgument := StarTMFMap{
			"path": []any{
				st.String("primero"),
				st.String("segundo"),
				st.String("tercero"),
			},
		}
		tokenArgument := StarTMFMap{}
		tmfArgument := StarTMFMap{}

		m.TakeAuthnDecision(Authorize, requestArgument, tokenArgument, tmfArgument)
	}

}
