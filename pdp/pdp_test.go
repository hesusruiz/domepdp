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

	tmfConfig := DefaultConfig(
		DOME_PRO,
	)

	tmf, err := NewTMFdb(tmfConfig)
	if err != nil {
		panic(err)
	}
	defer tmf.Close()

	type args struct {
		decision        Decision
		requestArgument StarTMFMap
		tokenArgument   StarTMFMap
		tmfArgument     StarTMFMap
		userArgument    StarTMFMap
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
				DOME_PRO,
				tt.name,
				true,
				tt.args.readFileFun,
				verificationKeyFunc,
			)
			if err != nil {
				panic(err)
			}

			input := StarTMFMap{
				"request": tt.args.requestArgument,
				"token":   tt.args.tokenArgument,
				"tmf":     tt.args.tmfArgument,
				"user":    tt.args.userArgument,
			}

			got, err := m.TakeAuthnDecision(tt.args.decision, input)
			if (err != nil) != tt.wantErr {
				t.Errorf("TakeAuthnDecision() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("TakeAuthnDecision() = %v, want %v", got, tt.want)
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

	tmfConfig := DefaultConfig(
		DOME_PRO,
	)

	tmf, err := NewTMFdb(tmfConfig)
	if err != nil {
		panic(err)
	}
	defer tmf.Close()

	m, err := NewPDP(
		DOME_PRO,
		"auth_policies.star",
		false,
		nil,
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
		userArgument := StarTMFMap{
			"isLEAR":  true,
			"country": "ES",
		}

		input := StarTMFMap{
			"request": requestArgument,
			"token":   tokenArgument,
			"tmf":     tmfArgument,
			"user":    userArgument,
		}

		m.TakeAuthnDecision(Authorize, input)
	}

}
