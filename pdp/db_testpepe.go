package pdp

import (
	"net/url"
	"testing"
)

func Test_buildWhereFromParms(t *testing.T) {
	type args struct {
		pr string
		qv url.Values
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test1",
			args: args{
				pr: "productOffering",
				qv: url.Values{
					"offset":          []string{"1"},
					"limit":           []string{"55"},
					"uno":             []string{"uno_uno"},
					"lifecycleStatus": []string{"Launched"},
					"dos":             []string{"dos_uno", "dos_dos"},
					"tres":            []string{"tres_uno", "tres_dos"},
				},
			},
			want: "SELECT * FROM tmfobject WHERE type = ? AND lifecycleStatus = ? AND content->>'$.dos' IN (?, ?) AND content->>'$.tres' IN (?, ?) AND content->>'$.uno' = ? ORDER BY updated DESC LIMIT 55 OFFSET 1",
		},
		{
			name: "Test2",
			args: args{
				pr: "",
				qv: url.Values{
					"offset":          []string{"1"},
					"limit":           []string{"55"},
					"uno":             []string{"uno_uno"},
					"lifecycleStatus": []string{"Launched"},
					"dos":             []string{"dos_uno", "dos_dos"},
					"tres":            []string{"tres_uno", "tres_dos"},
				},
			},
			want: "SELECT * FROM tmfobject WHERE content->>'$.uno' = ? AND lifecycleStatus = ? AND content->>'$.dos' IN (?, ?) AND content->>'$.tres' IN (?, ?) ORDER BY updated DESC LIMIT 55 OFFSET 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, args := buildSelectFromParms(tt.args.pr, tt.args.qv); got != tt.want {
				_ = args
				t.Errorf("buildWhereFromParms() = %v, want %v", got, tt.want)
			}
		})
	}
}
