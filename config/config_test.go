// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package config

import "testing"

func TestFromIdToResourceName(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test3NOK",
			args: args{
				id: "urn:ngsi-ld::32611feb-6f78-4ccd-a4a2-547cb01cf33d",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "test2NOK",
			args: args{
				id: "urn:ngsi-ld:-:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "test4OK",
			args: args{
				id: "urn:ngsi-ld:product:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
			},
			want:    "product",
			wantErr: false,
		},
		{
			name: "test3OK",
			args: args{
				id: "urn:ngsi-ld:product-Offering-price:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
			},
			want:    "productOfferingPrice",
			wantErr: false,
		},
		{
			name: "test1OK",
			args: args{
				id: "urn:ngsi-ld:product-offering-price:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
			},
			want:    "productOfferingPrice",
			wantErr: false,
		},
		{
			name: "test2OK",
			args: args{
				id: "urn:ngsi-ld:product-offering-:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
			},
			want:    "productOffering",
			wantErr: false,
		},
		{
			name: "test1NOK",
			args: args{
				id: "ur:ngsi-ld:product-offering-price:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FromIdToResourceType(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("FromIdToResourceName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FromIdToResourceName() = %v, want %v", got, tt.want)
			}
		})
	}
}
