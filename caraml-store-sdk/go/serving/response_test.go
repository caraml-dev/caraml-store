package serving

import (
	"fmt"
	servingproto "github.com/caraml-dev/caraml-store/caraml-store-sdk/go/protos/feast/serving"
	typesproto "github.com/caraml-dev/caraml-store/caraml-store-sdk/go/protos/feast/types"
	"github.com/google/go-cmp/cmp"
	"testing"
)

var response = OnlineFeaturesResponse{
	RawResponse: &servingproto.GetOnlineFeaturesResponse{
		Metadata: &servingproto.GetOnlineFeaturesResponseMetadata{
			FieldNames: &servingproto.FieldList{
				Val: []string{
					"featuretable1:feature1",
					"featuretable1:feature2",
				},
			},
		},
		Results: []*servingproto.GetOnlineFeaturesResponse_FieldVector{
			{
				Values: []*typesproto.Value{
					Int64Val(1),
					{},
				},
				Statuses: []servingproto.FieldStatus{
					servingproto.FieldStatus_PRESENT,
					servingproto.FieldStatus_NULL_VALUE,
				},
			},
			{
				Values: []*typesproto.Value{
					Int64Val(2),
					Int64Val(2),
				},
				Statuses: []servingproto.FieldStatus{
					servingproto.FieldStatus_PRESENT,
					servingproto.FieldStatus_PRESENT,
				},
			},
		},
	},
}

func TestOnlineFeaturesResponseToRow(t *testing.T) {
	actual := response.Rows()
	expected := []Row{
		{"featuretable1:feature1": Int64Val(1), "featuretable1:feature2": &typesproto.Value{}},
		{"featuretable1:feature1": Int64Val(2), "featuretable1:feature2": Int64Val(2)},
	}
	if len(expected) != len(actual) {
		t.Errorf("expected: %v, got: %v", expected, actual)
	}
	for i := range expected {
		if !expected[i].equalTo(actual[i]) {
			t.Errorf("expected: %v, got: %v", expected, actual)
		}
	}
}

func TestOnlineFeaturesResponseToStatuses(t *testing.T) {
	actual := response.Statuses()
	expected := []map[string]servingproto.FieldStatus{
		{
			"featuretable1:feature1": servingproto.FieldStatus_PRESENT,
			"featuretable1:feature2": servingproto.FieldStatus_NULL_VALUE,
		},
		{
			"featuretable1:feature1": servingproto.FieldStatus_PRESENT,
			"featuretable1:feature2": servingproto.FieldStatus_PRESENT,
		},
	}
	if len(expected) != len(actual) {
		t.Errorf("expected: %v, got: %v", expected, actual)
	}
	for i := range expected {
		if !cmp.Equal(expected[i], actual[i]) {
			t.Errorf("expected: %v, got: %v", expected, actual)
		}
	}
}

func TestOnlineFeaturesResponseToInt64Array(t *testing.T) {
	type args struct {
		order  []string
		fillNa []int64
	}
	tt := []struct {
		name    string
		args    args
		want    [][]int64
		wantErr bool
		err     error
	}{
		{
			name: "valid",
			args: args{
				order:  []string{"featuretable1:feature2", "featuretable1:feature1"},
				fillNa: []int64{-1, -1},
			},
			want:    [][]int64{{-1, 1}, {2, 2}},
			wantErr: false,
		},
		{
			name: "length mismatch",
			args: args{
				order:  []string{"ft:feature2", "ft:feature1"},
				fillNa: []int64{-1},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf(ErrLengthMismatch, 1, 2),
		},
		{
			name: "length mismatch",
			args: args{
				order:  []string{"featuretable1:feature2", "featuretable1:feature3"},
				fillNa: []int64{-1, -1},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf(ErrFeatureNotFound, "featuretable1:feature3"),
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got, err := response.Int64Arrays(tc.args.order, tc.args.fillNa)
			if (err != nil) != tc.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if tc.wantErr && err.Error() != tc.err.Error() {
				t.Errorf("error = %v, expected err = %v", err, tc.err)
				return
			}
			if !cmp.Equal(got, tc.want) {
				t.Errorf("got: \n%v\nwant:\n%v", got, tc.want)
			}
		})
	}
}
