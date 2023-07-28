package escheduler

import (
	"testing"
)

func TestParseTaskIDFromTaskKey(t *testing.T) {
	type args struct {
		rootName string
		key      string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "parse task from kv",
			args: args{
				rootName: "20220624",
				key:      "/20220624/task/192.168.193.131-125075/10",
			},
			want:    "10",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTaskIDFromTaskKey(tt.args.rootName, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTaskAbbrFromTaskKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseTaskAbbrFromTaskKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseWorkerIDFromTaskKey(t *testing.T) {
	type args struct {
		rootName string
		key      string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "parse worker from task key",
			args: args{
				rootName: "root",
				key:      "/root/task/192.168.193.131-125075/10",
			},
			want:    "192.168.193.131-125075",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWorkerIDFromTaskKey(tt.args.rootName, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTaskAbbrFromTaskKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseTaskAbbrFromTaskKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}
