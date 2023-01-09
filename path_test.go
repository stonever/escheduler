package escheduler

import (
	"reflect"
	"testing"
)

func TestParseTaskAbbrFromTaskKey(t *testing.T) {
	type args struct {
		key string
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
				key: "/20220624/task/192.168.193.131-125075/10",
			},
			want:    "10",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTaskAbbrFromTaskKey(tt.args.key)
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

func TestParseTaskFromKV(t *testing.T) {
	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name     string
		args     args
		wantTask Task
		wantErr  bool
	}{
		{
			name: "parse task from kv",
			args: args{
				key:   []byte("/20220624/task/192.168.193.131-125075/10"),
				value: []byte("raw task"),
			},
			wantTask: Task{ID: "10", Raw: []byte("raw task")},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTask, err := ParseTaskFromValue(tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTaskFromKV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotTask, tt.wantTask) {
				t.Errorf("ParseTaskFromKV() gotTask = %v, want %v", gotTask, tt.wantTask)
			}
		})
	}
}

func TestParseWorkerFromTaskKey(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseWorkerFromTaskKey(tt.args.key); got != tt.want {
				t.Errorf("ParseWorkerFromTaskKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseWorkerFromWorkerKey(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWorkerFromWorkerKey(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseWorkerFromWorkerKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseWorkerFromWorkerKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}
