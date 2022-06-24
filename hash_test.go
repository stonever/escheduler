package escheduler

import (
	log "github.com/sirupsen/logrus"
	"testing"
)

func Test_hash(t *testing.T) {
	type args struct {
		key        []byte
		numWorkers int32
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "hash 1",
			args: args{
				key:        []byte("abcdefgggg12"),
				numWorkers: 2,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := hash(tt.args.key, tt.args.numWorkers)
			if (err != nil) != tt.wantErr {
				t.Errorf("hash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			log.Infof("got:%v", got)
		})
	}
}
