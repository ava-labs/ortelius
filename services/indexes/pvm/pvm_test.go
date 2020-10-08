package pvm

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/gocraft/health"

	"github.com/ava-labs/ortelius/cfg"
)

const testNetworkID = 0

func TestBootstrap(t *testing.T) {
	w, closeFn := newTestWriter(t)
	defer closeFn()

	if err := w.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func newTestWriter(t *testing.T) (*Writer, func()) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal("Failed to create miniredis server:", err.Error())
	}

	conf := cfg.Services{
		DB: &cfg.DB{
			TXDB:   true,
			Driver: "mysql",
			DSN:    "root:password@tcp(127.0.0.1:3306)/ortelius_test?parseTime=true",
		},
		Redis: &cfg.Redis{
			Addr: s.Addr(),
		},
	}

	w, err := NewWriter(conf, testNetworkID)
	if err != nil {
		t.Fatal("Failed to bootstrap index:", err.Error())
	}

	w.stream = health.NewStream()

	return w, func() {
		s.Close()
		w.Close(context.Background())
	}
}
