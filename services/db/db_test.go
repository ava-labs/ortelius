package db

import (
	"strings"
	"testing"

	"github.com/ava-labs/ortelius/cfg"
)

func TestNewErrors(t *testing.T) {
	conn, err := New(nil, cfg.DB{
		Driver: "mysql",
		DSN:    "---",
	})

	if conn != nil {
		t.Fatal("Expected conn to be nil")
	}
	if !strings.HasPrefix(err.Error(), "invalid DSN") {
		t.Fatal("Expected an invalid DSN error")
	}

	conn, err = New(nil, cfg.DB{
		Driver: "mysql",
		DSN:    "::a.a.a.a.a.a.a.a::",
	})

	if conn != nil {
		t.Fatal("Expected conn to be nil")
	}
	if !strings.HasSuffix(err.Error(), "missing protocol scheme") {
		t.Fatal("Expected an invalid URI")
	}

	conn, err = New(nil, cfg.DB{
		Driver: "mysql",
		DSN:    "a:b@tcp(1.2.3.4)/foo",
	})
	if conn != nil {
		t.Fatal("Expected conn to be nil")
	}

	if err == nil {
		t.Fatal("Expected i/o or context deadline timeout")
	}
}
