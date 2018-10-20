package memcached

import (
	"os"
	"testing"
)

var (
	cp *ConnectionPool
)

func TestMain(m *testing.M) {
	ss := []Server{
		{
			Host:  "localhost",
			Port:  11211,
			Alias: "s1",
		},
		{
			Host:  "localhost",
			Port:  11212,
			Alias: "s2",
		},
		{
			Host:  "localhost",
			Port:  11213,
			Alias: "s3",
		},
		{
			Host:  "localhost",
			Port:  11214,
			Alias: "s4",
		},
	}
	cp = New(ss, "cache#")
	code := m.Run()
	cp.Close()
	os.Exit(code)
}
