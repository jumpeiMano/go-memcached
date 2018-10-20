package memcached

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	cp *ConnectionPool
)

func TestMain(m *testing.M) {
	ss := []Server{
		{
			Host:  "127.0.0.1",
			Port:  11211,
			Alias: "s1",
		},
		{
			Host:  "127.0.0.1",
			Port:  11212,
			Alias: "s2",
		},
		{
			Host:  "127.0.0.1",
			Port:  11213,
			Alias: "s3",
		},
		{
			Host:  "127.0.0.1",
			Port:  11214,
			Alias: "s4",
		},
	}
	cp = New(ss, "cache#")
	if err := cp.FlushAll(); err != nil {
		log.Fatalf("Failed FlushAll: %+v", err)
	}
	cp.SetConnMaxOpen(100)
	code := m.Run()
	cp.Close()
	os.Exit(code)
}

func TestConnectionPool_SetConnMaxLifetime(t *testing.T) {
	lifetime := 10 * time.Second
	cp.SetConnMaxLifetime(lifetime)
	assert.Equal(t, lifetime, cp.maxLifetime)
}

func TestConnectionPool_SetConnectTimeout(t *testing.T) {
	timeout := 3 * time.Second
	cp.SetConnectTimeout(timeout)
	assert.Equal(t, timeout, cp.connectTimeout)
}

func TestConnectionPool_SetPollTimeout(t *testing.T) {
	timeout := 1 * time.Second
	cp.SetPollTimeout(timeout)
	assert.Equal(t, timeout, cp.pollTimeout)
}
