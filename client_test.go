package memcached

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	cl *Client
	ss = []Server{
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
)

func TestMain(m *testing.M) {
	cl = New(ss, "cache#")
	cl.SetConnMaxOpen(100)
	cl.SetFailover(true)
	if err := cl.FlushAll(); err != nil {
		log.Fatalf("Failed FlushAll: %+v", err)
	}
	code := m.Run()
	cl.Close()
	os.Exit(code)
}

func Test_SetConnMaxLifetime(t *testing.T) {
	lifetime := 10 * time.Second
	cl.SetConnMaxLifetime(lifetime)
	assert.Equal(t, lifetime, cl.maxLifetime)
}

func Test_SetConnectTimeout(t *testing.T) {
	timeout := 3 * time.Second
	cl.SetConnectTimeout(timeout)
	assert.Equal(t, timeout, cl.connectTimeout)
}

func Test_SetPollTimeout(t *testing.T) {
	timeout := 1 * time.Second
	cl.SetPollTimeout(timeout)
	assert.Equal(t, timeout, cl.pollTimeout)
}
