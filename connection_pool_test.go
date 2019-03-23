package memcached

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	cp *ConnectionPool
	ss = []Server{
		{
			Host:  "memcached_1",
			Port:  11211,
			Alias: "s1",
		},
		{
			Host:  "memcached_2",
			Port:  11211,
			Alias: "s2",
		},
		{
			Host:  "memcached_3",
			Port:  11211,
			Alias: "s3",
		},
		{
			Host:  "memcached_4",
			Port:  11211,
			Alias: "s4",
		},
	}
)

func TestMain(m *testing.M) {
	cp = New(ss, "cache#")
	cp.SetConnMaxOpen(100)
	cp.SetFailover(true)
	if err := cp.FlushAll(); err != nil {
		log.Fatalf("Failed FlushAll: %+v", err)
	}
	code := m.Run()
	cp.Close()
	os.Exit(code)
}

func TestConnectionPool_MaybeOpenNewConnections(t *testing.T) {
	_cp := New(ss, "")
	_cp.SetConnMaxOpen(10)
	_cp.mu.Lock()
	req := make(chan connRequest, 1)
	reqKey := _cp.nextRequest
	_cp.nextRequest++
	_cp.connRequests[reqKey] = req
	_cp.maybeOpenNewConnections()
	_cp.mu.Unlock()
}

func TestConnectionPool_OpenNewConnection(t *testing.T) {
	_cp := New(ss, "")
	defer _cp.Close()
	_cp.openNewConnection()

	// already close
	cpClose := New(ss, "")
	defer cpClose.Close()
	cpClose.mu.Lock()
	cpClose.closed = true
	cpClose.mu.Unlock()
	cpClose.openNewConnection()
}

func TestConnectionPool_PutConnLocked(t *testing.T) {
	_cp := New(ss, "")
	defer _cp.Close()
	cn, err := _cp._conn(context.Background(), true)
	if err != nil {
		t.Fatalf("Failed _conn: %+v", err)
	}
	_cp.mu.Lock()
	_cp.putConnLocked(cn, err)
	_cp.mu.Unlock()

	cn, err = _cp._conn(context.Background(), true)
	if err != nil {
		t.Fatalf("Failed _conn: %+v", err)
	}
	_cp.mu.Lock()
	req := make(chan connRequest, 1)
	reqKey := _cp.nextRequest
	_cp.nextRequest++
	_cp.connRequests[reqKey] = req
	_cp.putConnLocked(cn, err)
	_cp.mu.Unlock()
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
