package memcached

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionPool_MaybeOpenNewConnections(t *testing.T) {
	_cl := New(ss, "")
	defer _cl.Close()
	_cl.SetConnMaxOpen(10)
	cp := _cl.cps[ss[0].getNodeName()]
	cp.mu.Lock()
	defer cp.mu.Unlock()
	req := make(chan connRequest, 1)
	reqKey := cp.nextRequest
	cp.nextRequest++
	cp.connRequests[reqKey] = req
	cp.maybeOpenNewConnections()
}

func TestConnectionPool_OpenNewConnection(t *testing.T) {
	_cl := New(ss, "")
	defer _cl.Close()
	cp := _cl.cps[ss[0].getNodeName()]
	cp.openNewConnection()

	// already close
	cpClose := _cl.cps[ss[1].getNodeName()]
	cpClose.mu.Lock()
	cpClose.closed = true
	cpClose.mu.Unlock()
	cpClose.openNewConnection()
}

func TestNewConn(t *testing.T) {
	_ss := []Server{
		{Host: "127.0.0.1", Port: 11211},
		{Host: "127.0.0.1", Port: 99999},
	}
	_cl := New(_ss, "")
	defer _cl.Close()
	_cl.SetLogger(log.Printf)
	cp1 := _cl.cps[_ss[0].getNodeName()]
	cp2 := _cl.cps[_ss[1].getNodeName()]
	cp1.mu.Lock()
	defer cp1.mu.Unlock()
	c, err := cp1.newConn()
	assert.NotNil(t, c)
	assert.Nil(t, err)
	c.close()
	c2, err := cp2.newConn()
	assert.Nil(t, c2)
	assert.NotNil(t, err)
}

func TestConnectionPool_PutConnLocked(t *testing.T) {
	_cl := New(ss, "")
	defer _cl.Close()
	cp := _cl.cps[ss[0].getNodeName()]
	cn, err := cp._conn(context.Background(), true)
	if err != nil {
		t.Fatalf("Failed _conn: %+v", err)
	}
	cp.mu.Lock()
	cp.putConnLocked(cn, err)
	cp.mu.Unlock()

	cn, err = cp._conn(context.Background(), true)
	if err != nil {
		t.Fatalf("Failed _conn: %+v", err)
	}
	cp.mu.Lock()
	req := make(chan connRequest, 1)
	reqKey := cp.nextRequest
	cp.nextRequest++
	cp.connRequests[reqKey] = req
	cp.putConnLocked(cn, err)
	cp.mu.Unlock()
}
