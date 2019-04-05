package memcached

import (
	"context"
	"testing"
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
