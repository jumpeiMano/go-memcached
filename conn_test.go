package memcached

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewConn(t *testing.T) {
	_ss := []Server{
		{Host: "memcached_1", Port: 11211},
		{Host: "memcached_1", Port: 99999},
	}
	_cp := New(_ss, "")
	defer _cp.Close()
	_cp.SetLogger(log.Printf)
	_, err := newConn(_cp)
	assert.NotNil(t, err)

	_cp.SetFailover(true)
	c2, err := newConn(_cp)
	assert.Nil(t, err)
	c2.close()
}

func TestTryReconnect(t *testing.T) {
	_ss := []Server{
		{Host: "memcached_1", Port: 11211, Alias: "1"},
		{Host: "memcached_2", Port: 11211, Alias: "2"},
	}
	_cp := New(_ss, "")
	defer _cp.Close()
	c, err := newConn(_cp)
	if err != nil {
		t.Fatalf("Failed newConn:%v", err)
	}
	_cp.SetFailover(true)
	_cp.SetLogger(log.Printf)
	c.removeNode("2")
	c.ncs["2"] = &nc{
		isAlive: false,
	}
	c.tryReconnect()
	time.Sleep(10 * time.Millisecond)
	c.RLock()
	defer c.RUnlock()
	assert.Equal(t, true, c.ncs["2"].isAlive)
}
