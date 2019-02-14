package memcached

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConn(t *testing.T) {
	_ss := []Server{
		{Host: "127.0.0.1", Port: 11211},
		{Host: "127.0.0.1", Port: 99999},
	}
	_cp := New(_ss, "")
	defer _cp.Close()
	_, err := newConn(_cp)
	assert.NotNil(t, err)

	_cp.SetFailover(true)
	c2, err := newConn(_cp)
	assert.Nil(t, err)
	c2.close()
}

func TestTryReconnect(t *testing.T) {
	_ss := []Server{
		{Host: "127.0.0.1", Port: 11211, Alias: "1"},
		{Host: "127.0.0.1", Port: 11212, Alias: "2"},
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
	assert.Equal(t, true, c.ncs["2"].isAlive)
}
