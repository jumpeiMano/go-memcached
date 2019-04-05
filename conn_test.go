package memcached

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConn(t *testing.T) {
	_ss := []Server{
		{Host: "127.0.0.1", Port: 11211},
		{Host: "127.0.0.1", Port: 99999},
	}
	_cl := New(_ss, "")
	defer _cl.Close()
	_cl.SetLogger(log.Printf)
	c, err := newConn(_cl, &_ss[0])
	assert.NotNil(t, c)
	assert.Nil(t, err)
	c.close()
	c2, err := newConn(_cl, &_ss[1])
	assert.Nil(t, c2)
	assert.NotNil(t, err)
}

// func TestTryReconnect(t *testing.T) {
// 	_ss := []Server{
// 		{Host: "127.0.0.1", Port: 11211, Alias: "1"},
// 		{Host: "127.0.0.1", Port: 11212, Alias: "2"},
// 	}
// 	_cp := New(_ss, "")
// 	defer _cp.Close()
// 	c, err := newConn(_cp)
// 	if err != nil {
// 		t.Fatalf("Failed newConn:%v", err)
// 	}
// 	_cp.SetFailover(true)
// 	_cp.SetLogger(log.Printf)
// 	c.removeNode("2")
// 	c.ncs["2"] = &nc{
// 		isAlive: false,
// 	}
// 	c.tryReconnect()
// 	time.Sleep(10 * time.Millisecond)
// 	c.RLock()
// 	defer c.RUnlock()
// 	assert.Equal(t, true, c.ncs["2"].isAlive)
// }
