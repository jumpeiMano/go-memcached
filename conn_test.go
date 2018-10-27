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
