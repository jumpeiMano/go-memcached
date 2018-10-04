package memcached

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type conn struct {
	cp *ConnectionPool
	sync.Mutex
	ncs       []*nc
	createdAt time.Time
	closed    bool
}

type nc struct {
	net.Conn
	buffered bufio.ReadWriter
}

// Item gives the cached data.
type Item struct {
	Key   string
	Value []byte
	Flags uint16
	Cas   uint64
}

// DefaultPort memcached port
const DefaultPort = 11211

// Error
var (
	ErrMemcachedClosed = errors.New("memcached is closed")
	ErrBadConn         = errors.New("bad conn")
)

func (c *conn) writestrings(strs ...string) {
	for _, s := range strs {
		c.writestring(s)
	}
}

func (c *conn) writestring(s string) {
	if _, err := c.ncs[0].buffered.WriteString(s); err != nil {
		panic(NewError("%s", err))
	}
}

func (c *conn) write(b []byte) {
	if _, err := c.ncs[0].buffered.Write(b); err != nil {
		panic(NewError("%s", err))
	}
}

func (c *conn) flush() {
	if err := c.ncs[0].buffered.Flush(); err != nil {
		panic(NewError("%s", err))
	}
}

func (c *conn) readline() string {
	c.flush()
	l, isPrefix, err := c.ncs[0].buffered.ReadLine()
	if isPrefix || err != nil {
		panic(NewError("Prefix: %v, %s", isPrefix, err))
	}
	return string(l)
}

func (c *conn) read(count int) []byte {
	c.flush()
	b := make([]byte, count)
	if _, err := io.ReadFull(c.ncs[0].buffered, b); err != nil {
		panic(NewError("%s", err))
	}
	return b
}

func (c *conn) close() error {
	for i := range c.ncs {
		if err := c.ncs[i].Conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return c.createdAt.Add(timeout).Before(time.Now())
}

func (c *conn) setDeadline() {
	for i := range c.ncs {
		if err := c.ncs[i].Conn.SetDeadline(time.Now().Add(c.cp.connectTimeout)); err != nil {
			panic(NewError("%s", err))
		}
	}
}

func (c *conn) getCreatedAt() time.Time {
	return c.createdAt
}

func (c *conn) getNCS() []*nc {
	return c.ncs
}

// Error is the error from CacheService.
type Error struct {
	Message string
}

// NewError creates a new Error.
func NewError(format string, args ...interface{}) Error {
	return Error{fmt.Sprintf(format, args...)}
}

func (merr Error) Error() string {
	return merr.Message
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(Error)
	}
}
