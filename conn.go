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
	ncs       map[string]*nc
	createdAt time.Time
	closed    bool
}

type nc struct {
	net.Conn
	count    int
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

func (c *conn) reset() {
	for node := range c.ncs {
		c.ncs[node].count = 0
	}
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

func (nc *nc) writestrings(strs ...string) {
	for _, s := range strs {
		nc.writestring(s)
	}
}

func (nc *nc) writestring(s string) {
	if _, err := nc.buffered.WriteString(s); err != nil {
		panic(NewError("%s", err))
	}
}

func (nc *nc) write(b []byte) {
	if _, err := nc.buffered.Write(b); err != nil {
		panic(NewError("%s", err))
	}
}

func (nc *nc) flush() {
	if err := nc.buffered.Flush(); err != nil {
		panic(NewError("%s", err))
	}
}

func (nc *nc) readline() string {
	nc.flush()
	l, isPrefix, err := nc.buffered.ReadLine()
	if isPrefix || err != nil {
		panic(NewError("Prefix: %v, %s", isPrefix, err))
	}
	return string(l)
}

func (nc *nc) read(count int) []byte {
	nc.flush()
	b := make([]byte, count)
	if _, err := io.ReadFull(nc.buffered, b); err != nil {
		panic(NewError("%s", err))
	}
	return b
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
