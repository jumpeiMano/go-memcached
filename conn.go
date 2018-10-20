package memcached

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/serialx/hashring"
)

type conn struct {
	cp *ConnectionPool
	sync.RWMutex
	hashRing  *hashring.HashRing
	ncs       map[string]*nc
	createdAt time.Time
	closed    bool
}

type nc struct {
	net.Conn
	cp       *ConnectionPool
	count    int
	buffered bufio.ReadWriter
}

// Item gives the cached data.
type Item struct {
	Key   string
	Value []byte
	Flags uint16
	Cas   uint64
	Exp   int
}

// DefaultPort memcached port
const DefaultPort = 11211

// Error
var (
	ErrMemcachedClosed = errors.New("memcached is closed")
	ErrBadConn         = errors.New("bad conn")
)

func newConn(cp *ConnectionPool) (*conn, error) {
	var network string
	ls := len(cp.servers)
	c := conn{
		cp:        cp,
		hashRing:  hashring.New(cp.servers.getAliases()),
		ncs:       make(map[string]*nc, ls),
		createdAt: time.Now(),
	}
	for _, s := range cp.servers {
		if strings.Contains(s.Host, "/") {
			network = "unix"
		} else {
			network = "tcp"
		}
		_nc := nc{cp: cp}
		var err error
		_nc.Conn, err = net.DialTimeout(network, s.getAddr(), cp.connectTimeout)
		if err != nil {
			if !cp.failover {
				return nil, err
			}
			c.hashRing = c.hashRing.RemoveNode(s.getAlias())
			continue
		}
		if tcpconn, ok := _nc.Conn.(*net.TCPConn); ok {
			tcpconn.SetKeepAlive(true)
		}
		_nc.buffered = bufio.ReadWriter{
			Reader: bufio.NewReader(&_nc),
			Writer: bufio.NewWriter(&_nc),
		}
		c.ncs[s.Alias] = &_nc
	}
	return &c, nil
}

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

func (c *conn) setDeadline() error {
	for i := range c.ncs {
		if err := c.ncs[i].Conn.SetDeadline(time.Now().Add(c.cp.pollTimeout)); err != nil {
			return errors.Wrap(err, "Failed SetDeadLine")
		}
	}
	return nil
}

func (nc *nc) writestrings(strs ...string) {
	for _, s := range strs {
		nc.writestring(s)
	}
}

func (nc *nc) writestring(s string) error {
	if _, err := nc.buffered.WriteString(s); err != nil {
		return errors.Wrap(err, "Failed buffered.WriteString")
	}
	return nil
}

func (nc *nc) write(b []byte) error {
	if _, err := nc.buffered.Write(b); err != nil {
		return errors.Wrap(err, "Failed buffered,Write")
	}
	return nil
}

func (nc *nc) flush() error {
	if err := nc.buffered.Flush(); err != nil {
		return errors.Wrapf(ErrBadConn, "Failed buffered.Flush: %+v", err)
	}
	return nil
}

func (nc *nc) readline() (string, error) {
	if err := nc.flush(); err != nil {
		return "", errors.Wrap(err, "Failed flush")
	}

	l, isPrefix, err := nc.buffered.ReadLine()
	if isPrefix || err != nil {
		return "", errors.Wrapf(ErrBadConn, "Failed bufferd.ReadLine: %+v", err)
	}
	return string(l), nil
}

func (nc *nc) read(count int) ([]byte, error) {
	if err := nc.flush(); err != nil {
		return []byte{}, errors.Wrap(err, "Failed flush")
	}
	b := make([]byte, count)
	if _, err := io.ReadFull(nc.buffered, b); err != nil {
		return b, errors.Wrapf(ErrBadConn, "Failed ReadFull: %+v", err)
	}
	return b, nil
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
