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
	count            int
	buffered         bufio.ReadWriter
	isAlive          bool
	nextAliveCheckAt time.Time
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
	ls := len(cp.servers)
	c := &conn{
		cp:        cp,
		hashRing:  hashring.New(cp.servers.getNodeNames()),
		ncs:       make(map[string]*nc, ls),
		createdAt: time.Now(),
	}
	for _, s := range cp.servers {
		_nc, err := c.newNC(&s)
		if err == nil {
			c.ncs[s.Alias] = _nc
			continue
		}
		if !c.cp.failover {
			return nil, err
		}
		c.hashRing = c.hashRing.RemoveNode(s.getNodeName())
		c.ncs[s.Alias] = &nc{
			isAlive:          false,
			nextAliveCheckAt: time.Now().Add(cp.aliveCheckPeriod),
		}
	}
	return c, nil
}

func (c *conn) checkAliveAndReconnect() {
	now := time.Now()
	for _, s := range c.cp.servers {
		node := s.getNodeName()
		if c.ncs[node] != nil && now.Before(c.ncs[node].nextAliveCheckAt) {
			continue
		}
		if c.ncs[node] == nil ||
			!c.ncs[node].isAlive ||
			!c.ncs[node].checkAlive() {
			_nc, err := c.newNC(&s)
			if err == nil {
				c.ncs[node] = _nc
				c.hashRing = c.hashRing.AddNode(node)
			} else {
				c.ncs[node] = &nc{
					isAlive:          false,
					nextAliveCheckAt: now.Add(c.cp.aliveCheckPeriod),
				}
				c.hashRing = c.hashRing.RemoveNode(node)
			}
		}
	}
}

func (c *conn) reset() {
	for node := range c.ncs {
		c.ncs[node].count = 0
	}
}

func (c *conn) close() error {
	for node := range c.ncs {
		if !c.ncs[node].isAlive {
			continue
		}
		if err := c.ncs[node].Conn.Close(); err != nil {
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
	for node := range c.ncs {
		if !c.ncs[node].isAlive {
			continue
		}
		if err := c.ncs[node].Conn.SetDeadline(time.Now().Add(c.cp.pollTimeout)); err != nil {
			return errors.Wrap(err, "Failed SetDeadLine")
		}
	}
	return nil
}

func (c *conn) newNC(s *Server) (*nc, error) {
	network := "tcp"
	if strings.Contains(s.Host, "/") {
		network = "unix"
	}
	var _nc nc
	var err error
	_nc.Conn, err = net.DialTimeout(network, s.getAddr(), c.cp.connectTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "Failed DialTimeout")
	}
	if tcpconn, ok := _nc.Conn.(*net.TCPConn); ok {
		tcpconn.SetKeepAlive(true)
		tcpconn.SetKeepAlivePeriod(1 * time.Minute)
	}
	_nc.buffered = bufio.ReadWriter{
		Reader: bufio.NewReader(&_nc),
		Writer: bufio.NewWriter(&_nc),
	}
	_nc.isAlive = true
	_nc.nextAliveCheckAt = time.Now().Add(c.cp.aliveCheckPeriod)
	return &_nc, nil
}

func (nc *nc) checkAlive() bool {
	nc.writestring("get ping\r\n")
	_, err := nc.readline()
	return err == nil
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
