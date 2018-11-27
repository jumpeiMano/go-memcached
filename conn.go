package memcached

import (
	"bufio"
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
	cp *ConnectionPool
	net.Conn
	count    int
	buffered bufio.ReadWriter
	isAlive  bool
}

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
			c.ncs[s.getNodeName()] = _nc
			continue
		}
		if !c.cp.failover {
			return nil, err
		}
		c.hashRing = c.hashRing.RemoveNode(s.getNodeName())
		c.ncs[s.getNodeName()] = &nc{
			isAlive: false,
		}
	}
	return c, nil
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

func (c *conn) newNC(s *Server) (*nc, error) {
	network := "tcp"
	if strings.Contains(s.Host, "/") {
		network = "unix"
	}
	_nc := nc{cp: c.cp}
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
	return &_nc, nil
}

func (nc *nc) writestrings(strs ...string) {
	for _, s := range strs {
		nc.writestring(s)
	}
}

func (nc *nc) writestring(s string) error {
	if err := nc.SetDeadline(time.Now().Add(nc.cp.pollTimeout)); err != nil {
		return errors.Wrap(err, "Failed SetDeadLine")
	}
	if _, err := nc.buffered.WriteString(s); err != nil {
		return errors.Wrap(err, "Failed buffered.WriteString")
	}
	return nil
}

func (nc *nc) write(b []byte) error {
	if err := nc.SetDeadline(time.Now().Add(nc.cp.pollTimeout)); err != nil {
		return errors.Wrap(err, "Failed SetDeadLine")
	}
	if _, err := nc.buffered.Write(b); err != nil {
		return errors.Wrap(err, "Failed buffered,Write")
	}
	return nil
}

func (nc *nc) flush() error {
	if err := nc.SetDeadline(time.Now().Add(nc.cp.pollTimeout)); err != nil {
		return errors.Wrap(err, "Failed SetDeadLine")
	}
	if err := nc.buffered.Flush(); err != nil {
		return errors.Wrapf(ErrBadConn, "Failed buffered.Flush: %+v", err)
	}
	return nil
}

func (nc *nc) readline() (string, error) {
	if err := nc.flush(); err != nil {
		return "", errors.Wrap(err, "Failed flush")
	}

	if err := nc.SetDeadline(time.Now().Add(nc.cp.pollTimeout)); err != nil {
		return "", errors.Wrap(err, "Failed SetDeadLine")
	}
	l, err := nc.buffered.ReadSlice('\n')
	if err != nil {
		return "", errors.Wrapf(ErrBadConn, "Failed bufferd.ReadLine: %+v", err)
	}
	if err = handleError(string(l)); err != nil {
		return "", err
	}
	return strings.Replace(string(l), "\r\n", "", -1), nil
}

func (nc *nc) read(count int) ([]byte, error) {
	if err := nc.flush(); err != nil {
		return []byte{}, errors.Wrap(err, "Failed flush")
	}
	b := make([]byte, count)
	if err := nc.SetDeadline(time.Now().Add(nc.cp.pollTimeout)); err != nil {
		return []byte{}, errors.Wrap(err, "Failed SetDeadLine")
	}
	if _, err := io.ReadFull(nc.buffered, b); err != nil {
		return b, errors.Wrapf(ErrBadConn, "Failed ReadFull: %+v", err)
	}
	if err := handleError(string(b)); err != nil {
		return []byte{}, err
	}
	return b, nil
}
