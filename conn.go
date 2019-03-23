package memcached

import (
	"bufio"
	"context"
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
	hashRing           *hashring.HashRing
	ncs                map[string]*nc
	createdAt          time.Time
	nextTryReconnectAt time.Time
	closed             bool
}

type nc struct {
	cp *ConnectionPool
	net.Conn
	count    int
	buffered bufio.ReadWriter
	isAlive  bool
	mu       sync.RWMutex
}

// Error
var (
	ErrMemcachedClosed = errors.New("memcached is closed")
	ErrBadConn         = errors.New("bad conn")
	ErrNotFound        = errors.New("not found")
)

func newConn(cp *ConnectionPool) (*conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cp.connectTimeout*2)
	defer cancel()

	ls := len(cp.servers)
	now := time.Now()
	c := &conn{
		cp:                 cp,
		hashRing:           hashring.New(cp.servers.getNodeNames()),
		ncs:                make(map[string]*nc, ls),
		createdAt:          now,
		nextTryReconnectAt: now,
	}
	var mu sync.Mutex
	var err error
	ec := make(chan error, len(cp.servers))
	for _, s := range cp.servers {
		go func(s Server) {
			node := s.getNodeName()
			_nc, err := c.newNC(&s)
			if err == nil {
				mu.Lock()
				c.ncs[node] = _nc
				mu.Unlock()
				ec <- nil
				return
			}
			if !c.cp.failover {
				ec <- err
				return
			}
			cp.logf("Failed connect to %s", node)
			c.removeNode(node)
			mu.Lock()
			c.ncs[node] = &nc{
				isAlive: false,
			}
			mu.Unlock()
			ec <- nil
		}(s)
	}
	for range cp.servers {
		select {
		case <-ctx.Done():
			return c, ErrCanceldByContext
		case err = <-ec:
			if err != nil {
				return c, err
			}
		}
	}

	var existsAlive bool
	for _, nc := range c.ncs {
		if nc.isAlive {
			existsAlive = true
			break
		}
	}
	if !existsAlive {
		return c, errors.New("There are any aliving connections")
	}
	return c, err
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
	c.closed = true
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
		if c.ncs[node].Conn == nil {
			continue
		}
		if err := c.ncs[node].SetDeadline(time.Now().Add(c.cp.pollTimeout)); err != nil {
			return errors.Wrap(err, "Failed SetDeadline")
		}
	}
	return nil
}

func (c *conn) removeNode(node string) {
	c.Lock()
	defer c.Unlock()
	c.hashRing = c.hashRing.RemoveNode(node)
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
		if err = tcpconn.SetKeepAlive(true); err != nil {
			return nil, errors.Wrap(err, "Failed SetKeepAlive")
		}
		if err = tcpconn.SetKeepAlivePeriod(c.cp.keepAlivePeriod); err != nil {
			return nil, errors.Wrap(err, "Failed SetKeepAlivePeriod")
		}
	}
	_nc.buffered = bufio.ReadWriter{
		Reader: bufio.NewReader(&_nc),
		Writer: bufio.NewWriter(&_nc),
	}
	_nc.isAlive = true
	return &_nc, nil
}

func (c *conn) tryReconnect() {
	if !c.cp.failover {
		return
	}
	now := time.Now()
	if now.Before(c.nextTryReconnectAt) {
		return
	}
	defer func() {
		c.Lock()
		defer c.Unlock()
		c.nextTryReconnectAt = now.Add(c.cp.tryReconnectPeriod)
	}()
	notAliveNodes := make([]string, 0, len(c.ncs))
	for node, nc := range c.ncs {
		if !nc.isAlive {
			notAliveNodes = append(notAliveNodes, node)
		}
	}
	if len(notAliveNodes) == 0 {
		return
	}
	for _, n := range notAliveNodes {
		_s := c.cp.servers.getByNode(n)
		if _s == nil {
			continue
		}
		c.cp.logf("Trying reconnect to %s", n)
		go func(s *Server, node string) {
			nc, err := c.newNC(s)
			if err != nil {
				return
			}
			if nc.isAlive {
				c.Lock()
				defer c.Unlock()
				if !c.closed {
					c.ncs[node] = nc
					c.hashRing = c.hashRing.AddNode(node)
				}
			}
		}(_s, n)
	}
}

func (nc *nc) writestrings(strs ...string) error {
	for _, s := range strs {
		if err := nc.writestring(s); err != nil {
			return errors.Wrap(err, "Failed writestring")
		}
	}
	return nil
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

	l, err := nc.buffered.ReadSlice('\n')
	if err != nil {
		return "", errors.Wrapf(ErrBadConn, "Failed bufferd.ReadSlice: %+v", err)
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

func handleError(s string) error {
	if s == "ERROR" {
		return ErrNonexistentCommand
	}
	if strings.HasPrefix(s, "CLIENT_ERROR") {
		return ErrClient
	}
	if strings.HasPrefix(s, "SERVER_ERROR") {
		return ErrServer
	}
	return nil
}
