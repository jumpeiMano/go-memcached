package memcached

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type conn struct {
	cl *Client
	mu sync.Mutex
	net.Conn
	buffered  bufio.ReadWriter
	isAlive   bool
	rownum    int
	createdAt time.Time
}

// Error
var (
	ErrMemcachedClosed = errors.New("memcached is closed")
	ErrBadConn         = errors.New("bad conn")
	ErrNotFound        = errors.New("not found")
)

func newConn(cl *Client, s *Server) (*conn, error) {
	network := "tcp"
	if strings.Contains(s.Host, "/") {
		network = "unix"
	}
	c := new(conn)
	c.cl = cl
	var err error
	c.Conn, err = net.DialTimeout(network, s.getAddr(), cl.connectTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "Failed DialTimeout")
	}
	if tcpconn, ok := c.Conn.(*net.TCPConn); ok {
		if err = tcpconn.SetKeepAlive(true); err != nil {
			return nil, errors.Wrap(err, "Failed SetKeepAlive")
		}
		if err = tcpconn.SetKeepAlivePeriod(c.cl.keepAlivePeriod); err != nil {
			return nil, errors.Wrap(err, "Failed SetKeepAlivePeriod")
		}
	}
	c.buffered = bufio.ReadWriter{
		Reader: bufio.NewReader(c),
		Writer: bufio.NewWriter(c),
	}
	c.isAlive = true
	return c, nil
}

func (c *conn) reset() error {
	c.rownum = 0
	return c.setDeadline()
}

func (c *conn) close() error {
	if !c.isAlive {
		return nil
	}
	if err := c.Conn.Close(); err != nil {
		return err
	}
	c.isAlive = false
	return nil
}

func (c *conn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return c.createdAt.Add(timeout).Before(time.Now())
}

func (c *conn) setDeadline() error {
	if c.Conn == nil {
		return nil
	}
	if err := c.SetDeadline(time.Now().Add(c.cl.pollTimeout)); err != nil {
		return errors.Wrap(err, "Failed SetDeadline")
	}
	return nil
}

// func (c *conn) tryReconnect() {
// 	if !c.cp.failover {
// 		return
// 	}
// 	now := time.Now()
// 	if now.Before(c.nextTryReconnectAt) {
// 		return
// 	}
// 	defer func() {
// 		c.Lock()
// 		defer c.Unlock()
// 		c.nextTryReconnectAt = now.Add(c.cp.tryReconnectPeriod)
// 	}()
// 	notAliveNodes := make([]string, 0, len(c.ncs))
// 	for node, nc := range c.ncs {
// 		if !c.isAlive {
// 			notAliveNodes = append(notAliveNodes, node)
// 		}
// 	}
// 	if len(notAliveNodes) == 0 {
// 		return
// 	}
// 	for _, n := range notAliveNodes {
// 		_s := c.cp.servers.getByNode(n)
// 		if _s == nil {
// 			continue
// 		}
// 		c.cp.logf("Trying reconnect to %s", n)
// 		go func(s *Server, node string) {
// 			nc, err := c.newNC(s)
// 			if err != nil {
// 				return
// 			}
// 			if c.isAlive {
// 				c.Lock()
// 				defer c.Unlock()
// 				if !c.closed {
// 					c.ncs[node] = nc
// 					c.hashRing = c.hashRing.AddNode(node)
// 					return
// 				}
// 				if err := c.Close(); err != nil {
// 					c.cp.logf("Failed c.Close: %v", err)
// 				}
// 			}
// 		}(_s, n)
// 	}
// }

func (c *conn) writestrings(strs ...string) error {
	for _, s := range strs {
		if err := c.writestring(s); err != nil {
			return errors.Wrap(err, "Failed writestring")
		}
	}
	return nil
}

func (c *conn) writestring(s string) error {
	if _, err := c.buffered.WriteString(s); err != nil {
		return errors.Wrap(err, "Failed buffered.WriteString")
	}
	return nil
}

func (c *conn) write(b []byte) error {
	if _, err := c.buffered.Write(b); err != nil {
		return errors.Wrap(err, "Failed buffered,Write")
	}
	return nil
}

func (c *conn) flush() error {
	if err := c.buffered.Flush(); err != nil {
		return errors.Wrapf(ErrBadConn, "Failed buffered.Flush: %+v", err)
	}
	return nil
}

func (c *conn) readline() (string, error) {
	if err := c.flush(); err != nil {
		return "", errors.Wrap(err, "Failed flush")
	}

	l, err := c.buffered.ReadSlice('\n')
	if err != nil {
		return "", errors.Wrapf(ErrBadConn, "Failed bufferd.ReadSlice: %+v", err)
	}
	if err = handleError(string(l)); err != nil {
		return "", err
	}
	return strings.Replace(string(l), "\r\n", "", -1), nil
}

func (c *conn) read(count int) ([]byte, error) {
	if err := c.flush(); err != nil {
		return []byte{}, errors.Wrap(err, "Failed flush")
	}
	b := make([]byte, count)
	if err := c.SetDeadline(time.Now().Add(c.cl.pollTimeout)); err != nil {
		return []byte{}, errors.Wrap(err, "Failed SetDeadLine")
	}
	if _, err := io.ReadFull(c.buffered, b); err != nil {
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
