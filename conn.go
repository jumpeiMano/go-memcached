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
	createdAt time.Time
}

// Error
var (
	ErrMemcachedClosed = errors.New("memcached is closed")
	ErrDeadNode        = errors.New("dead node")
	ErrConnect         = errors.New("connect error")
	ErrBadConn         = errors.New("bad conn")
	ErrNotFound        = errors.New("not found")
)

func (c *conn) reset() error {
	if rb := c.buffered.Reader.Buffered(); rb > 0 {
		if _, err := c.buffered.Discard(rb); err != nil {
			return errors.Wrap(err, "Failed Discard")
		}
	}
	if wb := c.buffered.Writer.Buffered(); wb > 0 {
		if err := c.flush(); err != nil {
			return errors.Wrap(err, "Failed flush")
		}
	}
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
