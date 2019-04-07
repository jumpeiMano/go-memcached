package memcached

import (
	"bufio"
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type connectionPool struct {
	*Server
	cl                 *Client
	mu                 sync.RWMutex
	freeConns          []*conn
	numOpen            int
	openerCh           chan struct{}
	connRequests       map[uint64]chan connRequest
	nextRequest        uint64
	cleanerCh          chan struct{}
	connectErrorCount  int64
	connectErrorPeriod time.Time
	closed             bool
}

type connRequest struct {
	*conn
	err error
}

func (cp *connectionPool) maybeOpenNewConnections() {
	if cp.closed {
		return
	}
	numRequests := len(cp.connRequests)
	if cp.cl.maxOpen > 0 {
		numCanOpen := cp.cl.maxOpen - cp.numOpen
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		cp.numOpen++
		numRequests--
		cp.openerCh <- struct{}{}
	}
}

func (cp *connectionPool) opener() {
	for range cp.openerCh {
		cp.openNewConnection()
	}
}

func (cp *connectionPool) openNewConnection() {
	var closed bool
	cp.mu.RLock()
	closed = cp.closed
	cp.mu.RUnlock()
	if closed {
		cp.mu.Lock()
		cp.numOpen--
		cp.mu.Unlock()
		return
	}
	c, err := cp.newConn()
	if err != nil {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		cp.numOpen--
		cp.maybeOpenNewConnections()
		return
	}
	if err = cp.putConn(c, nil); err != nil {
		cp.cl.logf("Failed putConn: %v", err)
	}
}

func (cp *connectionPool) newConn() (*conn, error) {
	network := "tcp"
	if strings.Contains(cp.Server.Host, "/") {
		network = "unix"
	}
	c := new(conn)
	c.cl = cp.cl
	var err error
	c.Conn, err = net.DialTimeout(network, cp.getAddr(), cp.cl.connectTimeout)
	if err != nil {
		return nil, errors.Wrapf(ErrConnect, "Failed DialTimeout: %v", err)
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

func (cp *connectionPool) putConn(c *conn, err error) error {
	cp.mu.Lock()
	if needCloseConn(err) ||
		!cp.putConnLocked(c, nil) {
		cp.numOpen--
		cp.mu.Unlock()
		c.close()
		return err
	}
	cp.mu.Unlock()
	return err
}

func (cp *connectionPool) circuitBreaker(err error) bool {
	if !cp.cl.failover {
		return false
	}
	if errors.Cause(err) != ErrConnect {
		return false
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	now := time.Now()
	if cp.connectErrorPeriod.After(now) {
		cp.connectErrorPeriod = now.Add(cp.cl.tryReconnectPeriod)
		cp.connectErrorCount = 0
	}
	cp.connectErrorCount++
	return cp.connectErrorCount >= cp.cl.maxErrorCount
}

func (cp *connectionPool) putConnLocked(c *conn, err error) bool {
	if cp.closed {
		return false
	}
	if cp.cl.maxOpen > 0 && cp.cl.maxOpen < cp.numOpen {
		return false
	}
	if len(cp.connRequests) > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range cp.connRequests {
			break
		}
		delete(cp.connRequests, reqKey)
		req <- connRequest{
			conn: c,
			err:  err,
		}
	} else {
		cp.freeConns = append(cp.freeConns, c)
		cp.startCleanerLocked()
	}
	return true
}

func (cp *connectionPool) conn(ctx context.Context) (*conn, error) {
	cn, err := cp._conn(ctx, true)
	if err == nil {
		return cn, nil
	}
	if errors.Cause(err) == ErrBadConn {
		return cp._conn(ctx, false)
	}
	return cn, err
}

func (cp *connectionPool) _conn(ctx context.Context, useFreeConn bool) (*conn, error) {
	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		return nil, ErrMemcachedClosed
	}
	// Check if the context is expired.
	select {
	default:
	case <-ctx.Done():
		cp.mu.Unlock()
		return nil, errors.Wrap(ctx.Err(), "the context is expired")
	}
	lifetime := cp.cl.maxLifetime

	var c *conn
	numFree := len(cp.freeConns)
	if useFreeConn && numFree > 0 {
		c = cp.freeConns[0]
		copy(cp.freeConns, cp.freeConns[1:])
		cp.freeConns = cp.freeConns[:numFree-1]
		cp.mu.Unlock()
		if c.expired(lifetime) {
			cp.mu.Lock()
			cp.numOpen--
			cp.mu.Unlock()
			c.close()
			return nil, ErrBadConn
		}
		// c.tryReconnect()
		err := c.reset()
		return c, errors.Wrap(err, "Failed reset")
	}

	if cp.cl.maxOpen > 0 && cp.cl.maxOpen <= cp.numOpen {
		req := make(chan connRequest, 1)
		reqKey := cp.nextRequest
		cp.nextRequest++
		cp.connRequests[reqKey] = req
		cp.mu.Unlock()

		select {
		// timeout
		case <-ctx.Done():
			// Remove the connection request and ensure no value has been sent
			// on it after removing.
			cp.mu.Lock()
			delete(cp.connRequests, reqKey)
			cp.mu.Unlock()
			select {
			case ret, ok := <-req:
				if ok {
					if err := cp.putConn(ret.conn, ret.err); err != nil {
						return c, errors.Wrap(err, "Failed putConn")
					}
				}
			default:
			}
			return nil, errors.Wrap(ctx.Err(), "Deadline of connRequests exceeded")
		case ret, ok := <-req:
			if !ok {
				return nil, ErrMemcachedClosed
			}
			if ret.err != nil {
				return ret.conn, errors.Wrap(ret.err, "Response has an error")
			}
			// ret.conn.tryReconnect()
			err := ret.conn.reset()
			return ret.conn, errors.Wrap(err, "Failed reset in response")
		}
	}

	cp.numOpen++
	cp.mu.Unlock()
	newCn, err := cp.newConn()
	if err != nil {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		cp.numOpen--
		cp.maybeOpenNewConnections()
		return nil, errors.Wrap(err, "Failed newConn")
	}
	err = newCn.reset()
	return newCn, errors.Wrap(err, "Failed reset of new conn")
}

func (cp *connectionPool) needStartCleaner() bool {
	return cp.cl.maxLifetime > 0 &&
		cp.numOpen > 0 &&
		cp.cleanerCh == nil
}

// startCleanerLocked starts connectionCleaner if needed.
func (cp *connectionPool) startCleanerLocked() {
	if cp.needStartCleaner() {
		cp.cleanerCh = make(chan struct{}, 1)
		go cp.connectionCleaner()
	}
}

func (cp *connectionPool) connectionCleaner() {
	const minInterval = time.Second

	d := cp.cl.maxLifetime
	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-cp.cleanerCh: // maxLifetime was changed or memcached was closed.
		}

		cp.mu.Lock()
		d = cp.cl.maxLifetime
		if cp.closed || cp.numOpen == 0 || d <= 0 {
			cp.cleanerCh = nil
			cp.mu.Unlock()
			return
		}

		expiredSince := time.Now().Add(-d)
		var closing []*conn
		for i := 0; i < len(cp.freeConns); i++ {
			c := cp.freeConns[i]
			if c.createdAt.Before(expiredSince) {
				closing = append(closing, c)
				last := len(cp.freeConns) - 1
				cp.freeConns[i] = cp.freeConns[last]
				cp.freeConns[last] = nil
				cp.freeConns = cp.freeConns[:last]
				cp.numOpen--
				i--
			}
		}
		cp.mu.Unlock()

		for _, c := range closing {
			if err := c.close(); err != nil {
				log.Println("Failed conn.close", err)
			}
		}

		if d < minInterval {
			d = minInterval
		}
		t.Reset(d)
	}
}

func (cp *connectionPool) close() error {
	cp.mu.Lock()
	if cp.closed {
		cp.mu.Unlock()
		return nil
	}

	close(cp.openerCh)
	if cp.cleanerCh != nil {
		close(cp.cleanerCh)
	}
	for _, cr := range cp.connRequests {
		close(cr)
	}
	cp.closed = true
	cp.mu.Unlock()
	var err error
	for _, c := range cp.freeConns {
		if err1 := c.close(); err1 != nil {
			err = err1
		}
	}
	cp.mu.Lock()
	cp.freeConns = nil
	cp.numOpen = 0
	cp.mu.Unlock()

	return err
}

func needCloseConn(err error) bool {
	if err == nil {
		return false
	}
	errcouse := errors.Cause(err)
	return errcouse == ErrBadConn ||
		errcouse == ErrServer
}
