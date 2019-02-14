package memcached

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const connRequestQueueSize = 1000000

const (
	defaultPort               = 11211
	defaultConnectTimeout     = 1 * time.Second
	defaultPollTimeout        = 1 * time.Second
	defaultTryReconnectPeriod = 60 * time.Second
	defaultKeepAlivePeriod    = 60 * time.Second
)

// ConnectionPool struct
type ConnectionPool struct {
	servers            Servers
	prefix             string
	connectTimeout     time.Duration
	pollTimeout        time.Duration
	cancelTimeout      time.Duration
	tryReconnectPeriod time.Duration
	keepAlivePeriod    time.Duration
	failover           bool
	mu                 sync.RWMutex
	freeConns          []*conn
	numOpen            int
	openerCh           chan struct{}
	connRequests       map[uint64]chan connRequest
	nextRequest        uint64
	maxLifetime        time.Duration // maximum amount of time a connection may be reused
	maxOpen            int           // maximum amount of connection num. maxOpen <= 0 means unlimited.
	cleanerCh          chan struct{}
	closed             bool
}

// Servers are slice of Server.
type Servers []Server

func (ss *Servers) getNodeNames() []string {
	nodes := make([]string, len(*ss))
	for i, s := range *ss {
		nodes[i] = s.getNodeName()
	}
	return nodes
}

func (ss *Servers) getByNode(node string) *Server {
	for _, s := range *ss {
		if s.getNodeName() == node {
			return &s
		}
	}
	return nil
}

// Server is the server's info of memcahced.
type Server struct {
	Host  string
	Port  int
	Alias string
}

func (s *Server) getAddr() string {
	port := s.Port
	if port == 0 {
		port = defaultPort
	}
	return fmt.Sprintf("%s:%d", s.Host, port)
}

func (s *Server) getNodeName() string {
	if s.Alias == "" {
		return s.getAddr()
	}
	return s.Alias
}

type connRequest struct {
	*conn
	err error
}

// New create ConnectionPool
func New(servers Servers, prefix string) (cp *ConnectionPool) {
	cp = new(ConnectionPool)
	cp.servers = servers
	cp.prefix = prefix
	cp.openerCh = make(chan struct{}, connRequestQueueSize)
	cp.connRequests = make(map[uint64]chan connRequest)
	cp.connectTimeout = defaultConnectTimeout
	cp.pollTimeout = defaultPollTimeout
	cp.cancelTimeout = defaultPollTimeout + (3 * time.Second)
	cp.tryReconnectPeriod = defaultTryReconnectPeriod
	cp.keepAlivePeriod = defaultKeepAlivePeriod

	go cp.opener()

	return
}

func finalizer(c *conn) {
	c.close()
}

func (cp *ConnectionPool) maybeOpenNewConnections() {
	if cp.closed {
		return
	}
	numRequests := len(cp.connRequests)
	if cp.maxOpen > 0 {
		numCanOpen := cp.maxOpen - cp.numOpen
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

func (cp *ConnectionPool) opener() {
	for range cp.openerCh {
		cp.openNewConnection()
	}
}

func (cp *ConnectionPool) openNewConnection() {
	if cp.closed {
		cp.mu.Lock()
		cp.numOpen--
		cp.mu.Unlock()
		return
	}
	c, err := newConn(cp)
	if err != nil {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		cp.numOpen--
		cp.maybeOpenNewConnections()
		return
	}
	cp.mu.Lock()
	if !cp.putConnLocked(c, nil) {
		cp.mu.Unlock()
		c.close()
		return
	}
	cp.mu.Unlock()
	return
}

func (cp *ConnectionPool) putConn(c *conn, err error) error {
	cp.mu.Lock()
	if needCloseConn(err) ||
		!cp.putConnLocked(c, nil) {
		cp.mu.Unlock()
		c.close()
		return err
	}
	cp.mu.Unlock()
	return err
}

func (cp *ConnectionPool) putConnLocked(c *conn, err error) bool {
	if cp.closed {
		return false
	}
	if cp.maxOpen > 0 && cp.maxOpen < cp.numOpen {
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

func (cp *ConnectionPool) conn(ctx context.Context) (*conn, error) {
	cn, err := cp._conn(ctx, true)
	if err == nil {
		return cn, nil
	}
	if errors.Cause(err) == ErrBadConn {
		return cp._conn(ctx, false)
	}
	return cn, err
}

func (cp *ConnectionPool) _conn(ctx context.Context, useFreeConn bool) (*conn, error) {
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
	lifetime := cp.maxLifetime

	var c *conn
	numFree := len(cp.freeConns)
	if useFreeConn && numFree > 0 {
		c = cp.freeConns[0]
		copy(cp.freeConns, cp.freeConns[1:])
		cp.freeConns = cp.freeConns[:numFree-1]
		cp.mu.Unlock()
		if c.expired(lifetime) {
			c.close()
			return nil, ErrBadConn
		}
		c.tryReconnect()
		err := c.setDeadline()
		return c, errors.Wrap(err, "Failed setDeadline")
	}

	if cp.maxOpen > 0 && cp.maxOpen <= cp.numOpen {
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
					cp.putConn(ret.conn, ret.err)
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
			ret.conn.tryReconnect()
			err := ret.conn.setDeadline()
			return ret.conn, errors.Wrap(err, "Failed setDeadline in response")
		}
	}

	cp.numOpen++
	cp.mu.Unlock()
	newCn, err := newConn(cp)
	if err != nil {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		cp.numOpen--
		cp.maybeOpenNewConnections()
		return nil, errors.Wrap(err, "Failed newConn")
	}
	err = newCn.setDeadline()
	return newCn, errors.Wrap(err, "Failed setDeadline of new conn")
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (cp *ConnectionPool) SetConnMaxLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	// wake cleaner up when lifetime is shortened.
	if d > 0 && d < cp.maxLifetime && cp.cleanerCh != nil {
		select {
		case cp.cleanerCh <- struct{}{}:
		default:
		}
	}
	cp.maxLifetime = d
	cp.startCleanerLocked()
}

// SetConnectTimeout sets the timeout of connect to memcached server.
func (cp *ConnectionPool) SetConnectTimeout(timeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.connectTimeout = timeout
}

// SetPollTimeout sets the timeout of polling from memcached server.
func (cp *ConnectionPool) SetPollTimeout(timeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.pollTimeout = timeout
	cp.cancelTimeout = timeout + (3 * time.Second)
}

// SetTryReconnectPeriod sets the period of trying reconnect.
func (cp *ConnectionPool) SetTryReconnectPeriod(period time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.tryReconnectPeriod = period
}

// SetKeepAlivePeriod sets the period of keep alive.
func (cp *ConnectionPool) SetKeepAlivePeriod(period time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.keepAlivePeriod = period
}

// SetConnMaxOpen sets the maximum amount of opening connections.
func (cp *ConnectionPool) SetConnMaxOpen(maxOpen int) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.maxOpen = maxOpen
}

// SetFailover is used to specify whether to use the failover option.
func (cp *ConnectionPool) SetFailover(failover bool) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.failover = failover
}

func (cp *ConnectionPool) needStartCleaner() bool {
	return cp.maxLifetime > 0 &&
		cp.numOpen > 0 &&
		cp.cleanerCh == nil
}

// startCleanerLocked starts connectionCleaner if needed.
func (cp *ConnectionPool) startCleanerLocked() {
	if cp.needStartCleaner() {
		cp.cleanerCh = make(chan struct{}, 1)
		go cp.connectionCleaner(cp.maxLifetime)
	}
}

func (cp *ConnectionPool) connectionCleaner(d time.Duration) {
	const minInterval = time.Second

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
		d = cp.maxLifetime
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

func (cp *ConnectionPool) removePrefix(key string) string {
	if len(cp.prefix) == 0 {
		return key
	}
	if strings.HasPrefix(key, "?") {
		return strings.Join([]string{"?", strings.Replace(key[1:], cp.prefix, "", 1)}, "")
	}
	return strings.Replace(key, cp.prefix, "", 1)
}

func (cp *ConnectionPool) addPrefix(key string) string {
	if len(cp.prefix) == 0 {
		return key
	}
	if strings.HasPrefix(key, "?") {
		return strings.Join([]string{"?", cp.prefix, key[1:]}, "")
	}
	return strings.Join([]string{cp.prefix, key}, "")
}

// Close will close the sockets to each memcached server
func (cp *ConnectionPool) Close() error {
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
		c.close()
	}
	cp.mu.Lock()
	cp.freeConns = nil
	cp.mu.Unlock()

	return err
}

func needCloseConn(err error) bool {
	if err == nil {
		return false
	}
	errcouse := errors.Cause(err)
	return errcouse == ErrBadConn ||
		errcouse == ErrServer ||
		errcouse == ErrCanceldByContext ||
		errcouse == context.DeadlineExceeded ||
		errcouse == context.Canceled
}
