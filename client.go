package memcached

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/serialx/hashring"
)

const connRequestQueueSize = 1000000

const (
	defaultPort               = 11211
	defaultConnectTimeout     = 1 * time.Second
	defaultPollTimeout        = 1 * time.Second
	defaultTryReconnectPeriod = 60 * time.Second
	defaultKeepAlivePeriod    = 60 * time.Second
	defaultMaxErrorCount      = 100
)

// Client is the client of go-memcached.
type Client struct {
	servers            Servers
	hashRing           *hashring.HashRing
	prefix             string
	connectTimeout     time.Duration
	pollTimeout        time.Duration
	tryReconnectPeriod time.Duration
	keepAlivePeriod    time.Duration
	failover           bool
	maxOpen            int           // maximum amount of connection num. maxOpen <= 0 means unlimited.
	maxLifetime        time.Duration // maximum amount of time a connection may be reused
	mu                 sync.RWMutex
	cps                map[string]*connectionPool
	maxErrorCount      int64
	logf               func(format string, params ...interface{})
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

// New create Client
func New(servers Servers, prefix string) (cl *Client) {
	cl = new(Client)
	cl.servers = servers
	cl.hashRing = hashring.New(cl.servers.getNodeNames())
	cl.prefix = prefix
	cl.connectTimeout = defaultConnectTimeout
	cl.pollTimeout = defaultPollTimeout
	cl.tryReconnectPeriod = defaultTryReconnectPeriod
	cl.keepAlivePeriod = defaultKeepAlivePeriod
	cl.maxErrorCount = defaultMaxErrorCount
	cl.logf = log.Printf

	cl.cps = make(map[string]*connectionPool, len(servers))
	for i := range servers {
		cl.cps[servers[i].getNodeName()] = cl.openConnectionPool(&servers[i])
	}

	return
}

func (cl *Client) openConnectionPool(server *Server) *connectionPool {
	cp := new(connectionPool)
	cp.cl = cl
	cp.Server = server
	cp.openerCh = make(chan struct{}, connRequestQueueSize)
	cp.connRequests = make(map[uint64]chan connRequest)
	go cp.opener()
	return cp
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are reused forever.
func (cl *Client) SetConnMaxLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.maxLifetime = d
	// wake cleaner up when lifetime is shortened.
	for node := range cl.cps {
		cp := cl.cps[node]
		if d > 0 && d < cl.maxLifetime && cp.cleanerCh != nil {
			select {
			case cp.cleanerCh <- struct{}{}:
			default:
			}
		}
		cp.startCleanerLocked()
	}
}

// SetConnectTimeout sets the timeout of connect to memcached server.
func (cl *Client) SetConnectTimeout(timeout time.Duration) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.connectTimeout = timeout
}

// SetPollTimeout sets the timeout of polling from memcached server.
func (cl *Client) SetPollTimeout(timeout time.Duration) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.pollTimeout = timeout
}

// SetTryReconnectPeriod sets the period of trying reconnect.
func (cl *Client) SetTryReconnectPeriod(period time.Duration) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.tryReconnectPeriod = period
}

// SetMaxErrorCount sets the max of error count to close the connection pool.
func (cl *Client) SetMaxErrorCount(count int64) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.maxErrorCount = count
}

// SetKeepAlivePeriod sets the period of keep alive.
func (cl *Client) SetKeepAlivePeriod(period time.Duration) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.keepAlivePeriod = period
}

// SetConnMaxOpen sets the maximum amount of opening connections.
func (cl *Client) SetConnMaxOpen(maxOpen int) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.maxOpen = maxOpen
}

// SetFailover is used to specify whether to use the failover option.
func (cl *Client) SetFailover(failover bool) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.failover = failover
}

// SetLogger is used to set logger
func (cl *Client) SetLogger(logf func(format string, params ...interface{})) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.logf = logf
}

func (cl *Client) removePrefix(key string) string {
	if len(cl.prefix) == 0 {
		return key
	}
	if strings.HasPrefix(key, "?") {
		return strings.Join([]string{"?", strings.Replace(key[1:], cl.prefix, "", 1)}, "")
	}
	return strings.Replace(key, cl.prefix, "", 1)
}

func (cl *Client) addPrefix(key string) string {
	if len(cl.prefix) == 0 {
		return key
	}
	if strings.HasPrefix(key, "?") {
		return strings.Join([]string{"?", cl.prefix, key[1:]}, "")
	}
	return strings.Join([]string{cl.prefix, key}, "")
}

func (cl *Client) conn(keys ...string) (map[string]*conn, error) {
	nodes := make([]string, 0, len(cl.cps))
	if len(keys) == 0 {
		for node := range cl.cps {
			nodes = append(nodes, node)
		}
		m, err := cl._conn(context.Background(), nodes)
		return m, errors.Wrap(err, "Failed _conn")
	}
	nodeMap := map[string]struct{}{}
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		rawkey := cl.addPrefix(key)
		node, ok := cl.hashRing.GetNode(rawkey)
		if !ok {
			return map[string]*conn{}, errors.New("Failed GetNode")
		}
		nodeMap[node] = struct{}{}
	}
	for node := range nodeMap {
		nodes = append(nodes, node)
	}

	m, err := cl._conn(context.Background(), nodes)
	return m, errors.Wrap(err, "Failed _conn")
}

func (cl *Client) _conn(ctx context.Context, nodes []string) (map[string]*conn, error) {
	var (
		wg sync.WaitGroup
		sm sync.Map
	)
	nl := len(nodes)
	m := make(map[string]*conn, nl)
	ec := make(chan error, nl)
	for _, node := range nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			cp := cl.cps[node]
			cp.mu.RLock()
			closed := cp.closed
			cp.mu.RUnlock()
			if closed {
				return
			}
			c, err := cp.conn(ctx)
			if err != nil {
				ec <- errors.Wrap(err, "Failed conn")

				if cp.circuitBreaker(err) {
					cl.removeNode(node)
					cp.close()
					go cl.tryReconnect()
				}
			}
			sm.Store(node, c)
		}(node)
	}
	wg.Wait()
	close(ec)
	for err := range ec {
		if err != nil {
			return m, err
		}
	}
	sm.Range(func(key interface{}, value interface{}) bool {
		k, ok := key.(string)
		if !ok {
			cl.logf("Unexpected key type: %T", key)
			return false
		}
		c, ok := value.(*conn)
		if !ok {
			cl.logf("Unexpected value type: %T", value)
			return false
		}
		m[k] = c
		return true
	})
	return m, nil
}

func (cl *Client) putConn(m map[string]*conn, err error) {
	for node, c := range m {
		cp := cl.cps[node]
		if err1 := cp.putConn(c, err); err1 != nil {
			cl.logf("Failed putConn: %v", err)
		}
	}
}

func (cl *Client) addNode(node string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.hashRing = cl.hashRing.AddNode(node)
}

func (cl *Client) removeNode(node string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.hashRing = cl.hashRing.RemoveNode(node)
}

// Close closes all connectionPools and channels
func (cl *Client) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for node := range cl.cps {
		if err := cl.cps[node].close(); err != nil {
			cl.logf("Failed pool.close: %v", err)
		}
	}
	return nil
}

func (cl *Client) tryReconnect() {
	for {
		time.Sleep(cl.tryReconnectPeriod)
		closedPools := make(map[string]*connectionPool, len(cl.cps))
		for node, cp := range cl.cps {
			if cp.closed {
				closedPools[node] = cp
			}
		}
		if len(closedPools) == 0 {
			return
		}

		var existsDeadConn bool
		for node, cp := range closedPools {
			if c, err := cp.newConn(); err == nil {
				c.close()
				cl.addNode(node)
				cp.mu.Lock()
				cp.closed = false
				cp.openerCh = make(chan struct{}, connRequestQueueSize)
				cp.mu.Unlock()
				go cp.opener()
				continue
			}
			existsDeadConn = true
		}
		if !existsDeadConn {
			return
		}
	}
}
