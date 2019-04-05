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
)

// Client is the client of go-memcached.
type Client struct {
	servers  Servers
	hashRing *hashring.HashRing
	// nextTryReconnectAt time.Time
	prefix             string
	connectTimeout     time.Duration
	pollTimeout        time.Duration
	cancelTimeout      time.Duration
	tryReconnectPeriod time.Duration
	keepAlivePeriod    time.Duration
	failover           bool
	maxOpen            int           // maximum amount of connection num. maxOpen <= 0 means unlimited.
	maxLifetime        time.Duration // maximum amount of time a connection may be reused
	mu                 sync.RWMutex
	cps                map[string]*connectionPool
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

// func (ss *Servers) getByNode(node string) *Server {
// 	for _, s := range *ss {
// 		if s.getNodeName() == node {
// 			return &s
// 		}
// 	}
// 	return nil
// }

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
	cl.cancelTimeout = defaultPollTimeout + (3 * time.Second)
	cl.tryReconnectPeriod = defaultTryReconnectPeriod
	cl.keepAlivePeriod = defaultKeepAlivePeriod
	cl.logf = log.Printf

	cl.cps = make(map[string]*connectionPool, len(servers))
	for _, s := range servers {
		cp := new(connectionPool)
		cp.cl = cl
		cp.Server = &s
		cp.openerCh = make(chan struct{}, connRequestQueueSize)
		cp.connRequests = make(map[uint64]chan connRequest)
		go cp.opener()
		cl.cps[s.getNodeName()] = cp
	}

	return
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
	cl.cancelTimeout = timeout + (3 * time.Second)
}

// SetTryReconnectPeriod sets the period of trying reconnect.
func (cl *Client) SetTryReconnectPeriod(period time.Duration) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.tryReconnectPeriod = period
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
	var nodes []string
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
		go func(n string) {
			defer wg.Done()
			c, err := cl.cps[n].conn(ctx)
			if err != nil {
				ec <- errors.Wrap(err, "Failed conn")
			}
			sm.Store(n, c)
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

// func (cl *Client) removeNode(node string) {
// 	cl.mu.Lock()
// 	defer cl.mu.Unlock()
// 	cl.hashRing = cl.hashRing.RemoveNode(node)
// }

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

// func (cl *Client) tryReconnect() {
// 	if !cl.failover {
// 		return
// 	}
// 	now := time.Now()
// 	if now.Before(cl.nextTryReconnectAt) {
// 		return
// 	}
// 	defer func() {
// 		cl.mu.Lock()
// 		defer cl.mu.Unlock()
// 		cl.nextTryReconnectAt = now.Add(cl.tryReconnectPeriod)
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
