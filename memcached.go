package memcached

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Item gives the cached data.
type Item struct {
	Key   string
	Value []byte
	Flags uint16
	Cas   uint64
}

const connRequestQueueSize = 1000000

// DefaultPort memcached port
const DefaultPort = 11211

// Error
var (
	ErrMemcachedClosed = errors.New("memcached is closed")
	ErrBadConn         = errors.New("bad conn")
)

// ConnectionPool struct
type ConnectionPool struct {
	servers        []string // []string{{<host>:[<port>] [alias]}}
	prefix         string
	noreply        bool
	hashFunc       int
	connectTimeout time.Duration
	mu             sync.Mutex
	freeConns      []*conn
	numOpen        int
	openerCh       chan struct{}
	connRequests   map[uint64]chan connRequest
	nextRequest    uint64
	maxLifetime    time.Duration // maximum amount of time a connection may be reused
	maxOpen        int           // maximum amount of connection num. maxOpen <= 0 means unlimited.
	cleanerCh      chan struct{}
	closed         bool
}

type connRequest struct {
	*conn
	err error
}

type conn struct {
	cp *ConnectionPool
	sync.Mutex
	ncs       []*nc
	createdAt time.Time
	closed    bool
}

type nc struct {
	net.Conn
	buffered bufio.ReadWriter
}

// New create ConnectionPool
func New(servers []string, noreply bool, prefix string) (cp *ConnectionPool) {
	cp = new(ConnectionPool)
	cp.servers = servers
	cp.prefix = prefix
	cp.noreply = noreply
	cp.openerCh = make(chan struct{}, connRequestQueueSize)
	cp.connRequests = make(map[uint64]chan connRequest)
	cp.maxOpen = 1 // default value

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
	c, err := cp.connect()
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
	if err == ErrBadConn || !cp.putConnLocked(c, nil) {
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
	if err == ErrBadConn {
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
		return nil, ctx.Err()
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
		return c, nil
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
			return nil, ctx.Err()
		case ret, ok := <-req:
			if !ok {
				return nil, ErrMemcachedClosed
			}
			return ret.conn, ret.err
		}
	}

	cp.numOpen++
	cp.mu.Unlock()
	newCn, err := cp.connect()
	if err != nil {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		cp.numOpen--
		cp.maybeOpenNewConnections()
		return nil, err
	}
	return newCn, nil
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
	// wake cleaner up when lifetime is shortened.
	if d > 0 && d < cp.maxLifetime && cp.cleanerCh != nil {
		select {
		case cp.cleanerCh <- struct{}{}:
		default:
		}
	}
	cp.maxLifetime = d
	cp.startCleanerLocked()
	cp.mu.Unlock()
}

// SetConnMaxOpen sets the maximum amount of opening connections.
func (cp *ConnectionPool) SetConnMaxOpen(maxOpen int) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.maxOpen = maxOpen
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
			for i := range c.ncs {
				if err := c.ncs[i].Conn.Close(); err != nil {
					log.Println("Failed conn.close", err)
				}
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

func (cp *ConnectionPool) connect() (*conn, error) {
	var network string
	ls := len(cp.servers)
	c := conn{
		cp:        cp,
		ncs:       make([]*nc, ls),
		createdAt: time.Now(),
	}
	for i, s := range cp.servers {
		if strings.Contains(s, "/") {
			network = "unix"
		} else {
			network = "tcp"
		}
		var _nc nc
		var err error
		_nc.Conn, err = net.DialTimeout(network, s, cp.connectTimeout)
		if err != nil {
			return nil, err
		}
		_nc.buffered = bufio.ReadWriter{
			Reader: bufio.NewReader(_nc),
			Writer: bufio.NewWriter(_nc),
		}
		c.ncs[i] = &_nc
	}
	return &c, nil
}

func (c *conn) close() error {
	for i := range c.ncs {
		if err := c.ncs[i].Conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Get returns cached data for given keys.
func (cp *ConnectionPool) Get(keys ...string) (results []Item, err error) {
	defer handleError(&err)
	results = cp.get("get", keys)
	return
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (cp *ConnectionPool) Gets(keys ...string) (results []Item, err error) {
	defer handleError(&err)
	results = cp.get("gets", keys)
	return
}

// Set set the value with specified cache key.
func (cp *ConnectionPool) Set(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return cp.store("set", key, flags, timeout, value, 0), nil
}

// Add store the value only if it does not already exist.
func (cp *ConnectionPool) Add(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return cp.store("add", key, flags, timeout, value, 0), nil
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (cp *ConnectionPool) Replace(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return cp.store("replace", key, flags, timeout, value, 0), nil
}

// Append appends the value after the last bytes in an existing item.
func (cp *ConnectionPool) Append(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return cp.store("append", key, flags, timeout, value, 0), nil
}

// Prepend prepends the value before existing value.
func (cp *ConnectionPool) Prepend(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error) {
	defer handleError(&err)
	return cp.store("prepend", key, flags, timeout, value, 0), nil
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (cp *ConnectionPool) Cas(key string, flags uint16, timeout uint64, value []byte, cas uint64) (stored bool, err error) {
	defer handleError(&err)
	return cp.store("cas", key, flags, timeout, value, cas), nil
}

// Delete delete the value for the specified cache key.
func (cp *ConnectionPool) Delete(key string) (deleted bool, err error) {
	defer handleError(&err)

	c, err := cp.conn(context.Background())
	if err != nil {
		return false, err
	}
	defer func() {
		cp.putConn(c, err)
	}()

	// delete <key> [<time>] [noreply]\r\n
	c.writestrings("delete ", key, "\r\n")
	reply := c.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewError("Server error"))
	}
	return strings.HasPrefix(reply, "DELETED"), nil
}

// FlushAll purges the entire cache.
func (cp *ConnectionPool) FlushAll() (err error) {
	defer handleError(&err)
	c, err := cp.conn(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.setDeadline()
	// flush_all [delay] [noreply]\r\n
	c.writestrings("flush_all\r\n")
	response := c.readline()
	if !strings.Contains(response, "OK") {
		panic(NewError(fmt.Sprintf("Error in FlushAll %v", response)))
	}
	return nil
}

// Stats returns a list of basic stats.
func (cp *ConnectionPool) Stats(argument string) (result []byte, err error) {
	defer handleError(&err)
	c, err := cp.conn(context.Background())
	if err != nil {
		return result, err
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.setDeadline()
	if argument == "" {
		c.writestrings("stats\r\n")
	} else {
		c.writestrings("stats ", argument, "\r\n")
	}
	c.flush()
	for {
		l := c.readline()
		if strings.HasPrefix(l, "END") {
			break
		}
		if strings.Contains(l, "ERROR") {
			return nil, NewError(l)
		}
		result = append(result, l...)
		result = append(result, '\n')
	}
	return result, err
}

func (cp *ConnectionPool) get(command string, keys []string) (results []Item) {
	c, err := cp.conn(context.Background())
	if err != nil {
		return
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.setDeadline()
	results = make([]Item, 0, len(keys))
	if len(keys) == 0 {
		return
	}
	// get(s) <key>*\r\n
	c.writestrings(command)
	for _, key := range keys {
		c.writestrings(" ", key)
	}
	c.writestrings("\r\n")
	header := c.readline()
	var result Item
	for strings.HasPrefix(header, "VALUE") {
		// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		chunks := strings.Split(header, " ")
		if len(chunks) < 4 {
			panic(NewError("Malformed response: %s", string(header)))
		}
		result.Key = chunks[1]
		flags64, err := strconv.ParseUint(chunks[2], 10, 16)
		if err != nil {
			panic(NewError("%v", err))
		}
		result.Flags = uint16(flags64)
		size, err := strconv.ParseUint(chunks[3], 10, 64)
		if err != nil {
			panic(NewError("%v", err))
		}
		if len(chunks) == 5 {
			result.Cas, err = strconv.ParseUint(chunks[4], 10, 64)
			if err != nil {
				panic(NewError("%v", err))
			}
		}
		// <data block>\r\n
		result.Value = c.read(int(size) + 2)[:size]
		results = append(results, result)
		header = c.readline()
	}
	if !strings.HasPrefix(header, "END") {
		panic(NewError("Malformed response: %s", string(header)))
	}
	return
}

func (cp *ConnectionPool) store(command, key string, flags uint16, timeout uint64, value []byte, cas uint64) (stored bool) {
	if len(value) > 1000000 {
		return false
	}

	c, err := cp.conn(context.Background())
	if err != nil {
		return false
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.setDeadline()
	// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	c.writestrings(command, " ", key, " ")
	c.write(strconv.AppendUint(nil, uint64(flags), 10))
	c.writestring(" ")
	c.write(strconv.AppendUint(nil, timeout, 10))
	c.writestring(" ")
	c.write(strconv.AppendInt(nil, int64(len(value)), 10))
	if cas != 0 {
		c.writestring(" ")
		c.write(strconv.AppendUint(nil, cas, 10))
	}
	c.writestring("\r\n")
	// <data block>\r\n
	c.write(value)
	c.writestring("\r\n")
	reply := c.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewError("Server error"))
	}
	return strings.HasPrefix(reply, "STORED")
}

func (c *conn) writestrings(strs ...string) {
	for _, s := range strs {
		c.writestring(s)
	}
}

func (c *conn) writestring(s string) {
	if _, err := c.ncs[0].buffered.WriteString(s); err != nil {
		panic(NewError("%s", err))
	}
}

func (c *conn) write(b []byte) {
	if _, err := c.ncs[0].buffered.Write(b); err != nil {
		panic(NewError("%s", err))
	}
}

func (c *conn) flush() {
	if err := c.ncs[0].buffered.Flush(); err != nil {
		panic(NewError("%s", err))
	}
}

func (c *conn) readline() string {
	c.flush()
	l, isPrefix, err := c.ncs[0].buffered.ReadLine()
	if isPrefix || err != nil {
		panic(NewError("Prefix: %v, %s", isPrefix, err))
	}
	return string(l)
}

func (c *conn) read(count int) []byte {
	c.flush()
	b := make([]byte, count)
	if _, err := io.ReadFull(c.ncs[0].buffered, b); err != nil {
		panic(NewError("%s", err))
	}
	return b
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

func (c *conn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return c.createdAt.Add(timeout).Before(time.Now())
}

func (c *conn) setDeadline() {
	for i := range c.ncs {
		if err := c.ncs[i].Conn.SetDeadline(time.Now().Add(c.cp.connectTimeout)); err != nil {
			panic(NewError("%s", err))
		}
	}
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

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(Error)
	}
}
