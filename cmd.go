package memcached

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

const (
	maxKeyLength = 250
	gatMaxKeyNum = 50
)

// errors
var (
	ErrNonexistentCommand = errors.New("nonexiststent command error")
	ErrClient             = errors.New("client error")
	ErrServer             = errors.New("server error")
	ErrOverMaxKeyLength   = errors.New("key's length is too long")
	ErrCanceldByContext   = errors.New("canceled by context")
)

// Item gives the cached data.
type Item struct {
	Key   string
	Value []byte
	Flags uint16
	Cas   uint64
	Exp   int64
}

// Get returns cached data for given keys.
func (cp *ConnectionPool) Get(keys ...string) (results []*Item, err error) {
	results, err = cp.getOrGat("get", 0, keys)
	return
}

// GetOrSet gets from memcached, and if no hit, Set value gotten by callback, and return the value
func (cp *ConnectionPool) GetOrSet(key string, cb func(key string) (*Item, error)) (*Item, error) {
	items, err := cp.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "Failed Get")
	}
	if len(items) > 0 {
		return items[0], nil
	}
	item, err := cb(key)
	if err != nil {
		return nil, errors.Wrap(err, "Failed cb")
	}
	_, err = cp.Set(false, item)
	return item, errors.Wrap(err, "Failed Set")
}

// GetOrSetMulti gets from memcached, and if no hit, Set value gotten by callback, and return the value
func (cp *ConnectionPool) GetOrSetMulti(keys []string, cb func(keys []string) ([]*Item, error)) ([]*Item, error) {
	items, err := cp.Get(keys...)
	if err != nil {
		return []*Item{}, errors.Wrap(err, "Failed Get")
	}
	gotNum := len(items)
	gotMap := make(map[string]struct{}, gotNum)
	for _, item := range items {
		gotMap[item.Key] = struct{}{}
	}
	remainKeys := make([]string, 0, len(keys)-gotNum)
	for _, key := range keys {
		if _, ok := gotMap[key]; !ok {
			remainKeys = append(remainKeys, key)
		}
	}
	if len(remainKeys) == 0 {
		return items, nil
	}

	cbItems, err := cb(remainKeys)
	if err != nil {
		return []*Item{}, errors.Wrap(err, "Failed cb")
	}
	if len(cbItems) == 0 {
		return items, nil
	}
	if _, err = cp.Set(true, cbItems...); err != nil {
		return items, errors.Wrap(err, "Failed Set")
	}
	items = append(items, cbItems...)
	return items, nil
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (cp *ConnectionPool) Gets(keys ...string) (results []*Item, err error) {
	results, err = cp.getOrGat("gets", 0, keys)
	return
}

// Gat is used to fetch items and update the expiration time of an existing items.
func (cp *ConnectionPool) Gat(exp int64, keys ...string) (results []*Item, err error) {
	keylen := len(keys)
	for i := 0; keylen > i*gatMaxKeyNum; i++ {
		limit := (i + 1) * gatMaxKeyNum
		if keylen < limit {
			limit = keylen
		}
		_results, err1 := cp.getOrGat("gat", exp, keys[i*gatMaxKeyNum:limit])
		if err1 != nil {
			return results, err1
		}
		results = append(results, _results...)
	}
	return
}

// GatOrSet gets from memcached via `gat`, and if no hit, Set value gotten by callback, and return the value
func (cp *ConnectionPool) GatOrSet(key string, exp int64, cb func(key string) (*Item, error)) (*Item, error) {
	items, err := cp.Gat(exp, key)
	if err != nil {
		return nil, errors.Wrap(err, "Failed Gat")
	}
	if len(items) > 0 {
		return items[0], nil
	}
	item, err := cb(key)
	if err != nil {
		return nil, errors.Wrap(err, "Failed cb")
	}
	_, err = cp.Set(false, item)
	return item, errors.Wrap(err, "Failed Set")
}

// GatOrSetMulti gets from memcached via `gat`, and if no hit, Set value gotten by callback, and return the value
func (cp *ConnectionPool) GatOrSetMulti(keys []string, exp int64, cb func(keys []string) ([]*Item, error)) ([]*Item, error) {
	items, err := cp.Gat(exp, keys...)
	if err != nil {
		return []*Item{}, errors.Wrap(err, "Failed Gat")
	}
	gotNum := len(items)
	gotMap := make(map[string]struct{}, gotNum)
	for _, item := range items {
		gotMap[item.Key] = struct{}{}
	}
	remainKeys := make([]string, 0, len(keys)-gotNum)
	for _, key := range keys {
		if _, ok := gotMap[key]; !ok {
			remainKeys = append(remainKeys, key)
		}
	}
	if len(remainKeys) == 0 {
		return items, nil
	}

	cbItems, err := cb(remainKeys)
	if err != nil {
		return []*Item{}, errors.Wrap(err, "Failed cb")
	}
	if len(cbItems) == 0 {
		return items, nil
	}
	if _, err = cp.Set(true, cbItems...); err != nil {
		return items, errors.Wrap(err, "Failed Set")
	}
	items = append(items, cbItems...)
	return items, nil
}

// Gats is used to fetch items and update the expiration time of an existing items.
func (cp *ConnectionPool) Gats(exp int64, keys ...string) (results []*Item, err error) {
	results, err = cp.getOrGat("gats", exp, keys)
	return
}

// Set set the value with specified cache key.
func (cp *ConnectionPool) Set(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cp.store("set", items, noreply)
}

// Add store the value only if it does not already exist.
func (cp *ConnectionPool) Add(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cp.store("add", items, noreply)
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (cp *ConnectionPool) Replace(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cp.store("replace", items, noreply)
}

// Append appends the value after the last bytes in an existing item.
func (cp *ConnectionPool) Append(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cp.store("append", items, noreply)
}

// Prepend prepends the value before existing value.
func (cp *ConnectionPool) Prepend(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cp.store("prepend", items, noreply)
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (cp *ConnectionPool) Cas(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cp.store("cas", items, noreply)
}

// Touch is used to update the expiration time of an existing item without fetching it.
func (cp *ConnectionPool) Touch(key string, exp int64, noreply bool) error {
	c, err := cp.conn(context.Background())
	if err != nil {
		return errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.Lock()
	defer c.Unlock()

	// touch <key> <exptime> [noreply]\r\n
	rawkey := cp.addPrefix(key)
	node, ok := c.hashRing.GetNode(rawkey)
	if !ok {
		return errors.New("Failed GetNode")
	}
	nc, ok := c.ncs[node]
	if !ok {
		return fmt.Errorf("Failed to get a connection: %s", node)
	}
	nc.writestrings("touch ", rawkey, " ")
	nc.write(strconv.AppendUint(nil, uint64(exp), 10))
	if noreply {
		nc.writestring(" noreply ")
		nc.writestrings("\r\n")
		err = nc.flush()
		return err
	}
	nc.writestrings("\r\n")
	reply, err := nc.readline()
	if err != nil {
		return errors.Wrap(err, "Failed readline")
	}
	if strings.HasPrefix(reply, "NOT_FOUND") {
		return ErrNotFound
	}
	if !strings.HasPrefix(reply, "TOUCHED") {
		return fmt.Errorf("Malformed response: %s", string(reply))
	}
	return nil
}

// Delete delete the value for the specified cache key.
func (cp *ConnectionPool) Delete(noreply bool, keys ...string) (failedKeys []string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), cp.pollTimeout)
	defer cancel()
	c, err := cp.conn(ctx)
	if err != nil {
		return []string{}, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.Lock()
	defer c.Unlock()
	c.reset()

	var mu sync.Mutex
	var wg sync.WaitGroup
	ec := make(chan error, len(c.ncs))
	if !noreply {
		ec = make(chan error, len(keys))
	}
	// delete <key> [<time>] [noreply]\r\n
	for _, key := range keys {
		rawkey := cp.addPrefix(key)
		node, ok := c.hashRing.GetNode(rawkey)
		if !ok {
			return []string{}, errors.New("Failed GetNode")
		}
		_nc, ok := c.ncs[node]
		if !ok {
			return []string{}, fmt.Errorf("Failed to get a connection: %s", node)
		}
		_nc.count++
		wg.Add(1)
		go func(nc *nc, key string) {
			defer wg.Done()
			nc.mu.Lock()
			defer nc.mu.Unlock()
			nc.writestrings("delete ", rawkey)
			if noreply {
				nc.writestring(" noreply")
				nc.writestrings("\r\n")
				return
			}
			nc.writestrings("\r\n")
			reply, err1 := nc.readline()
			if err1 != nil {
				ec <- errors.Wrap(err1, "Failed readline")
				return
			}
			if !strings.HasPrefix(reply, "DELETED") {
				mu.Lock()
				failedKeys = append(failedKeys, key)
				mu.Unlock()
			}
			ec <- nil
		}(_nc, key)
	}
	wg.Wait()
	if noreply {
		for _, _nc := range c.ncs {
			if _nc.count == 0 {
				continue
			}
			go func(nc *nc) {
				nc.mu.Lock()
				defer nc.mu.Unlock()
				ec <- nc.flush()
			}(_nc)
		}
	}
	for _, nc := range c.ncs {
		if nc.count == 0 {
			continue
		}
		if noreply {
			select {
			case <-ctx.Done():
				err = ErrCanceldByContext
				return
			case err = <-ec:
				if err != nil {
					return
				}
			}
		} else {
			for i := 0; i < nc.count; i++ {
				select {
				case <-ctx.Done():
					err = ErrCanceldByContext
					return
				case err = <-ec:
					if err != nil {
						return
					}
				}
			}
		}
	}
	return
}

// FlushAll purges the entire cache.
func (cp *ConnectionPool) FlushAll() error {
	c, err := cp.conn(context.Background())
	if err != nil {
		return errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.Lock()
	defer c.Unlock()

	// flush_all [delay] [noreply]\r\n
	for _, nc := range c.ncs {
		if !nc.isAlive {
			continue
		}
		nc.writestrings("flush_all\r\n")
		_, err = nc.readline()
		if err != nil {
			return errors.Wrap(err, "Failed readline")
		}
	}
	return nil
}

// Stats returns a list of basic stats.
func (cp *ConnectionPool) Stats(argument string) (resultMap map[string][]byte, err error) {
	resultMap = map[string][]byte{}
	c, err := cp.conn(context.Background())
	if err != nil {
		return resultMap, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.Lock()
	defer c.Unlock()
	for node := range c.ncs {
		if !c.ncs[node].isAlive {
			continue
		}
		if argument == "" {
			c.ncs[node].writestrings("stats\r\n")
		} else {
			c.ncs[node].writestrings("stats ", argument, "\r\n")
		}
		c.ncs[node].flush()
		var result []byte
		for {
			l, err1 := c.ncs[node].readline()
			if err1 != nil {
				return resultMap, errors.Wrap(err1, "Failed readline")
			}
			if strings.HasPrefix(l, "END") {
				break
			}
			result = append(result, l...)
			result = append(result, '\n')
			if strings.Contains(l, "ERROR") {
				break
			}
		}
		resultMap[node] = result
	}
	return resultMap, err
}

func (cp *ConnectionPool) getOrGat(command string, exp int64, keys []string) ([]*Item, error) {
	var results []*Item
	ctx, cancel := context.WithTimeout(context.Background(), cp.pollTimeout)
	defer cancel()
	c, err := cp.conn(ctx)
	if err != nil {
		return results, err
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.Lock()
	defer c.Unlock()
	c.reset()
	results = make([]*Item, 0, len(keys))
	if len(keys) == 0 {
		return results, nil
	}
	// get(s) <key>*\r\n
	// gat(s) <exp> <key>+\r\n
	for _, key := range keys {
		if key == "" {
			continue
		}
		if len(key) > maxKeyLength {
			return results, ErrOverMaxKeyLength
		}
		rawkey := cp.addPrefix(key)
		node, ok := c.hashRing.GetNode(rawkey)
		if !ok {
			return []*Item{}, errors.New("Failed GetNode")
		}
		nc, ok := c.ncs[node]
		if !ok {
			return []*Item{}, fmt.Errorf("Failed to get a connection: %s", node)
		}
		if nc.count == 0 {
			nc.writestrings(command)
			if exp > 0 {
				nc.writestring(" ")
				nc.write(strconv.AppendUint(nil, uint64(exp), 10))
			}
		}
		nc.writestrings(" ", rawkey)
		nc.count++
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	ec := make(chan error, len(c.ncs))
	for _, _nc := range c.ncs {
		if _nc.count == 0 {
			continue
		}
		wg.Add(1)
		go func(nc *nc) {
			defer wg.Done()
			nc.mu.Lock()
			defer nc.mu.Unlock()
			nc.writestrings("\r\n")
			header, err1 := nc.readline()
			if err1 != nil {
				ec <- errors.Wrap(err1, "Failed readline")
				return
			}
			for strings.HasPrefix(header, "VALUE") {
				// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
				chunks := strings.Split(header, " ")
				if len(chunks) < 4 {
					ec <- fmt.Errorf("Malformed response: %s", string(header))
					return
				}
				var result Item
				result.Key = cp.removePrefix(chunks[1])
				flags64, err1 := strconv.ParseUint(chunks[2], 10, 16)
				if err1 != nil {
					ec <- errors.Wrap(err1, "Failed ParseUint")
					return
				}
				result.Flags = uint16(flags64)
				size, err1 := strconv.ParseUint(chunks[3], 10, 64)
				if err1 != nil {
					ec <- errors.Wrap(err1, "Failed ParseUint")
					return
				}
				if len(chunks) == 5 {
					result.Cas, err1 = strconv.ParseUint(chunks[4], 10, 64)
					if err1 != nil {
						ec <- errors.Wrap(err1, "Failed ParseUint")
						return
					}
				}
				// <data block>\r\n
				b, err1 := nc.read(int(size) + 2)
				if err1 != nil {
					ec <- errors.Wrap(err1, "Failed read")
					return
				}
				result.Value = b[:size]
				mu.Lock()
				results = append(results, &result)
				mu.Unlock()
				header, err1 = nc.readline()
				if err1 != nil {
					ec <- errors.Wrap(err1, "Failed readline")
					return
				}
			}
			if !strings.HasPrefix(header, "END") {
				ec <- fmt.Errorf("Malformed response: %s", string(header))
				return
			}
			ec <- nil
		}(_nc)
	}
	wg.Wait()
	for _, nc := range c.ncs {
		if nc.count == 0 {
			continue
		}
		select {
		case <-ctx.Done():
			err = ErrCanceldByContext
			return results, err
		case err = <-ec:
			if err != nil {
				return results, err
			}
		}
	}
	if err != nil {
		return results, err
	}

	// sort
	resultMap := make(map[string]*Item, len(results))
	for _, result := range results {
		resultMap[result.Key] = result
	}
	_results := make([]*Item, 0, len(results))
	for _, k := range keys {
		if r, ok := resultMap[k]; ok {
			_results = append(_results, r)
		}
	}
	return _results, nil
}

func (cp *ConnectionPool) store(command string, items []*Item, noreply bool) (failedKeys []string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), cp.pollTimeout)
	defer cancel()
	c, err := cp.conn(ctx)
	if err != nil {
		return []string{}, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	var mu sync.Mutex
	var wg sync.WaitGroup
	ec := make(chan error, len(c.ncs))
	if !noreply {
		ec = make(chan error, len(items))
	}

	c.Lock()
	defer c.Unlock()
	c.reset()
	for _, item := range items {
		if item.Key == "" {
			continue
		}
		if len(item.Key) > maxKeyLength {
			return []string{}, ErrOverMaxKeyLength
		}
		rawkey := cp.addPrefix(item.Key)
		node, ok := c.hashRing.GetNode(rawkey)
		if !ok {
			return []string{}, errors.New("Failed GetNode")
		}
		_nc, ok := c.ncs[node]
		if !ok {
			return []string{}, fmt.Errorf("Failed to get a connection: %s", node)
		}
		_nc.count++
		wg.Add(1)
		go func(nc *nc, item *Item) {
			defer wg.Done()
			nc.mu.Lock()
			defer nc.mu.Unlock()
			// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
			nc.writestrings(command, " ", rawkey, " ")
			nc.write(strconv.AppendUint(nil, uint64(item.Flags), 10))
			nc.writestring(" ")
			nc.write(strconv.AppendUint(nil, uint64(item.Exp), 10))
			nc.writestring(" ")
			nc.write(strconv.AppendInt(nil, int64(len(item.Value)), 10))
			if item.Cas != 0 {
				nc.writestring(" ")
				nc.write(strconv.AppendUint(nil, item.Cas, 10))
			}
			if noreply {
				nc.writestring(" noreply")
			}
			nc.writestring("\r\n")
			// <data block>\r\n
			nc.write(item.Value)
			nc.writestring("\r\n")
			if noreply {
				return
			}
			reply, err1 := nc.readline()
			if err1 != nil {
				ec <- errors.Wrap(err1, "Failed readline")
				return
			}
			if !strings.HasPrefix(reply, "STORED") {
				mu.Lock()
				failedKeys = append(failedKeys, item.Key)
				mu.Unlock()
			}
			ec <- nil
		}(_nc, item)
	}
	wg.Wait()
	if noreply {
		for _, _nc := range c.ncs {
			if _nc.count == 0 {
				continue
			}
			go func(nc *nc) {
				nc.mu.Lock()
				defer nc.mu.Unlock()
				ec <- nc.flush()
			}(_nc)
		}
	}

	for _, nc := range c.ncs {
		if nc.count == 0 {
			continue
		}
		if noreply {
			select {
			case <-ctx.Done():
				err = ErrCanceldByContext
				return
			case err = <-ec:
				if err != nil {
					return
				}
			}
		} else {
			for i := 0; i < nc.count; i++ {
				select {
				case <-ctx.Done():
					err = ErrCanceldByContext
					return
				case err = <-ec:
					if err != nil {
						return
					}
				}
			}
		}
	}
	return
}

func handleError(s string) error {
	if !strings.Contains(s, "ERROR") {
		return nil
	}
	if s == "ERROR" {
		return ErrNonexistentCommand
	}
	if strings.HasPrefix(s, "CLIENT_ERROR") {
		return ErrClient
	}
	if strings.HasPrefix(s, "SERVER_ERROR") {
		return ErrServer
	}
	return fmt.Errorf("Error has occured: %s", s)
}
