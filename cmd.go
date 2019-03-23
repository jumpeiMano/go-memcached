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
		_results, err := cp.getOrGat("gat", exp, keys[i*gatMaxKeyNum:limit])
		if err != nil {
			return results, err
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
		if err = cp.putConn(c, err); err != nil {
			cp.logf("Failed putConn: %v", err)
		}
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
	if err = nc.writestrings("touch ", rawkey, " "); err != nil {
		return errors.Wrap(err, "Failed writestrings")
	}
	if err = nc.write(strconv.AppendUint(nil, uint64(exp), 10)); err != nil {
		return errors.Wrap(err, "Failed write")
	}
	if noreply {
		if err = nc.writestring(" noreply "); err != nil {
			return errors.Wrap(err, "Failed writestring")
		}
		if err = nc.writestrings("\r\n"); err != nil {
			return errors.Wrap(err, "Failed writestrings")
		}
		err = nc.flush()
		return err
	}
	if err = nc.writestrings("\r\n"); err != nil {
		return errors.Wrap(err, "Failed writestrings")
	}
	reply, err := nc.readline()
	if err != nil {
		return errors.Wrap(err, "Failed readline")
	}
	if strings.HasPrefix(reply, "NOT_FOUND") {
		return ErrNotFound
	}
	if !strings.HasPrefix(reply, "TOUCHED") {
		return errors.Wrapf(ErrBadConn, "Malformed response: %s", string(reply))
	}
	return nil
}

// Delete delete the value for the specified cache key.
func (cp *ConnectionPool) Delete(noreply bool, keys ...string) (failedKeys []string, err error) {
	ctx := context.Background()
	c, err := cp.conn(ctx)
	if err != nil {
		return []string{}, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		if err = cp.putConn(c, err); err != nil {
			cp.logf("Failed putConn: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, cp.cancelTimeout)
	defer cancel()

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
			if err := nc.writestrings("delete ", rawkey); err != nil {
				ec <- errors.Wrap(err, "Failed writestrings")
				return
			}
			if noreply {
				if err := nc.writestring(" noreply"); err != nil {
					ec <- errors.Wrap(err, "Failed writestring")
					return
				}
				if err := nc.writestrings("\r\n"); err != nil {
					ec <- errors.Wrap(err, "Failed writestrings")
					return
				}
				return
			}
			if err := nc.writestrings("\r\n"); err != nil {
				ec <- errors.Wrap(err, "Failed writestrings")
				return
			}
			reply, err := nc.readline()
			if err != nil {
				ec <- errors.Wrap(err, "Failed readline")
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
		if err := cp.putConn(c, err); err != nil {
			cp.logf("Failed putConn: %v", err)
		}
	}()

	c.Lock()
	defer c.Unlock()

	// flush_all [delay] [noreply]\r\n
	for _, nc := range c.ncs {
		if !nc.isAlive {
			continue
		}
		if err := nc.writestrings("flush_all\r\n"); err != nil {
			return errors.Wrap(err, "Failed writestrings")
		}
		_, err := nc.readline()
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
		if err := cp.putConn(c, err); err != nil {
			cp.logf("Failed putConn: %v", err)
		}
	}()

	c.Lock()
	defer c.Unlock()
	for node := range c.ncs {
		if !c.ncs[node].isAlive {
			continue
		}
		if argument == "" {
			if err := c.ncs[node].writestrings("stats\r\n"); err != nil {
				return resultMap, errors.Wrap(err, "Failed writestrings")
			}
		} else {
			if err := c.ncs[node].writestrings("stats ", argument, "\r\n"); err != nil {
				return resultMap, errors.Wrap(err, "Failed writestrings")
			}
		}
		c.ncs[node].flush()
		var result []byte
		for {
			l, err := c.ncs[node].readline()
			if err != nil {
				return resultMap, errors.Wrap(err, "Failed readline")
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
	ctx := context.Background()
	c, err := cp.conn(ctx)
	if err != nil {
		return results, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		if err := cp.putConn(c, err); err != nil {
			cp.logf("Failed putConn: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, cp.cancelTimeout)
	defer cancel()

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
			if err := nc.writestrings(command); err != nil {
				return []*Item{}, errors.Wrap(err, "Failed writestrings")
			}
			if exp > 0 {
				if err := nc.writestring(" "); err != nil {
					return []*Item{}, errors.Wrap(err, "Failed writestring")
				}
				if err := nc.write(strconv.AppendUint(nil, uint64(exp), 10)); err != nil {
					return []*Item{}, errors.Wrap(err, "Failed write")
				}
			}
		}
		if err := nc.writestrings(" ", rawkey); err != nil {
			return []*Item{}, errors.Wrap(err, "Failed writestrings")
		}
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
			if err := nc.writestrings("\r\n"); err != nil {
				ec <- errors.Wrap(err, "Failed writestrings")
				return
			}
			header, err := nc.readline()
			if err != nil {
				ec <- errors.Wrap(err, "Failed readline")
				return
			}
			for strings.HasPrefix(header, "VALUE") {
				// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
				chunks := strings.Split(header, " ")
				if len(chunks) < 4 {
					ec <- errors.Wrapf(ErrBadConn, "Malformed response: %s", string(header))
					return
				}
				var result Item
				result.Key = cp.removePrefix(chunks[1])
				flags64, err := strconv.ParseUint(chunks[2], 10, 16)
				if err != nil {
					ec <- errors.Wrap(err, "Failed ParseUint")
					return
				}
				result.Flags = uint16(flags64)
				size, err := strconv.ParseUint(chunks[3], 10, 64)
				if err != nil {
					ec <- errors.Wrap(err, "Failed ParseUint")
					return
				}
				if len(chunks) == 5 {
					result.Cas, err = strconv.ParseUint(chunks[4], 10, 64)
					if err != nil {
						ec <- errors.Wrap(err, "Failed ParseUint")
						return
					}
				}
				// <data block>\r\n
				b, err := nc.read(int(size) + 2)
				if err != nil {
					ec <- errors.Wrap(err, "Failed read")
					return
				}
				result.Value = b[:size]
				mu.Lock()
				results = append(results, &result)
				mu.Unlock()
				header, err = nc.readline()
				if err != nil {
					ec <- errors.Wrap(err, "Failed readline")
					return
				}
			}
			if !strings.HasPrefix(header, "END") {
				ec <- errors.Wrapf(ErrBadConn, "Malformed response: %s", string(header))
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
			return results, ErrCanceldByContext
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
	ctx := context.Background()
	c, err := cp.conn(ctx)
	if err != nil {
		return []string{}, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		if err := cp.putConn(c, err); err != nil {
			cp.logf("Failed putConn: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, cp.cancelTimeout)
	defer cancel()

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
			if err := nc.writestrings(command, " ", rawkey, " "); err != nil {
				ec <- errors.Wrap(err, "Failed writestrings")
				return
			}
			if err := nc.write(strconv.AppendUint(nil, uint64(item.Flags), 10)); err != nil {
				ec <- errors.Wrap(err, "Failed write")
				return
			}
			if err := nc.writestring(" "); err != nil {
				ec <- errors.Wrap(err, "Failed writestring")
				return
			}
			if err := nc.write(strconv.AppendUint(nil, uint64(item.Exp), 10)); err != nil {
				ec <- errors.Wrap(err, "Failed write")
				return
			}
			if err := nc.writestring(" "); err != nil {
				ec <- errors.Wrap(err, "Failed writestring")
				return
			}
			if err := nc.write(strconv.AppendInt(nil, int64(len(item.Value)), 10)); err != nil {
				ec <- errors.Wrap(err, "Failed write")
				return
			}
			if item.Cas != 0 {
				if err = nc.writestring(" "); err != nil {
					ec <- errors.Wrap(err, "Failed writestrint")
					return
				}
				if err := nc.write(strconv.AppendUint(nil, item.Cas, 10)); err != nil {
					ec <- errors.Wrap(err, "Failed write")
					return
				}
			}
			if noreply {
				if err := nc.writestring(" noreply"); err != nil {
					ec <- errors.Wrap(err, "Failed writestring")
					return
				}
			}
			if err := nc.writestring("\r\n"); err != nil {
				ec <- errors.Wrap(err, "Failed writestring")
				return
			}
			// <data block>\r\n
			if err := nc.write(item.Value); err != nil {
				ec <- errors.Wrap(err, "Failed writee")
				return
			}
			if err := nc.writestring("\r\n"); err != nil {
				ec <- errors.Wrap(err, "Faield writestring")
				return
			}
			if noreply {
				return
			}
			reply, err := nc.readline()
			if err != nil {
				ec <- errors.Wrap(err, "Failed readline")
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
