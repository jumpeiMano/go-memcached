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
func (cl *Client) Get(keys ...string) (results []*Item, err error) {
	results, err = cl.getOrGat("get", 0, keys)
	return
}

// GetOrSet gets from memcached, and if no hit, Set value gotten by callback, and return the value
func (cl *Client) GetOrSet(key string, cb func(key string) (*Item, error)) (*Item, error) {
	items, err := cl.Get(key)
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
	_, err = cl.Set(true, item)
	return item, errors.Wrap(err, "Failed Set")
}

// GetOrSetMulti gets from memcached, and if no hit, Set value gotten by callback, and return the value
func (cl *Client) GetOrSetMulti(keys []string, cb func(keys []string) ([]*Item, error)) ([]*Item, error) {
	items, err := cl.Get(keys...)
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
	if _, err = cl.Set(true, cbItems...); err != nil {
		return items, errors.Wrap(err, "Failed Set")
	}
	items = append(items, cbItems...)
	return items, nil
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (cl *Client) Gets(keys ...string) (results []*Item, err error) {
	results, err = cl.getOrGat("gets", 0, keys)
	return
}

// Gat is used to fetch items and update the expiration time of an existing items.
func (cl *Client) Gat(exp int64, keys ...string) (results []*Item, err error) {
	keylen := len(keys)
	for i := 0; keylen > i*gatMaxKeyNum; i++ {
		limit := (i + 1) * gatMaxKeyNum
		if keylen < limit {
			limit = keylen
		}
		_results, err := cl.getOrGat("gat", exp, keys[i*gatMaxKeyNum:limit])
		if err != nil {
			return results, err
		}
		results = append(results, _results...)
	}
	return
}

// GatOrSet gets from memcached via `gat`, and if no hit, Set value gotten by callback, and return the value
func (cl *Client) GatOrSet(key string, exp int64, cb func(key string) (*Item, error)) (*Item, error) {
	items, err := cl.Gat(exp, key)
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
	_, err = cl.Set(true, item)
	return item, errors.Wrap(err, "Failed Set")
}

// GatOrSetMulti gets from memcached via `gat`, and if no hit, Set value gotten by callback, and return the value
func (cl *Client) GatOrSetMulti(keys []string, exp int64, cb func(keys []string) ([]*Item, error)) ([]*Item, error) {
	items, err := cl.Gat(exp, keys...)
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
	if _, err = cl.Set(true, cbItems...); err != nil {
		return items, errors.Wrap(err, "Failed Set")
	}
	items = append(items, cbItems...)
	return items, nil
}

// Gats is used to fetch items and update the expiration time of an existing items.
func (cl *Client) Gats(exp int64, keys ...string) (results []*Item, err error) {
	results, err = cl.getOrGat("gats", exp, keys)
	return
}

// Set set the value with specified cache key.
func (cl *Client) Set(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cl.store("set", items, noreply)
}

// Add store the value only if it does not already exist.
func (cl *Client) Add(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cl.store("add", items, noreply)
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (cl *Client) Replace(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cl.store("replace", items, noreply)
}

// Append appends the value after the last bytes in an existing item.
func (cl *Client) Append(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cl.store("append", items, noreply)
}

// Prepend prepends the value before existing value.
func (cl *Client) Prepend(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cl.store("prepend", items, noreply)
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (cl *Client) Cas(noreply bool, items ...*Item) (failedKeys []string, err error) {
	return cl.store("cas", items, noreply)
}

// Touch is used to update the expiration time of an existing item without fetching it.
func (cl *Client) Touch(key string, exp int64, noreply bool) error {
	// touch <key> <exptime> [noreply]\r\n
	rawkey := cl.addPrefix(key)
	node, ok := cl.hashRing.GetNode(rawkey)
	if !ok {
		return errors.New("Failed GetNode")
	}
	cp := cl.cps[node]
	c, err := cp.conn(context.Background())
	if err != nil {
		return errors.Wrap(err, "Failed cl.conn")
	}
	defer func() {
		if err = cp.putConn(c, err); err != nil {
			cl.logf("Failed putConn: %v", err)
		}
	}()
	cmd := fmt.Sprintf("touch %s %d", rawkey, exp)
	if noreply {
		cmd += " noreply\r\n"
		if err = c.writestring(cmd); err != nil {
			return errors.Wrap(err, "Failed writestring")
		}
		err = c.flush()
		return err
	}
	cmd += "\r\n"
	if err = c.writestring(cmd); err != nil {
		return errors.Wrap(err, "Failed writestrings")
	}
	reply, err1 := c.readline()
	if err1 != nil {
		err = errors.Wrap(err1, "Failed readline")
		return err
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
func (cl *Client) Delete(noreply bool, keys ...string) (failedKeys []string, err error) {
	connMap, err := cl.conn(keys...)
	if err != nil {
		return []string{}, errors.Wrap(err, "Failed cl.conn")
	}
	defer func() {
		cl.putConn(connMap, err)
	}()

	kl := len(keys)
	ec := make(chan error, len(connMap))
	if !noreply {
		ec = make(chan error, kl)
	}

	var wg sync.WaitGroup
	failedKeyChan := make(chan string, kl)

	// delete <key> [<time>] [noreply]\r\n
	for _, key := range keys {
		rawkey := cl.addPrefix(key)
		node, ok := cl.hashRing.GetNode(rawkey)
		if !ok {
			return []string{}, errors.New("Failed GetNode")
		}
		c, ok := connMap[node]
		if !ok {
			return []string{}, fmt.Errorf("Failed to get a connection: %s", node)
		}
		wg.Add(1)
		go func(c *conn, key, rawkey string) {
			defer wg.Done()
			c.mu.Lock()
			defer c.mu.Unlock()
			cmd := fmt.Sprintf("delete %s", rawkey)
			if noreply {
				cmd += " noreply\r\n"
				if err := c.writestring(cmd); err != nil {
					ec <- errors.Wrap(err, "Failed writestring")
					return
				}
				return
			}
			cmd += "\r\n"
			if err := c.writestring(cmd); err != nil {
				ec <- errors.Wrap(err, "Failed writestring")
				return
			}
			reply, err := c.readline()
			if err != nil {
				ec <- errors.Wrap(err, "Failed readline")
				return
			}
			if !strings.HasPrefix(reply, "DELETED") {
				failedKeyChan <- key
			}
		}(c, key, rawkey)
	}
	wg.Wait()
	close(failedKeyChan)
	if noreply {
		for _, c := range connMap {
			wg.Add(1)
			go func(c *conn) {
				defer wg.Done()
				c.mu.Lock()
				defer c.mu.Unlock()
				if err := c.flush(); err != nil {
					ec <- errors.Wrap(err, "Failed flush")
				}
			}(c)
		}
		wg.Wait()
	}
	close(ec)
	for err = range ec {
		if err != nil {
			return
		}
	}
	if noreply {
		return
	}
	for fk := range failedKeyChan {
		if len(fk) > 0 {
			failedKeys = append(failedKeys, fk)
		}
	}
	return
}

// FlushAll purges the entire cache.
func (cl *Client) FlushAll() error {
	connMap, err := cl.conn()
	if err != nil {
		return errors.Wrap(err, "Failed cl.conn")
	}
	defer func() {
		cl.putConn(connMap, err)
	}()

	// flush_all [delay] [noreply]\r\n
	for _, c := range connMap {
		if err = c.writestrings("flush_all\r\n"); err != nil {
			return errors.Wrap(err, "Failed writestrings")
		}
		_, err = c.readline()
		if err != nil {
			return errors.Wrap(err, "Failed readline")
		}
	}
	return nil
}

// Stats returns a list of basic stats.
func (cl *Client) Stats(argument string) (resultMap map[string][]byte, err error) {
	resultMap = map[string][]byte{}
	connMap, err := cl.conn()
	if err != nil {
		return resultMap, errors.Wrap(err, "Failed cl.conn")
	}
	defer func() {
		cl.putConn(connMap, err)
	}()

	for node, c := range connMap {
		if argument == "" {
			if err := c.writestrings("stats\r\n"); err != nil {
				return resultMap, errors.Wrap(err, "Failed writestrings")
			}
		} else {
			if err := c.writestrings("stats ", argument, "\r\n"); err != nil {
				return resultMap, errors.Wrap(err, "Failed writestrings")
			}
		}
		var result []byte
		for {
			l, err1 := c.readline()
			if err1 != nil {
				err = errors.Wrap(err1, "Failed readline")
				return resultMap, err
			}
			if strings.HasPrefix(l, "END") {
				break
			}
			if err = handleError(l); err != nil {
				return resultMap, err
			}
			result = append(result, l...)
			result = append(result, '\n')
		}
		resultMap[node] = result
	}
	return resultMap, nil
}

func (cl *Client) getOrGat(command string, exp int64, keys []string) ([]*Item, error) {
	var results []*Item
	lk := len(keys)
	resultChan := make(chan *Item, lk)
	results = make([]*Item, 0, lk)
	if len(keys) == 0 {
		return results, nil
	}
	connMap, err := cl.conn(keys...)
	if err != nil {
		return results, errors.Wrap(err, "Failed cl.conn")
	}
	defer func() {
		cl.putConn(connMap, err)
	}()

	// get(s) <key>*\r\n
	// gat(s) <exp> <key>+\r\n
	for _, key := range keys {
		if key == "" {
			continue
		}
		if len(key) > maxKeyLength {
			return results, ErrOverMaxKeyLength
		}
		rawkey := cl.addPrefix(key)
		node, ok := cl.hashRing.GetNode(rawkey)
		if !ok {
			return []*Item{}, errors.New("Failed GetNode")
		}
		c, ok := connMap[node]
		if !ok {
			return results, fmt.Errorf("Failed to get a connection: %s", node)
		}
		var cmd string
		if c.buffered.Writer.Buffered() == 0 {
			cmd = command
			if exp > 0 {
				cmd += fmt.Sprintf(" %d", exp)
			}
		}
		cmd += fmt.Sprintf(" %s", rawkey)
		if err = c.writestrings(cmd); err != nil {
			return results, errors.Wrap(err, "Failed writestrings")
		}
	}

	var wg sync.WaitGroup
	ec := make(chan error, len(connMap))
	for _, c := range connMap {
		wg.Add(1)
		go func(c *conn) {
			defer wg.Done()
			c.mu.Lock()
			defer c.mu.Unlock()
			if err := c.writestrings("\r\n"); err != nil {
				ec <- errors.Wrap(err, "Failed writestrings")
				return
			}
			header, err := c.readline()
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
				result.Key = cl.removePrefix(chunks[1])
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
				b, err := c.read(int(size) + 2)
				if err != nil {
					ec <- errors.Wrap(err, "Failed read")
					return
				}
				result.Value = b[:size]
				resultChan <- &result
				header, err = c.readline()
				if err != nil {
					ec <- errors.Wrap(err, "Failed readline")
					return
				}
			}
			if !strings.HasPrefix(header, "END") {
				ec <- errors.Wrapf(ErrBadConn, "Malformed response: %s", string(header))
				return
			}
		}(c)
	}
	wg.Wait()
	close(ec)
	close(resultChan)
	for err = range ec {
		if err != nil {
			return results, err
		}
	}
	resultMap := make(map[string]*Item, len(results))
	for result := range resultChan {
		if result != nil {
			resultMap[result.Key] = result
		}
	}
	sortedResult := make([]*Item, 0, len(results))
	for _, k := range keys {
		if r, ok := resultMap[k]; ok {
			sortedResult = append(sortedResult, r)
		}
	}
	return sortedResult, nil
}

func (cl *Client) store(command string, items []*Item, noreply bool) ([]string, error) {
	il := len(items)
	keys := make([]string, il)
	failedKeys := make([]string, 0, il)
	for i, item := range items {
		keys[i] = item.Key
	}
	connMap, err := cl.conn(keys...)
	if err != nil {
		return failedKeys, errors.Wrap(err, "Failed conn")
	}
	defer func() {
		cl.putConn(connMap, err)
	}()

	var wg sync.WaitGroup
	ec := make(chan error, il)
	failedKeyChan := make(chan string, il)

	for _, item := range items {
		if len(item.Key) == 0 {
			continue
		}
		if len(item.Key) > maxKeyLength {
			return failedKeys, ErrOverMaxKeyLength
		}
		rawkey := cl.addPrefix(item.Key)
		node, ok := cl.hashRing.GetNode(rawkey)
		if !ok {
			return failedKeys, errors.New("Failed GetNode")
		}
		c, ok := connMap[node]
		if !ok {
			return failedKeys, fmt.Errorf("Failed to get a connection: %s", node)
		}
		wg.Add(1)
		go func(c *conn, item *Item, rawkey string) {
			defer wg.Done()
			c.mu.Lock()
			defer c.mu.Unlock()
			// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
			cmd := fmt.Sprintf(
				"%s %s %d %d %d ",
				command, rawkey, item.Flags, item.Exp, len(item.Value),
			)
			if item.Cas != 0 {
				cmd += fmt.Sprintf(" %d", item.Cas)
			}
			if noreply {
				cmd += " noreply"
			}
			cmd += "\r\n"
			// <data block>\r\n
			if err := c.writestring(cmd); err != nil {
				ec <- errors.Wrap(err, "Failed writestring")
				return
			}
			if err := c.write(item.Value); err != nil {
				ec <- errors.Wrap(err, "Failed writee")
				return
			}
			if err := c.writestring("\r\n"); err != nil {
				ec <- errors.Wrap(err, "Faield writestring")
				return
			}
			if noreply {
				return
			}
			reply, err := c.readline()
			if err != nil {
				ec <- errors.Wrap(err, "Failed readline")
				return
			}
			if !strings.HasPrefix(reply, "STORED") {
				failedKeyChan <- item.Key
			}
		}(c, item, rawkey)
	}
	wg.Wait()
	if noreply {
		for _, _c := range connMap {
			wg.Add(1)
			go func(c *conn) {
				defer wg.Done()
				c.mu.Lock()
				defer c.mu.Unlock()
				if err := c.flush(); err != nil {
					ec <- errors.Wrap(err, "Failed flush")
				}
			}(_c)
		}
	}
	wg.Wait()
	close(failedKeyChan)
	close(ec)
	for err = range ec {
		if err != nil {
			return failedKeys, err
		}
	}
	for fk := range failedKeyChan {
		if len(fk) > 0 {
			failedKeys = append(failedKeys, fk)
		}
	}
	return failedKeys, nil
}
