package memcached

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	ErrNonexistentCommand = errors.New("nonexiststent command error")
	ErrClient             = errors.New("client error")
	ErrServer             = errors.New("server error")
)

// Get returns cached data for given keys.
func (cp *ConnectionPool) Get(keys ...string) (results []*Item, err error) {
	results, err = cp.get("get", keys)
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
	_, err = cp.Set(item)
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
	if _, err = cp.Set(cbItems...); err != nil {
		return items, errors.Wrap(err, "Failed Set")
	}
	items = append(items, cbItems...)
	return items, nil
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (cp *ConnectionPool) Gets(keys ...string) (results []*Item, err error) {
	results, err = cp.get("gets", keys)
	return
}

// Set set the value with specified cache key.
func (cp *ConnectionPool) Set(items ...*Item) (failedKeys []string, err error) {
	return cp.store("set", items)
}

// Add store the value only if it does not already exist.
func (cp *ConnectionPool) Add(items ...*Item) (failedKeys []string, err error) {
	return cp.store("add", items)
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (cp *ConnectionPool) Replace(items ...*Item) (failedKeys []string, err error) {
	return cp.store("replace", items)
}

// Append appends the value after the last bytes in an existing item.
func (cp *ConnectionPool) Append(items ...*Item) (failedKeys []string, err error) {
	return cp.store("append", items)
}

// Prepend prepends the value before existing value.
func (cp *ConnectionPool) Prepend(items ...*Item) (failedKeys []string, err error) {
	return cp.store("prepend", items)
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (cp *ConnectionPool) Cas(items ...*Item) (failedKeys []string, err error) {
	return cp.store("cas", items)
}

// Delete delete the value for the specified cache key.
func (cp *ConnectionPool) Delete(keys ...string) (failedKeys []string, err error) {
	c, err := cp.conn(context.Background())
	if err != nil {
		return []string{}, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.reset()
	// delete <key> [<time>] [noreply]\r\n
	for _, key := range keys {
		rawkey := cp.addPrefix(key)
		node, _ := c.hashRing.GetNode(rawkey)
		c.ncs[node].writestrings("delete ", rawkey)
		if cp.noreply {
			c.ncs[node].writestring(" noreply")
			c.ncs[node].writestrings("\r\n")
			c.ncs[node].count++
			continue
		}
		c.ncs[node].writestrings("\r\n")
		c.setDeadline()
		reply, err1 := c.ncs[node].readline()
		if err1 != nil {
			return []string{}, errors.Wrap(err1, "Failed readline")
		}
		if !strings.HasPrefix(reply, "DELETED") {
			failedKeys = append(failedKeys, key)
		}
	}
	if cp.noreply {
		for node := range c.ncs {
			if c.ncs[node].count == 0 {
				continue
			}
			c.setDeadline()
			err = c.ncs[node].flush()
			if err != nil {
				return []string{}, errors.Wrap(err, "Failed flush")
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

	c.setDeadline()
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

	for node := range c.ncs {
		if !c.ncs[node].isAlive {
			continue
		}
		if argument == "" {
			c.ncs[node].writestrings("stats\r\n")
		} else {
			c.ncs[node].writestrings("stats ", argument, "\r\n")
		}
		c.setDeadline()
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

func (cp *ConnectionPool) get(command string, keys []string) ([]*Item, error) {
	var results []*Item
	c, err := cp.conn(context.Background())
	if err != nil {
		return results, err
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.reset()
	results = make([]*Item, 0, len(keys))
	if len(keys) == 0 {
		return results, nil
	}
	// get(s) <key>*\r\n
	for _, key := range keys {
		rawkey := cp.addPrefix(key)
		node, _ := c.hashRing.GetNode(rawkey)
		if c.ncs[node].count == 0 {
			c.ncs[node].writestrings(command)
		}
		c.ncs[node].writestrings(" ", rawkey)
		c.ncs[node].count++
	}

	for node := range c.ncs {
		if c.ncs[node].count == 0 {
			continue
		}
		c.ncs[node].writestrings("\r\n")
		header, err := c.ncs[node].readline()
		if err != nil {
			return results, errors.Wrap(err, "Failed readline")
		}
		for strings.HasPrefix(header, "VALUE") {
			// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
			chunks := strings.Split(header, " ")
			if len(chunks) < 4 {
				return results, fmt.Errorf("Malformed response: %s", string(header))
			}
			var result Item
			result.Key = cp.removePrefix(chunks[1])
			flags64, err := strconv.ParseUint(chunks[2], 10, 16)
			if err != nil {
				return results, errors.Wrap(err, "Failed ParseUint")
			}
			result.Flags = uint16(flags64)
			size, err := strconv.ParseUint(chunks[3], 10, 64)
			if err != nil {
				return results, errors.Wrap(err, "Failed ParseUint")
			}
			if len(chunks) == 5 {
				result.Cas, err = strconv.ParseUint(chunks[4], 10, 64)
				if err != nil {
					return results, errors.Wrap(err, "Failed ParseUint")
				}
			}
			// <data block>\r\n
			c.setDeadline()
			b, err := c.ncs[node].read(int(size) + 2)
			if err != nil {
				return results, errors.Wrap(err, "Failed read")
			}
			result.Value = b[:size]
			results = append(results, &result)
			header, err = c.ncs[node].readline()
			if err != nil {
				return results, errors.Wrap(err, "Failed readline")
			}
		}
		if !strings.HasPrefix(header, "END") {
			return results, fmt.Errorf("Malformed response: %s", string(header))
		}
	}
	return results, nil
}

func (cp *ConnectionPool) store(command string, items []*Item) (failedKeys []string, err error) {
	c, err := cp.conn(context.Background())
	if err != nil {
		return []string{}, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.reset()
	for _, item := range items {
		rawkey := cp.addPrefix(item.Key)
		node, _ := c.hashRing.GetNode(rawkey)
		// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
		c.ncs[node].writestrings(command, " ", rawkey, " ")
		c.ncs[node].write(strconv.AppendUint(nil, uint64(item.Flags), 10))
		c.ncs[node].writestring(" ")
		c.ncs[node].write(strconv.AppendUint(nil, uint64(item.Exp), 10))
		c.ncs[node].writestring(" ")
		c.ncs[node].write(strconv.AppendInt(nil, int64(len(item.Value)), 10))
		if item.Cas != 0 {
			c.ncs[node].writestring(" ")
			c.ncs[node].write(strconv.AppendUint(nil, item.Cas, 10))
		}
		if cp.noreply {
			c.ncs[node].writestring(" noreply")
		}
		c.ncs[node].writestring("\r\n")
		// <data block>\r\n
		c.ncs[node].write(item.Value)
		c.ncs[node].writestring("\r\n")
		if cp.noreply {
			c.ncs[node].count++
			continue
		}
		c.setDeadline()
		reply, err1 := c.ncs[node].readline()
		if err1 != nil {
			return []string{}, errors.Wrap(err1, "Failed readline")
		}
		if !strings.HasPrefix(reply, "STORED") {
			failedKeys = append(failedKeys, item.Key)
		}
	}
	if cp.noreply {
		for node := range c.ncs {
			if c.ncs[node].count == 0 {
				continue
			}
			c.setDeadline()
			err = c.ncs[node].flush()
			if err != nil {
				return []string{}, errors.Wrap(err, "Failed flush")
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
