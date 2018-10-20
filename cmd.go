package memcached

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// errors
var (
	ErrBadRequest = errors.New("Bad Request")
	ErrServer     = errors.New("Server Error")
)

// Get returns cached data for given keys.
func (cp *ConnectionPool) Get(keys ...string) (results []Item, err error) {
	results, err = cp.get("get", keys)
	return
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (cp *ConnectionPool) Gets(keys ...string) (results []Item, err error) {
	results, err = cp.get("gets", keys)
	return
}

// Set set the value with specified cache key.
func (cp *ConnectionPool) Set(item Item) (stored bool, err error) {
	return cp.store("set", item)
}

// Add store the value only if it does not already exist.
func (cp *ConnectionPool) Add(item Item) (stored bool, err error) {
	return cp.store("add", item)
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (cp *ConnectionPool) Replace(item Item) (stored bool, err error) {
	return cp.store("replace", item)
}

// Append appends the value after the last bytes in an existing item.
func (cp *ConnectionPool) Append(item Item) (stored bool, err error) {
	return cp.store("append", item)
}

// Prepend prepends the value before existing value.
func (cp *ConnectionPool) Prepend(item Item) (stored bool, err error) {
	return cp.store("prepend", item)
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (cp *ConnectionPool) Cas(item Item) (stored bool, err error) {
	return cp.store("cas", item)
}

// Delete delete the value for the specified cache key.
func (cp *ConnectionPool) Delete(key string) (deleted bool, err error) {
	c, err := cp.conn(context.Background())
	if err != nil {
		return false, err
	}
	defer func() {
		cp.putConn(c, err)
	}()

	// delete <key> [<time>] [noreply]\r\n
	node, _ := c.hashRing.GetNode(key)
	nc := c.ncs[node]
	nc.writestrings("delete ", key, "\r\n")
	reply, err := nc.readline()
	if err != nil {
		return false, errors.Wrap(err, "Failed readline")
	}
	return strings.HasPrefix(reply, "DELETED"), nil
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
	for _, s := range cp.servers {
		nc := c.ncs[s.Alias]
		nc.writestrings("flush_all\r\n")
		_, err = nc.readline()
		if err != nil {
			return errors.Wrap(err, "Failed readline")
		}
	}
	return nil
}

// // Stats returns a list of basic stats.
// func (cp *ConnectionPool) Stats(argument string) (result []byte, err error) {
// 	defer handleError(&err)
// 	c, err := cp.conn(context.Background())
// 	if err != nil {
// 		return result, err
// 	}
// 	defer func() {
// 		cp.putConn(c, err)
// 	}()
//
// 	c.setDeadline()
// 	if argument == "" {
// 		c.writestrings("stats\r\n")
// 	} else {
// 		c.writestrings("stats ", argument, "\r\n")
// 	}
// 	c.flush()
// 	for {
// 		l := c.readline()
// 		if strings.HasPrefix(l, "END") {
// 			break
// 		}
// 		if strings.Contains(l, "ERROR") {
// 			return nil, NewError(l)
// 		}
// 		result = append(result, l...)
// 		result = append(result, '\n')
// 	}
// 	return result, err
// }
//
func (cp *ConnectionPool) get(command string, keys []string) ([]Item, error) {
	var results []Item
	c, err := cp.conn(context.Background())
	if err != nil {
		return results, err
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.reset()
	c.setDeadline()
	results = make([]Item, 0, len(keys))
	if len(keys) == 0 {
		return results, nil
	}
	// get(s) <key>*\r\n
	for _, key := range keys {
		node, _ := c.hashRing.GetNode(key)
		if c.ncs[node].count == 0 {
			c.ncs[node].writestrings(command)
		}
		c.ncs[node].writestrings(" ", key)
		c.ncs[node].count++
	}

	for node := range c.ncs {
		if c.ncs[node].count == 0 {
			continue
		}
		var result Item
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
			result.Key = chunks[1]
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
			b, err := c.ncs[node].read(int(size) + 2)
			if err != nil {
				return results, errors.Wrap(err, "Failed read")
			}
			result.Value = b[:size]
			results = append(results, result)
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

func (cp *ConnectionPool) store(command string, item Item) (stored bool, err error) {
	if len(item.Value) > 1000000 {
		return false, ErrBadRequest
	}

	c, err := cp.conn(context.Background())
	if err != nil {
		return false, errors.Wrap(err, "Failed cp.conn")
	}
	defer func() {
		cp.putConn(c, err)
	}()

	c.reset()
	c.setDeadline()
	node, _ := c.hashRing.GetNode(item.Key)
	nc := c.ncs[node]
	// <command name> <key> <flags> <exptime> <bytes> noreply\r\n
	nc.writestrings(command, " ", item.Key, " ")
	nc.write(strconv.AppendUint(nil, uint64(item.Flags), 10))
	nc.writestring(" ")
	nc.write(strconv.AppendUint(nil, uint64(item.Exp), 10))
	nc.writestring(" ")
	nc.write(strconv.AppendInt(nil, int64(len(item.Value)), 10))
	if item.Cas != 0 {
		nc.writestring(" ")
		nc.write(strconv.AppendUint(nil, item.Cas, 10))
	}
	nc.writestring("\r\n")
	// <data block>\r\n
	nc.write(item.Value)
	nc.writestring("\r\n")
	reply, err := nc.readline()
	if err != nil {
		return false, errors.Wrap(err, "Failed readline")
	}
	stored = strings.HasPrefix(reply, "STORED")
	return
}
