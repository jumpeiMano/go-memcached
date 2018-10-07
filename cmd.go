package memcached

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Get returns cached data for given keys.
func (cp *ConnectionPool) Get(keys ...string) (results []Item, err error) {
	defer handleError(&err)
	results, err = cp.get("get", keys)
	return
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (cp *ConnectionPool) Gets(keys ...string) (results []Item, err error) {
	defer handleError(&err)
	results, err = cp.get("gets", keys)
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
	node, _ := c.hashRing.GetNode(key)
	nc := c.ncs[node]
	nc.writestrings("delete ", key, "\r\n")
	reply := nc.readline()
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
	for _, s := range cp.servers {
		nc := c.ncs[s.Aliase]
		nc.writestrings("flush_all\r\n")
		response := nc.readline()
		if !strings.Contains(response, "OK") {
			panic(NewError(fmt.Sprintf("Error in FlushAll %v", response)))
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
		header := c.ncs[node].readline()
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
			result.Value = c.ncs[node].read(int(size) + 2)[:size]
			results = append(results, result)
			header = c.ncs[node].readline()
		}
		if !strings.HasPrefix(header, "END") {
			panic(NewError("Malformed response: %s", string(header)))
		}
	}
	return results, nil
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

	c.reset()
	c.setDeadline()
	node, _ := c.hashRing.GetNode(key)
	nc := c.ncs[node]
	// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	nc.writestrings(command, " ", key, " ")
	nc.write(strconv.AppendUint(nil, uint64(flags), 10))
	nc.writestring(" ")
	nc.write(strconv.AppendUint(nil, timeout, 10))
	nc.writestring(" ")
	nc.write(strconv.AppendInt(nil, int64(len(value)), 10))
	if cas != 0 {
		nc.writestring(" ")
		nc.write(strconv.AppendUint(nil, cas, 10))
	}
	nc.writestring("\r\n")
	// <data block>\r\n
	nc.write(value)
	nc.writestring("\r\n")
	reply := nc.readline()
	if strings.Contains(reply, "ERROR") {
		panic(NewError("Server error"))
	}
	return strings.HasPrefix(reply, "STORED")
}
