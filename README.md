# go-memcached

## What's this?
go-memcached is the client library of memcached for Go.

## Install
```
go get github.com/jumpeiMano/go-memcached
```

## Example
```GO
package main

import (
  gm "github.com/jumpeiMano/go-memcached"
)

func main() {
  cp := gm.New(
    gm.Server{
      {Host: "localhost", Port: 11211, Alias: "mem1"},
    }, "prefix",
  )
  defer cp.Close()
  items, err := cp.Get("key1", "key2")
  if err != nil {
    log.Fatalf("Failed Get: %+v", err)
  }
  keyValueMap := make(map[string]string, len(items))
  for _, item := range items {
    keyValueMap[item.Key] = string(item.Value)
  }
  fmt.Println(keyValueMap)
}
```

## Configure
### SetConnMaxOpen
SetConnMaxOpen sets the maximum amount of opening connections. Default is 0 (unlimited).

### SetConnectTimeout
SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
Expired connections may be closed lazily before reuse. If d <= 0, connections are reused forever.
Default is 0.

### SetConnectTimeout
SetConnectTimeout sets the timeout of connect to memcached server. Default is 1 second.

### SetPollTimeout
SetPollTimeout sets the timeout of polling from memcached server. Default is 1 second.

### SetFailover
SetFailover is used to specify whether to use the failover option. Default is false.

### SetAliveCheckPeriod
SetAliveCheckPeriod sets the period of connection's alive check to memcached server. Default is 10 seconds.
It will work only when the `failover` is true.

### SetNoreply
SetNoreply is used to specify whether to use the noreply option. Default is false.

## Testing
```
$ docker-compose up -d
$ go test -race ./...
```
