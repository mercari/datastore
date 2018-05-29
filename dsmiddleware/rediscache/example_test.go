package rediscache_test

import (
	"context"
	"net"
	"time"

	"github.com/garyburd/redigo/redis"
	"go.mercari.io/datastore/clouddatastore"
	"go.mercari.io/datastore/dsmiddleware/rediscache"
)

const redisAddress = ":6379"

func Example_howToUse() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	dial, err := net.Dial("tcp", redisAddress)
	if err != nil {
		panic(err)
	}
	defer dial.Close()
	conn := redis.NewConn(dial, 100*time.Millisecond, 100*time.Millisecond)
	defer conn.Close()

	mw := rediscache.New(conn)
	client.AppendMiddleware(mw)
}
