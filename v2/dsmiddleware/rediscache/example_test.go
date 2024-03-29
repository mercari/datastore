package rediscache_test

import (
	"context"
	"net"
	"time"

	"github.com/gomodule/redigo/redis"
	"go.mercari.io/datastore/v2/clouddatastore"
	"go.mercari.io/datastore/v2/dsmiddleware/rediscache"
	"go.mercari.io/datastore/v2/internal/testutils"
)

const redisAddress = ":6379"

func Example_howToUse() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	defer testutils.CleanUpAllEntities(ctx, client)

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
