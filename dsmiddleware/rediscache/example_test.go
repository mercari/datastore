package rediscache_test

import (
	"context"
	"github.com/go-redis/redis/v7"
	"os"

	"go.mercari.io/datastore/clouddatastore"
	"go.mercari.io/datastore/dsmiddleware/rediscache"
	"go.mercari.io/datastore/internal/testutils"
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

	redisClient := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
		DB:   0,
	})

	mw := rediscache.New(redisClient)
	client.AppendMiddleware(mw)
}
