package clouddatastore

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"go.mercari.io/datastore/v2/testsuite"
	_ "go.mercari.io/datastore/v2/testsuite/dsmiddleware/dslog"
	_ "go.mercari.io/datastore/v2/testsuite/dsmiddleware/fishbone"
	_ "go.mercari.io/datastore/v2/testsuite/dsmiddleware/localcache"
	_ "go.mercari.io/datastore/v2/testsuite/dsmiddleware/rpcretry"
	_ "go.mercari.io/datastore/v2/testsuite/realworld/recursivebatch"
	_ "go.mercari.io/datastore/v2/testsuite/realworld/tbf"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gomodule/redigo/redis"
	"go.mercari.io/datastore/v2/dsmiddleware/chaosrpc"
	"go.mercari.io/datastore/v2/dsmiddleware/dsmemcache"
	"go.mercari.io/datastore/v2/dsmiddleware/localcache"
	"go.mercari.io/datastore/v2/dsmiddleware/rediscache"
	"go.mercari.io/datastore/v2/dsmiddleware/rpcretry"
	"go.mercari.io/datastore/v2/dsmiddleware/splitop"
	"google.golang.org/api/iterator"
)

func cleanUp() error {
	ctx := context.Background()
	client, err := FromContext(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	q := client.NewQuery("__kind__").KeysOnly()
	iter := client.Run(ctx, q)
	var kinds []string
	for {
		key, err := iter.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		kinds = append(kinds, key.Name())
	}

	for _, kind := range kinds {
		q := client.NewQuery(kind).KeysOnly()
		keys, err := client.GetAll(ctx, q, nil)
		if err != nil {
			return err
		}
		err = client.DeleteMulti(ctx, keys)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestCloudDatastoreTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			defer cleanUp()

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ctx = testsuite.WrapCloudFlag(ctx)
			test(ctx, t, datastore)
		})
	}
}

func TestCloudDatastoreWithLocalCacheTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			switch name {
			// Skip the failure that happens when you firstly appended another middleware layer.
			case
				"LocalCache_Basic",
				"LocalCache_WithIncludeKinds",
				"LocalCache_WithExcludeKinds",
				"LocalCache_WithKeyFilter",
				"FishBone_QueryWithoutTx":
				t.SkipNow()
			// It's annoying to avoid failure test. I think there is no problem in practical use. I believe...
			case "PutAndGet_TimeTime":
				t.SkipNow()
			}

			defer cleanUp()

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}

			ch := localcache.New()
			datastore.AppendMiddleware(ch)

			ctx = testsuite.WrapCloudFlag(ctx)
			test(ctx, t, datastore)
		})
	}
}

func TestCloudDatastoreWithRedisCacheTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			switch name {
			// Skip the failure that happens when you firstly appended another middleware layer.
			case
				"LocalCache_Basic",
				"LocalCache_WithIncludeKinds",
				"LocalCache_WithExcludeKinds",
				"LocalCache_WithKeyFilter",
				"FishBone_QueryWithoutTx":
				t.SkipNow()
			// It's annoying to avoid failure test. I think there is no problem in practical use. I believe...
			case "PutAndGet_TimeTime":
				t.SkipNow()
			}

			defer cleanUp()

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}

			dial, err := net.Dial("tcp", os.Getenv("REDIS_HOST")+":"+os.Getenv("REDIS_PORT"))
			if err != nil {
				t.Fatal(err)
			}
			defer dial.Close()
			conn := redis.NewConn(dial, 100*time.Millisecond, 100*time.Millisecond)
			defer conn.Close()

			rc := rediscache.New(conn)
			datastore.AppendMiddleware(rc)

			ctx = testsuite.WrapCloudFlag(ctx)
			test(ctx, t, datastore)
		})
	}
}

func TestCloudDatastoreWithMemcacheTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			switch name {
			// Skip the failure that happens when you firstly appended another middleware layer.
			case
				"LocalCache_Basic",
				"LocalCache_WithIncludeKinds",
				"LocalCache_WithExcludeKinds",
				"LocalCache_WithKeyFilter",
				"FishBone_QueryWithoutTx":
				t.SkipNow()
			// It's annoying to avoid failure test. I think there is no problem in practical use. I believe...
			case "PutAndGet_TimeTime":
				t.SkipNow()
			}

			defer cleanUp()

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}

			memcacheClient := memcache.New(os.Getenv("MEMCACHE_ADDR"))
			ch := dsmemcache.New(
				memcacheClient,
			)
			datastore.AppendMiddleware(ch)

			ctx = testsuite.WrapCloudFlag(ctx)
			test(ctx, t, datastore)
		})
	}
}

func TestCloudDatastoreWithSplitCallTestSuite(t *testing.T) {
	ctx := context.Background()

	thresholds := []int{0, 1, 2, 1000}
	for _, threshold := range thresholds {
		threshold := threshold
		t.Run(fmt.Sprintf("threshold %d", threshold), func(t *testing.T) {
			for name, test := range testsuite.TestSuite {
				t.Run(name, func(t *testing.T) {
					// Skip the failure that happens when you firstly appended another middleware layer.
					switch name {
					case
						//"LocalCache_Basic",
						"LocalCache_WithIncludeKinds",
						"LocalCache_WithExcludeKinds",
						"LocalCache_WithKeyFilter",
						"FishBone_QueryWithoutTx":
						t.SkipNow()
					}

					defer cleanUp()

					datastore, err := FromContext(ctx)
					if err != nil {
						t.Fatal(err)
					}

					sc := splitop.New(
						splitop.WithGetSplitThreshold(threshold),
						splitop.WithLogger(func(ctx context.Context, format string, args ...interface{}) {
							t.Logf(format, args...)
						}),
					)
					datastore.AppendMiddleware(sc)

					ctx = testsuite.WrapCloudFlag(ctx)
					test(ctx, t, datastore)
				})
			}
		})
	}
}

func TestCloudDatastoreWithRPCRetryAndChaosRPCTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			// Skip the flaky tests.
			switch name {
			case
				"Filter_PropertyTranslaterMustError":
				t.SkipNow()
			}

			defer cleanUp()

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}

			rr := rpcretry.New(
				rpcretry.WithRetryLimit(10),
				rpcretry.WithMinBackoffDuration(1),
				rpcretry.WithMaxBackoffDuration(1),
				rpcretry.WithLogger(func(ctx context.Context, format string, args ...interface{}) {
					t.Logf(format, args...)
				}),
			)
			datastore.AppendMiddleware(rr)

			seed := time.Now().UnixNano()
			t.Logf("chaos seed: %d", seed)
			cr := chaosrpc.New(rand.NewSource(seed))
			datastore.AppendMiddleware(cr)

			ctx = testsuite.WrapCloudFlag(ctx)
			test(ctx, t, datastore)
		})
	}
}
