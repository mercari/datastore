package clouddatastore

import (
	"context"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"go.mercari.io/datastore/testsuite"
	_ "go.mercari.io/datastore/testsuite/dsmiddleware/dslog"
	_ "go.mercari.io/datastore/testsuite/dsmiddleware/fishbone"
	_ "go.mercari.io/datastore/testsuite/dsmiddleware/localcache"
	_ "go.mercari.io/datastore/testsuite/dsmiddleware/rpcretry"
	_ "go.mercari.io/datastore/testsuite/favcliptools"
	_ "go.mercari.io/datastore/testsuite/realworld/recursive_batch"
	_ "go.mercari.io/datastore/testsuite/realworld/tbf"

	"github.com/garyburd/redigo/redis"
	"go.mercari.io/datastore/dsmiddleware/chaosrpc"
	"go.mercari.io/datastore/dsmiddleware/localcache"
	"go.mercari.io/datastore/dsmiddleware/rediscache"
	"go.mercari.io/datastore/dsmiddleware/rpcretry"
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
			test(t, ctx, datastore)
		})
	}
}

func TestCloudDatastoreWithLocalCacheTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			// Skip the failure that happens when you firstly appended another middleware layer.
			switch name {
			case
				"LocalCache_Basic",
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

			ch := localcache.New()
			datastore.AppendMiddleware(ch)

			ctx = testsuite.WrapCloudFlag(ctx)
			test(t, ctx, datastore)
		})
	}
}

func TestCloudDatastoreWithRedisCacheTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			// Skip the failure that happens when you firstly appended another middleware layer.
			switch name {
			case
				"LocalCache_Basic",
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
			test(t, ctx, datastore)
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
				rpcretry.WithLogf(func(ctx context.Context, format string, args ...interface{}) {
					t.Logf(format, args...)
				}),
			)
			datastore.AppendMiddleware(rr)

			seed := time.Now().UnixNano()
			t.Logf("chaos seed: %d", seed)
			cr := chaosrpc.New(rand.NewSource(seed))
			datastore.AppendMiddleware(cr)

			ctx = testsuite.WrapCloudFlag(ctx)
			test(t, ctx, datastore)
		})
	}
}
