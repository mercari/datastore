package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/aememcache"
	"go.mercari.io/datastore/internal/testutils"
	"google.golang.org/appengine/memcache"
)

func TestBoom_AEMemcache(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupAEDatastore(t)
	defer cleanUp()

	cacheKey := func(key datastore.Key) string {
		return "mercari:aememcache:" + key.Encode()
	}
	ch := aememcache.New(
		aememcache.WithLogger(func(ctx context.Context, format string, args ...interface{}) {
			t.Logf(format, args...)
		}),
		aememcache.WithCacheKey(cacheKey),
	)
	client.AppendMiddleware(ch)

	type Data struct {
		ID   int64 `boom:"id"`
		Name string
	}

	bm := FromClient(ctx, client)

	{ // with complete key
		obj := &Data{
			ID:   100,
			Name: "foo",
		}
		key := bm.Key(obj)

		memcacheKey := cacheKey(key)

		_, err := memcache.Get(ctx, memcacheKey)
		if err != memcache.ErrCacheMiss {
			t.Fatal(err)
		}

		_, err = bm.Put(obj)
		if err != nil {
			t.Fatal(err)
		}

		_, err = memcache.Get(ctx, memcacheKey)
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // with incomplete key
		obj := &Data{
			Name: "foo",
		}
		key := bm.Key(obj)
		if v := key.Incomplete(); !v {
			t.Errorf("unexpected: %v", v)
		}

		key, err := bm.Put(obj)
		if err != nil {
			t.Fatal(err)
		}

		_, err = memcache.Get(ctx, cacheKey(key))
		if err != nil {
			t.Fatal(err)
		}
	}
}
