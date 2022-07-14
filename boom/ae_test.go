package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/aememcachev2"
	"go.mercari.io/datastore/internal/testutils"
	"google.golang.org/appengine/v2/memcache"
)

func TestBoom_AEMemcacheV2(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupAEDatastoreV2(t)
	defer cleanUp()

	cacheKey := func(key datastore.Key) string {
		return "mercari:aememcache:" + key.Encode()
	}
	ch := aememcachev2.New(
		aememcachev2.WithLogger(func(ctx context.Context, format string, args ...interface{}) {
			t.Logf(format, args...)
		}),
		aememcachev2.WithCacheKey(cacheKey),
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
