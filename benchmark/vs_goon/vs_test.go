package vs_goon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mjibson/goon"
	"go.mercari.io/datastore/boom"
	"go.mercari.io/datastore/dsmiddleware/aememcache"
	"go.mercari.io/datastore/dsmiddleware/localcache"
	"go.mercari.io/datastore/internal/testutils"
)

func BenchmarkGoonFromAEMemcache(b *testing.B) {
	ctx, _, cleanUp := testutils.SetupAEDatastore(b)
	defer cleanUp()

	type Data struct {
		ID        int64 `datastore:"-" goon:"id"`
		Name      string
		N1        int
		N2        int
		N3        int
		N4        int
		N5        int
		N6        int
		N7        int
		N8        int
		N9        int
		N10       int
		CreatedAt time.Time
	}

	const size = 300

	g := goon.FromContext(ctx)

	// データの用意
	list := make([]*Data, 0, size)
	for i := 0; i < size; i++ {
		list = append(list, &Data{
			Name:      fmt.Sprintf("#%d", i+1),
			CreatedAt: time.Now(),
		})
	}
	keys, err := g.PutMulti(list)
	if err != nil {
		b.Fatal(err)
	}

	// Memcacheに載せる
	list = make([]*Data, 0, size)
	for idx := range list {
		list = append(list, &Data{
			ID: keys[idx].IntID(),
		})
	}
	err = g.GetMulti(list)
	if err != nil {
		b.Fatal(err.Error())
	}

	b.ResetTimer()

	// MemcacheからGetする時のベンチマーク
	for i := 0; i < b.N; i++ {
		g.FlushLocalCache()
		list = make([]*Data, 0, size)
		for i := 0; i < size; i++ {
			list = append(list, &Data{
				ID: keys[i].IntID(),
			})
		}
		err = g.GetMulti(list)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkBoomFromAEMemcache(b *testing.B) {
	ctx, client, cleanUp := testutils.SetupAEDatastore(b)
	defer cleanUp()

	lc := localcache.New()
	lc.Logf = func(ctx context.Context, format string, args ...interface{}) {}
	client.AppendMiddleware(lc)

	am := aememcache.New()
	am.Logf = func(ctx context.Context, format string, args ...interface{}) {}
	client.AppendMiddleware(am)

	type Data struct {
		ID        int64 `datastore:"-" goon:"id"`
		Name      string
		N1        int
		N2        int
		N3        int
		N4        int
		N5        int
		N6        int
		N7        int
		N8        int
		N9        int
		N10       int
		CreatedAt time.Time
	}

	const size = 300

	bm := boom.FromClient(ctx, client)

	// データの用意
	list := make([]*Data, 0, size)
	for i := 0; i < size; i++ {
		list = append(list, &Data{
			Name:      fmt.Sprintf("#%d", i+1),
			CreatedAt: time.Now(),
		})
	}
	keys, err := bm.PutMulti(list)
	if err != nil {
		b.Fatal(err)
	}

	// Memcacheに載せる
	list = make([]*Data, 0, size)
	for idx := range list {
		list = append(list, &Data{
			ID: keys[idx].ID(),
		})
	}
	err = bm.GetMulti(list)
	if err != nil {
		b.Fatal(err.Error())
	}

	b.ResetTimer()

	// MemcacheからGetする時のベンチマーク
	for i := 0; i < b.N; i++ {
		lc.FlushLocalCache()
		list = make([]*Data, 0, size)
		for i := 0; i < size; i++ {
			list = append(list, &Data{
				ID: keys[i].ID(),
			})
		}
		err = bm.GetMulti(list)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkGoonFromLocalCache(b *testing.B) {
	ctx, _, cleanUp := testutils.SetupAEDatastore(b)
	defer cleanUp()

	type Data struct {
		ID        int64 `datastore:"-" goon:"id"`
		Name      string
		N1        int
		N2        int
		N3        int
		N4        int
		N5        int
		N6        int
		N7        int
		N8        int
		N9        int
		N10       int
		CreatedAt time.Time
	}

	const size = 300

	g := goon.FromContext(ctx)

	// データの用意
	list := make([]*Data, 0, size)
	for i := 0; i < size; i++ {
		list = append(list, &Data{
			Name:      fmt.Sprintf("#%d", i+1),
			CreatedAt: time.Now(),
		})
	}
	keys, err := g.PutMulti(list)
	if err != nil {
		b.Fatal(err)
	}

	// LocalCacheに載せる
	list = make([]*Data, 0, size)
	for idx := range list {
		list = append(list, &Data{
			ID: keys[idx].IntID(),
		})
	}
	err = g.GetMulti(list)
	if err != nil {
		b.Fatal(err.Error())
	}

	b.ResetTimer()

	// LocalCacheからGetする時のベンチマーク
	for i := 0; i < b.N; i++ {
		list = make([]*Data, 0, size)
		for i := 0; i < size; i++ {
			list = append(list, &Data{
				ID: keys[i].IntID(),
			})
		}
		err = g.GetMulti(list)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}
func BenchmarkBoomFromLocalCache(b *testing.B) {
	ctx, client, cleanUp := testutils.SetupAEDatastore(b)
	defer cleanUp()

	lc := localcache.New()
	lc.Logf = func(ctx context.Context, format string, args ...interface{}) {}
	client.AppendMiddleware(lc)

	type Data struct {
		ID        int64 `datastore:"-" goon:"id"`
		Name      string
		N1        int
		N2        int
		N3        int
		N4        int
		N5        int
		N6        int
		N7        int
		N8        int
		N9        int
		N10       int
		CreatedAt time.Time
	}

	const size = 300

	bm := boom.FromClient(ctx, client)

	// データの用意
	list := make([]*Data, 0, size)
	for i := 0; i < size; i++ {
		list = append(list, &Data{
			Name:      fmt.Sprintf("#%d", i+1),
			CreatedAt: time.Now(),
		})
	}
	keys, err := bm.PutMulti(list)
	if err != nil {
		b.Fatal(err)
	}

	// LocalCacheに載せる
	list = make([]*Data, 0, size)
	for idx := range list {
		list = append(list, &Data{
			ID: keys[idx].ID(),
		})
	}
	err = bm.GetMulti(list)
	if err != nil {
		b.Fatal(err.Error())
	}

	b.ResetTimer()

	// LocalCacheからGetする時のベンチマーク
	for i := 0; i < b.N; i++ {
		list = make([]*Data, 0, size)
		for i := 0; i < size; i++ {
			list = append(list, &Data{
				ID: keys[i].ID(),
			})
		}
		err = bm.GetMulti(list)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}
