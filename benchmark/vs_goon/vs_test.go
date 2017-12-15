package vs_goon

import (
	"fmt"
	"testing"
	"time"
	"github.com/mjibson/goon"
	"github.com/favclip/testerator"
	"go.mercari.io/datastore/aedatastore"
	"go.mercari.io/datastore/dsmiddleware/aememcache"
	"go.mercari.io/datastore/boom"
	"go.mercari.io/datastore/dsmiddleware/localcache"
	"context"
)

func BenchmarkGoonFromAEMemcache(b *testing.B) {
	_, ctx, err := testerator.SpinUp()
	if err != nil {
		b.Fatal(err.Error())
	}
	defer testerator.SpinDown()

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
	_, ctx, err := testerator.SpinUp()
	if err != nil {
		b.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	client, err := aedatastore.FromContext(ctx)
	if err != nil {
		b.Fatal(err.Error())
	}

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
