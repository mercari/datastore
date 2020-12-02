package dsmemcache

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/bradfitz/gomemcache/memcache"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/dslog"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
	"go.mercari.io/datastore/internal/testutils"
)

func inCache(ctx context.Context, ch storagecache.Storage, key datastore.Key) (bool, error) {
	resp, err := ch.GetMulti(ctx, []datastore.Key{key})
	if err != nil {
		return false, err
	} else if v := len(resp); v != 1 {
		return false, nil
	} else if v := resp[0]; v == nil {
		return false, nil
	}

	return true, nil
}

func TestMemcache_Basic(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	memcacheClient := memcache.New(os.Getenv("MEMCACHE_ADDR"))
	ch := New(
		memcacheClient,
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		if err := memcacheClient.FlushAll(); err != nil {
			t.Fatal(err)
		}
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(ch)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	// exec.

	type Data struct {
		Name string
	}

	// Put. add to cache.
	key := client.IDKey("Data", 111, nil)
	objBefore := &Data{Name: "Data"}
	if _, err := client.Put(ctx, key, objBefore); err != nil {
		t.Fatal(err)
	}

	hit, err := inCache(ctx, ch, key)
	if err != nil {
		t.Fatal(err)
	} else if v := hit; !v {
		t.Fatalf("unexpected: %v", v)
	}

	// Get. from cache.
	objAfter := &Data{}
	err = client.Get(ctx, key, objAfter)
	if err != nil {
		t.Fatal(err)
	}

	// Delete.
	err = client.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	hit, err = inCache(ctx, ch, key)
	if err != nil {
		t.Fatal(err)
	} else if v := hit; v {
		t.Fatalf("unexpected: %v", v)
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, keys=[/Data,111]
		dsmiddleware/dsmemcache.SetMulti: incoming len=1
		before: PutMultiWithoutTx #1, keys=[/Data,111]
		dsmiddleware/dsmemcache.GetMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: hit=1 miss=0
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/dsmemcache.GetMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: hit=1 miss=0
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/dsmemcache.DeleteMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: hit=0 miss=1
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func TestMemcache_BasicWithoutExpire(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	memcacheClient := memcache.New(os.Getenv("MEMCACHE_ADDR"))
	ch := New(
		memcacheClient,
		WithExpireDuration(0),
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		if err := memcacheClient.FlushAll(); err != nil {
			t.Fatal(err)
		}
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(ch)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	// exec.

	type Data struct {
		Name string
	}

	// Put. add to cache.
	key := client.IDKey("Data", 111, nil)
	objBefore := &Data{Name: "Data"}
	if _, err := client.Put(ctx, key, objBefore); err != nil {
		t.Fatal(err)
	}

	hit, err := inCache(ctx, ch, key)
	if err != nil {
		t.Fatal(err)
	} else if v := hit; !v {
		t.Fatalf("unexpected: %v", v)
	}

	// Get. from cache.
	objAfter := &Data{}
	err = client.Get(ctx, key, objAfter)
	if err != nil {
		t.Fatal(err)
	}

	// Delete.
	err = client.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	hit, err = inCache(ctx, ch, key)
	if err != nil {
		t.Fatal(err)
	} else if v := hit; v {
		t.Fatalf("unexpected: %v", v)
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, keys=[/Data,111]
		dsmiddleware/dsmemcache.SetMulti: incoming len=1
		before: PutMultiWithoutTx #1, keys=[/Data,111]
		dsmiddleware/dsmemcache.GetMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: hit=1 miss=0
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/dsmemcache.GetMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: hit=1 miss=0
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/dsmemcache.DeleteMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: incoming len=1
		dsmiddleware/dsmemcache.GetMulti: hit=0 miss=1
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func TestMemcache_MultiError(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	// setup. strategies are first in - first apply.
	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	memcacheClient := memcache.New(os.Getenv("MEMCACHE_ADDR"))
	ch := New(
		memcacheClient,
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		if err := memcacheClient.FlushAll(); err != nil {
			t.Fatal(err)
		}
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(ch)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	// exec.

	type Data struct {
		Name string
	}

	const size = 10

	keys := make([]datastore.Key, 0, size)
	list := make([]*Data, 0, size)
	for i := 1; i <= size; i++ {
		keys = append(keys, client.IDKey("Data", int64(i), nil))
		list = append(list, &Data{
			Name: fmt.Sprintf("#%d", i),
		})
	}

	_, err := client.PutMulti(ctx, keys, list)
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		if key.ID()%2 == 0 {
			// delete cache id=2, 4, 6, 8, 10
			err := ch.DeleteMulti(ctx, []datastore.Key{key})
			if err != nil {
				t.Fatal(err)
			}
		}
		if key.ID()%3 == 0 {
			client.RemoveMiddleware(ch)
			err := client.Delete(ctx, key)
			if err != nil {
				t.Fatal(err)
			}

			client.RemoveMiddleware(aLog)
			client.AppendMiddleware(ch)
			client.AppendMiddleware(aLog)
		}
	}

	list = make([]*Data, size)
	err = client.GetMulti(ctx, keys, list)
	merr, ok := err.(datastore.MultiError)
	if !ok {
		t.Fatal(err)
	}

	if v := len(merr); v != size {
		t.Fatalf("unexpected: %v", v)
	}
	for idx, err := range merr {
		key := keys[idx]
		if key.ID()%2 == 0 && key.ID()%3 == 0 {
			// not exists on cache & datastore both
			if err != datastore.ErrNoSuchEntity {
				t.Error(err)
			}
		} else {
			if v := list[idx].Name; v != fmt.Sprintf("#%d", idx+1) {
				t.Errorf("unexpected: %v", v)
			}
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=10, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		after: PutMultiWithoutTx #1, len(keys)=10, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		after: PutMultiWithoutTx #1, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		dsmiddleware/dsmemcache.SetMulti: incoming len=10
		before: PutMultiWithoutTx #1, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		dsmiddleware/dsmemcache.DeleteMulti: incoming len=1
		before: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,3]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,3]
		dsmiddleware/dsmemcache.DeleteMulti: incoming len=1
		dsmiddleware/dsmemcache.DeleteMulti: incoming len=1
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,6]
		after: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,6]
		dsmiddleware/dsmemcache.DeleteMulti: incoming len=1
		before: DeleteMultiWithoutTx #4, len(keys)=1, keys=[/Data,9]
		after: DeleteMultiWithoutTx #4, len(keys)=1, keys=[/Data,9]
		dsmiddleware/dsmemcache.DeleteMulti: incoming len=1
		before: GetMultiWithoutTx #5, len(keys)=10, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		dsmiddleware/dsmemcache.GetMulti: incoming len=10
		dsmiddleware/dsmemcache.GetMulti: hit=5 miss=5
		after: GetMultiWithoutTx #5, len(keys)=5, keys=[/Data,2, /Data,4, /Data,6, /Data,8, /Data,10]
		after: GetMultiWithoutTx #5, err=datastore: no such entity
		dsmiddleware/dsmemcache.SetMulti: incoming len=4
		before: GetMultiWithoutTx #5, err=datastore: no such entity
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}
