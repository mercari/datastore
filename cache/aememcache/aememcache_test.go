package aememcache

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/favclip/testerator"
	_ "github.com/favclip/testerator/datastore"
	_ "github.com/favclip/testerator/memcache"

	"github.com/MakeNowJust/heredoc"
	"github.com/golang/protobuf/proto"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/cache/dslog"
	memcachepb "go.mercari.io/datastore/internal/pb/memcache"
	"go.mercari.io/datastore/internal/testutils"
	netcontext "golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"
)

func TestMain(m *testing.M) {
	_, _, err := testerator.SpinUp()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}

	status := m.Run()

	err = testerator.SpinDown()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}

	os.Exit(status)
}

func TestAEMemcacheCache_Basic(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupAEDatastore(t)
	defer cleanUp()

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	// setup. strategies are first in - last apply.

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendCacheStrategy(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(aLog)
	}()

	ch := New()
	client.AppendCacheStrategy(ch)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(ch)
	}()

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendCacheStrategy(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(bLog)
	}()

	// exec.

	type Data struct {
		Name string
	}

	// Put. add to cache.
	key := client.IDKey("Data", 111, nil)
	objBefore := &Data{Name: "Data"}
	_, err := client.Put(ctx, key, objBefore)
	if err != nil {
		t.Fatal(err)
	}

	_, err = memcache.Get(ctx, ch.cacheKey(key))
	if err != nil {
		t.Fatal(err)
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

	_, err = memcache.Get(ctx, ch.cacheKey(key))
	if err != memcache.ErrCacheMiss {
		t.Fatal(err)
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, keys=[/Data,111]
		before: PutMultiWithoutTx #1, keys=[/Data,111]
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func TestAEMemcacheCache_Query(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupAEDatastore(t)
	defer cleanUp()

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	// setup. strategies are first in - last apply.

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendCacheStrategy(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(aLog)
	}()

	ch := New()
	client.AppendCacheStrategy(ch)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(ch)
	}()

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendCacheStrategy(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(bLog)
	}()

	// exec.

	type Data struct {
		Name string
	}

	const size = 3

	keys := make([]datastore.Key, size)
	list := make([]*Data, size)
	for i := 0; i < size; i++ {
		keys[i] = client.NameKey("Data", fmt.Sprintf("#%d", i+1), nil)
		list[i] = &Data{
			Name: fmt.Sprintf("#%d", i+1),
		}
	}
	_, err := client.PutMulti(ctx, keys, list)
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery("Data").Order("-Name")

	// Run
	iter := client.Run(ctx, q)

	// Next
	cnt := 0
	for {
		obj := &Data{}
		key, err := iter.Next(obj)
		if err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if v := obj.Name; v == "" || v != key.Name() {
			t.Errorf("unexpected: %v", cnt)
		}
		cnt++
	}
	if cnt != size {
		t.Errorf("unexpected: %v", cnt)
	}

	// GetAll
	list = nil
	_, err = client.GetAll(ctx, q, &list)
	if err != nil {
		t.Fatal(err)
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, keys=[/Data,#1, /Data,#2, /Data,#3]
		before: PutMultiWithoutTx #1, keys=[/Data,#1, /Data,#2, /Data,#3]
		before: Run #2, q=v1:Data&or=-Name
		after: Run #2, q=v1:Data&or=-Name
		before: Next #3, q=v1:Data&or=-Name
		after: Next #3, q=v1:Data&or=-Name
		after: Next #3, key=/Data,#3
		before: Next #3, key=/Data,#3
		before: Next #4, q=v1:Data&or=-Name
		after: Next #4, q=v1:Data&or=-Name
		after: Next #4, key=/Data,#2
		before: Next #4, key=/Data,#2
		before: Next #5, q=v1:Data&or=-Name
		after: Next #5, q=v1:Data&or=-Name
		after: Next #5, key=/Data,#1
		before: Next #5, key=/Data,#1
		before: Next #6, q=v1:Data&or=-Name
		after: Next #6, q=v1:Data&or=-Name
		after: Next #6, err=no more items in iterator
		before: Next #6, err=no more items in iterator
		before: GetAll #7, q=v1:Data&or=-Name
		after: GetAll #7, q=v1:Data&or=-Name
		after: GetAll #7, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
		before: GetAll #7, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func TestAEMemcacheCache_Transaction(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupAEDatastore(t)
	defer cleanUp()

	var rpcLogs []string
	rpcLogf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		rpcLogs = append(rpcLogs, fmt.Sprintf(format, args...))
	}

	ctx = appengine.WithAPICallFunc(ctx, func(ctx netcontext.Context, service, method string, in, out proto.Message) error {
		origErr := appengine.APICall(ctx, service, method, in, out)

		switch service {
		case "memcache":
			switch method {
			case "Set":
				{
					b, err := proto.Marshal(in)
					if err != nil {
						return err
					}
					req := &memcachepb.MemcacheSetRequest{}
					err = proto.Unmarshal(b, req)
					if err != nil {
						t.Fatal(err)
					}
					rpcLogf(ctx, "memcache.Set: len=%d", len(req.GetItem()))
					for _, item := range req.GetItem() {
						keyStr := string(item.GetKey())
						keyStr = strings.TrimPrefix(keyStr, "mercari:aememcache:")
						key, err := client.DecodeKey(keyStr)
						if err != nil {
							t.Fatal(err)
						}
						rpcLogf(ctx, "memcache.Set: req=%s", key.String())
					}

					b, err = proto.Marshal(out)
					if err != nil {
						return err
					}
					resp := &memcachepb.MemcacheSetResponse{}
					err = proto.Unmarshal(b, resp)
					if err != nil {
						t.Fatal(err)
					}
					rpcLogf(ctx, "memcache.Set: resp len=%d", len(resp.GetSetStatus()))
					for _, status := range resp.GetSetStatus() {
						rpcLogf(ctx, "memcache.Set: resp=%s", status.String())
					}
				}

			case "Get":
				{
					b, err := proto.Marshal(in)
					if err != nil {
						return err
					}
					req := &memcachepb.MemcacheGetRequest{}
					err = proto.Unmarshal(b, req)
					if err != nil {
						t.Fatal(err)
					}
					rpcLogf(ctx, "memcache.Get: req len=%d", len(req.GetKey()))
					for _, key := range req.GetKey() {
						keyStr := string(key)
						keyStr = strings.TrimPrefix(keyStr, "mercari:aememcache:")
						key, err := client.DecodeKey(keyStr)
						if err != nil {
							t.Fatal(err)
						}
						rpcLogf(ctx, "memcache.Get: req=%s", key.String())
					}

					b, err = proto.Marshal(out)
					if err != nil {
						return err
					}
					resp := &memcachepb.MemcacheGetResponse{}
					err = proto.Unmarshal(b, resp)
					if err != nil {
						t.Fatal(err)
					}
					rpcLogf(ctx, "memcache.Get: resp len=%d", len(resp.GetItem()))
					for _, item := range resp.GetItem() {
						keyStr := string(item.Key)
						keyStr = strings.TrimPrefix(keyStr, "mercari:aememcache:")
						key, err := client.DecodeKey(keyStr)
						if err != nil {
							t.Fatal(err)
						}
						rpcLogf(ctx, "memcache.Get: resp=%s", key.String())
					}
				}

			case "Delete":
				{
					b, err := proto.Marshal(in)
					if err != nil {
						return err
					}
					req := &memcachepb.MemcacheDeleteRequest{}
					err = proto.Unmarshal(b, req)
					if err != nil {
						t.Fatal(err)
					}
					rpcLogf(ctx, "memcache.Delete: req len=%d", len(req.GetItem()))
					for _, item := range req.GetItem() {
						keyStr := string(item.GetKey())
						keyStr = strings.TrimPrefix(keyStr, "mercari:aememcache:")
						key, err := client.DecodeKey(keyStr)
						if err != nil {
							t.Fatal(err)
						}
						rpcLogf(ctx, "memcache.Delete: req=%s", key.String())
					}

					b, err = proto.Marshal(out)
					if err != nil {
						return err
					}
					resp := &memcachepb.MemcacheDeleteResponse{}
					err = proto.Unmarshal(b, resp)
					if err != nil {
						t.Fatal(err)
					}
					rpcLogf(ctx, "memcache.Delete: resp len=%d", len(resp.GetDeleteStatus()))
					for _, status := range resp.GetDeleteStatus() {
						rpcLogf(ctx, "memcache.Delete: resp=%s", status.String())
					}
				}

			}
		}

		return origErr
	})
	client.SwapContext(ctx)

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	// setup. strategies are first in - last apply.

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendCacheStrategy(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(aLog)
	}()

	ch := New()
	ch.raiseMemcacheError = true
	client.AppendCacheStrategy(ch)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(ch)
	}()

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendCacheStrategy(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(bLog)
	}()

	// exec.

	stats, err := memcache.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v := stats.Items; v != 0 {
		t.Fatalf("unexpected: %v", v)
	}

	type Data struct {
		Name string
	}

	key := client.NameKey("Data", "a", nil)

	// put to cache
	_, err = client.Put(ctx, key, &Data{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = memcache.Get(ctx, fmt.Sprintf("mercari:aememcache:%s", key.Encode()))
	if err != nil {
		t.Fatal(err)
	}

	{ // Rollback
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// don't put to cache before commit
		key2 := client.NameKey("Data", "b", nil)
		_, err = tx.Put(key2, &Data{Name: "After"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = memcache.Get(ctx, fmt.Sprintf("mercari:aememcache:%s", key2.Encode()))
		if err != memcache.ErrCacheMiss {
			t.Fatal(err)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		// don't delete from cache before commit
		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}
		_, err = memcache.Get(ctx, fmt.Sprintf("mercari:aememcache:%s", key.Encode()))
		if err != nil {
			t.Fatal(err)
		}

		// rollback.
		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
		stats, err := memcache.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if v := stats.Items; v != 1 {
			t.Fatalf("unexpected: %v", v)
		}
	}

	{ // Commit
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// don't put to cache before commit
		key2 := client.IncompleteKey("Data", nil)
		pKey, err := tx.Put(key2, &Data{Name: "After"})
		if err != nil {
			t.Fatal(err)
		}
		stats, err := memcache.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if v := stats.Items; v != 1 {
			t.Fatalf("unexpected: %v", v)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		// don't delete from cache before commit
		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}
		_, err = memcache.Get(ctx, fmt.Sprintf("mercari:aememcache:%s", key.Encode()))
		if err != nil {
			t.Fatal(err)
		}

		// commit.
		commit, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}

		key3 := commit.Key(pKey)
		if v := key3.Name(); v != key2.Name() {
			t.Errorf("unexpected: %v", v)
		}
		// commited, but don't put to cache in tx.
		_, err = memcache.Get(ctx, fmt.Sprintf("mercari:aememcache:%s", key3.Encode()))
		if err != memcache.ErrCacheMiss {
			t.Fatal(err)
		}

		stats, err = memcache.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if v := stats.Items; v != 0 {
			t.Fatalf("unexpected: %v", v)
		}
	}

	var expected *regexp.Regexp
	{
		expectedPattern := heredoc.Doc(`
			before: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,a]
			after: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,a]
			after: PutMultiWithoutTx #1, keys=[/Data,a]
			before: PutMultiWithoutTx #1, keys=[/Data,a]
			before: PutMultiWithTx #2, len(keys)=1, keys=[/Data,b]
			after: PutMultiWithTx #2, len(keys)=1, keys=[/Data,b]
			before: GetMultiWithTx #3, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #3, len(keys)=1, keys=[/Data,a]
			before: DeleteMultiWithTx #4, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #4, len(keys)=1, keys=[/Data,a]
			before: PostRollback #5
			after: PostRollback #5
			before: PutMultiWithTx #6, len(keys)=1, keys=[/Data,0]
			after: PutMultiWithTx #6, len(keys)=1, keys=[/Data,0]
			before: GetMultiWithTx #7, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #7, len(keys)=1, keys=[/Data,a]
			before: DeleteMultiWithTx #8, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #8, len(keys)=1, keys=[/Data,a]
			before: PostCommit #9 Put keys=[/Data,@####@]
			after: PostCommit #9 Put keys=[/Data,@####@]
		`)
		ss := strings.Split(expectedPattern, "@####@")
		var buf bytes.Buffer
		for idx, s := range ss {
			buf.WriteString(regexp.QuoteMeta(s))
			if idx != (len(ss) - 1) {
				buf.WriteString("[0-9]+")
			}
		}
		expected = regexp.MustCompile(buf.String())
	}
	if v := strings.Join(logs, "\n") + "\n"; !expected.MatchString(v) {
		t.Errorf("unexpected: %v", v)
	}

	{
		expectedPattern := heredoc.Doc(`
			memcache.Set: len=1
			memcache.Set: req=/Data,a
			memcache.Set: resp len=1
			memcache.Set: resp=STORED
			memcache.Get: req len=1
			memcache.Get: req=/Data,a
			memcache.Get: resp len=1
			memcache.Get: resp=/Data,a
			memcache.Get: req len=1
			memcache.Get: req=/Data,b
			memcache.Get: resp len=0
			memcache.Get: req len=1
			memcache.Get: req=/Data,a
			memcache.Get: resp len=1
			memcache.Get: resp=/Data,a
			memcache.Get: req len=1
			memcache.Get: req=/Data,a
			memcache.Get: resp len=1
			memcache.Get: resp=/Data,a
			memcache.Delete: req len=3
			memcache.Delete: req=/Data,@####@
			memcache.Delete: req=/Data,a
			memcache.Delete: req=/Data,a
			memcache.Delete: resp len=3
			memcache.Delete: resp=NOT_FOUND
			memcache.Delete: resp=DELETED
			memcache.Delete: resp=NOT_FOUND
			memcache.Get: req len=1
			memcache.Get: req=/Data,@####@
			memcache.Get: resp len=0
		`)
		ss := strings.Split(expectedPattern, "@####@")
		var buf bytes.Buffer
		for idx, s := range ss {
			buf.WriteString(regexp.QuoteMeta(s))
			if idx != (len(ss) - 1) {
				buf.WriteString("[0-9]+")
			}
		}
		expected = regexp.MustCompile(buf.String())
	}
	if v := strings.Join(rpcLogs, "\n") + "\n"; !expected.MatchString(v) {
		t.Errorf("unexpected: %v", v)
	}
}

func TestAEMemcacheCache_MultiError(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupAEDatastore(t)
	defer cleanUp()

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}

	// setup. strategies are first in - last apply.

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendCacheStrategy(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(aLog)
	}()

	ch := New()
	ch.Logf = logf
	client.AppendCacheStrategy(ch)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(ch)
	}()

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendCacheStrategy(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(bLog)
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
			err := memcache.Delete(ctx, ch.cacheKey(key))
			if err != nil {
				t.Fatal(err)
			}
		}
		if key.ID()%3 == 0 {
			// Delete entity where out of aememcache scope
			// delete entity id=3, 6, 9
			client.RemoveCacheStrategy(ch)
			err := client.Delete(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			client.AppendCacheStrategy(ch)
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
			// not exists on memcache & datastore both
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
		cache/aememcache.SetMulti: incoming len=10
		cache/aememcache.SetMulti: len=10
		before: PutMultiWithoutTx #1, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		before: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,3]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,3]
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,6]
		after: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,6]
		before: DeleteMultiWithoutTx #4, len(keys)=1, keys=[/Data,9]
		after: DeleteMultiWithoutTx #4, len(keys)=1, keys=[/Data,9]
		cache/aememcache.GetMulti: incoming len=10
		cache/aememcache.GetMulti: got len=5
		before: GetMultiWithoutTx #5, len(keys)=5, keys=[/Data,2, /Data,4, /Data,6, /Data,8, /Data,10]
		after: GetMultiWithoutTx #5, len(keys)=5, keys=[/Data,2, /Data,4, /Data,6, /Data,8, /Data,10]
		after: GetMultiWithoutTx #5, err=datastore: no such entity
		before: GetMultiWithoutTx #5, err=datastore: no such entity
		cache/aememcache.SetMulti: incoming len=4
		cache/aememcache.SetMulti: len=4
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}
