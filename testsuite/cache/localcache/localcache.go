package localcache

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/MakeNowJust/heredoc"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/cache/dslog"
	"go.mercari.io/datastore/cache/localcache"
	"go.mercari.io/datastore/testsuite"
	"google.golang.org/api/iterator"
)

var TestSuite = map[string]testsuite.Test{
	"LocalCache_Basic":       LocalCache_Basic,
	"LocalCache_Query":       LocalCache_Query,
	"LocalCache_Transaction": LocalCache_Transaction,
}

func init() {
	testsuite.MergeTestSuite(TestSuite)
}

func LocalCache_Basic(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

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

	ch := localcache.New()
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

	if v := ch.Has(key); !v {
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

	if v := ch.Has(key); v {
		t.Fatalf("unexpected: %v", v)
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

func LocalCache_Query(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

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

	ch := localcache.New()
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

func LocalCache_Transaction(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

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

	ch := localcache.New()
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

	key := client.NameKey("Data", "a", nil)

	// put to cache
	_, err := client.Put(ctx, key, &Data{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	if v := ch.Has(key); !v {
		t.Fatalf("unexpected: %v", v)
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
		if v := ch.Has(key2); v {
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
		if v := ch.Has(key); !v {
			t.Fatalf("unexpected: %v", v)
		}

		// rollback.
		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
		if v := ch.Len(); v != 1 {
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
		if v := ch.Len(); v != 1 {
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
		if v := ch.Has(key); !v {
			t.Fatalf("unexpected: %v", v)
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
		if v := ch.Has(key3); v {
			t.Fatalf("unexpected: %v", v)
		}

		if v := ch.Len(); v != 0 {
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
}
