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
	"go.mercari.io/datastore/dsmiddleware/dslog"
	"go.mercari.io/datastore/dsmiddleware/localcache"
	"go.mercari.io/datastore/testsuite"
	"google.golang.org/api/iterator"
)

var TestSuite = map[string]testsuite.Test{
	"LocalCache_Basic":            LocalCache_Basic,
	"LocalCache_WithIncludeKinds": LocalCache_WithIncludeKinds,
	"LocalCache_WithExcludeKinds": LocalCache_WithExcludeKinds,
	"LocalCache_WithKeyFilter":    LocalCache_WithKeyFilter,
	"LocalCache_FlushLocalCache":  LocalCache_FlushLocalCache,
	"LocalCache_Query":            LocalCache_Query,
	"LocalCache_Transaction":      LocalCache_Transaction,
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

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	ch := localcache.New()
	client.AppendMiddleware(ch)
	defer func() {
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

	// Put. add to dsmiddleware.
	key := client.IDKey("Data", 111, nil)
	objBefore := &Data{Name: "Data"}
	_, err := client.Put(ctx, key, objBefore)
	if err != nil {
		t.Fatal(err)
	}

	if v := ch.HasCache(key); !v {
		t.Fatalf("unexpected: %v", v)
	}

	// Get. from dsmiddleware.
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

	if v := ch.HasCache(key); v {
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

func LocalCache_WithIncludeKinds(t *testing.T, ctx context.Context, client datastore.Client) {
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

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	ch := localcache.New(
		localcache.WithIncludeKinds("DataA"),
		localcache.WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
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

	{ // Put. dsmiddleware target.
		key := client.IDKey("DataA", 111, nil)
		objBefore := &Data{Name: "A"}
		_, err := client.Put(ctx, key, objBefore)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = client.Get(ctx, key, obj)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Name; v != "A" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.Delete(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. dsmiddleware ignored.
		key := client.IDKey("DataB", 111, nil)
		objBefore := &Data{Name: "B"}
		_, err := client.Put(ctx, key, objBefore)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = client.Get(ctx, key, obj)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Name; v != "B" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.Delete(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. dsmiddleware target & ignored.
		keyInc := client.IDKey("DataA", 111, nil)
		keyExc := client.IDKey("DataB", 111, nil)

		list := []*Data{{Name: "A"}, {Name: "B"}}
		_, err := client.PutMulti(ctx, []datastore.Key{keyInc, keyExc}, list)
		if err != nil {
			t.Fatal(err)
		}

		list = make([]*Data, 2)
		err = client.GetMulti(ctx, []datastore.Key{keyInc, keyExc}, list)
		if err != nil {
			t.Fatal(err)
		}
		if v := len(list); v != 2 {
			t.Fatalf("unexpected: %v", v)
		}
		if v := list[0].Name; v != "A" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[1].Name; v != "B" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.DeleteMulti(ctx, []datastore.Key{keyInc, keyExc})
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. partially hit
		keyIncA := client.IDKey("DataA", 111, nil)
		keyIncB := client.IDKey("DataA", 222, nil)
		keyExcA := client.IDKey("DataB", 111, nil)
		keyExcB := client.IDKey("DataB", 222, nil)

		list := []*Data{{Name: "A1"}, {Name: "A2"}, {Name: "B1"}, {Name: "B2"}}
		_, err := client.PutMulti(ctx, []datastore.Key{keyIncA, keyIncB, keyExcA, keyExcB}, list)
		if err != nil {
			t.Fatal(err)
		}

		ch.DeleteCache(ctx, keyIncB)
		ch.DeleteCache(ctx, keyExcB)

		list = make([]*Data, 4)
		err = client.GetMulti(ctx, []datastore.Key{keyIncA, keyIncB, keyExcA, keyExcB}, list)
		if err != nil {
			t.Fatal(err)
		}
		if v := len(list); v != 4 {
			t.Fatalf("unexpected: %v", v)
		}
		if v := list[0].Name; v != "A1" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[1].Name; v != "A2" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[2].Name; v != "B1" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[3].Name; v != "B2" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.DeleteMulti(ctx, []datastore.Key{keyIncA, keyIncB, keyExcA, keyExcB})
		if err != nil {
			t.Fatal(err)
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=1, keys=[/DataA,111]
		after: PutMultiWithoutTx #1, len(keys)=1, keys=[/DataA,111]
		after: PutMultiWithoutTx #1, keys=[/DataA,111]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataA,111 len(ps)=1
		before: PutMultiWithoutTx #1, keys=[/DataA,111]
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/DataA,111]
		dsmiddleware/localcache.GetMulti: len=1
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataA,111
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataA,111 len(ps)=1
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/DataA,111]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/DataA,111]
		dsmiddleware/localcache.DeleteMulti: len=1
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataA,111
		before: PutMultiWithoutTx #4, len(keys)=1, keys=[/DataB,111]
		after: PutMultiWithoutTx #3, len(keys)=1, keys=[/DataB,111]
		after: PutMultiWithoutTx #3, keys=[/DataB,111]
		before: PutMultiWithoutTx #4, keys=[/DataB,111]
		before: GetMultiWithoutTx #5, len(keys)=1, keys=[/DataB,111]
		after: GetMultiWithoutTx #4, len(keys)=1, keys=[/DataB,111]
		before: DeleteMultiWithoutTx #6, len(keys)=1, keys=[/DataB,111]
		after: DeleteMultiWithoutTx #5, len(keys)=1, keys=[/DataB,111]
		before: PutMultiWithoutTx #7, len(keys)=2, keys=[/DataA,111, /DataB,111]
		after: PutMultiWithoutTx #6, len(keys)=2, keys=[/DataA,111, /DataB,111]
		after: PutMultiWithoutTx #6, keys=[/DataA,111, /DataB,111]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataA,111 len(ps)=1
		before: PutMultiWithoutTx #7, keys=[/DataA,111, /DataB,111]
		before: GetMultiWithoutTx #8, len(keys)=2, keys=[/DataA,111, /DataB,111]
		dsmiddleware/localcache.GetMulti: len=1
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataA,111
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataA,111 len(ps)=1
		after: GetMultiWithoutTx #7, len(keys)=1, keys=[/DataB,111]
		before: DeleteMultiWithoutTx #9, len(keys)=2, keys=[/DataA,111, /DataB,111]
		after: DeleteMultiWithoutTx #8, len(keys)=2, keys=[/DataA,111, /DataB,111]
		dsmiddleware/localcache.DeleteMulti: len=1
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataA,111
		before: PutMultiWithoutTx #10, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: PutMultiWithoutTx #9, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: PutMultiWithoutTx #9, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.SetMulti: len=2
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataA,111 len(ps)=1
		dsmiddleware/localcache.SetMulti: idx=1 key=/DataA,222 len(ps)=1
		before: PutMultiWithoutTx #10, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.DeleteCache: key=/DataA,222
		dsmiddleware/localcache.DeleteCache: key=/DataB,222
		before: GetMultiWithoutTx #11, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.GetMulti: len=2
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataA,111
		dsmiddleware/localcache.GetMulti: idx=1 key=/DataA,222
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataA,111 len(ps)=1
		dsmiddleware/localcache.GetMulti: idx=1, missed key=/DataA,222
		after: GetMultiWithoutTx #10, len(keys)=3, keys=[/DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataA,222 len(ps)=1
		before: DeleteMultiWithoutTx #12, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: DeleteMultiWithoutTx #11, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.DeleteMulti: len=2
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataA,111
		dsmiddleware/localcache.DeleteMulti: idx=1 key=/DataA,222
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func LocalCache_WithExcludeKinds(t *testing.T, ctx context.Context, client datastore.Client) {
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

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	ch := localcache.New(
		localcache.WithExcludeKinds("DataA"),
		localcache.WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
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

	{ // Put. ignored kind.
		key := client.IDKey("DataA", 111, nil)
		objBefore := &Data{Name: "A"}
		_, err := client.Put(ctx, key, objBefore)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = client.Get(ctx, key, obj)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Name; v != "A" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.Delete(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. dsmiddleware ignored.
		key := client.IDKey("DataB", 111, nil)
		objBefore := &Data{Name: "B"}
		_, err := client.Put(ctx, key, objBefore)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = client.Get(ctx, key, obj)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Name; v != "B" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.Delete(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. dsmiddleware target & ignored.
		keyInc := client.IDKey("DataA", 111, nil)
		keyExc := client.IDKey("DataB", 111, nil)

		list := []*Data{{Name: "A"}, {Name: "B"}}
		_, err := client.PutMulti(ctx, []datastore.Key{keyInc, keyExc}, list)
		if err != nil {
			t.Fatal(err)
		}

		list = make([]*Data, 2)
		err = client.GetMulti(ctx, []datastore.Key{keyInc, keyExc}, list)
		if err != nil {
			t.Fatal(err)
		}
		if v := len(list); v != 2 {
			t.Fatalf("unexpected: %v", v)
		}
		if v := list[0].Name; v != "A" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[1].Name; v != "B" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.DeleteMulti(ctx, []datastore.Key{keyInc, keyExc})
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. partially hit
		keyIncA := client.IDKey("DataA", 111, nil)
		keyIncB := client.IDKey("DataA", 222, nil)
		keyExcA := client.IDKey("DataB", 111, nil)
		keyExcB := client.IDKey("DataB", 222, nil)

		list := []*Data{{Name: "A1"}, {Name: "A2"}, {Name: "B1"}, {Name: "B2"}}
		_, err := client.PutMulti(ctx, []datastore.Key{keyIncA, keyIncB, keyExcA, keyExcB}, list)
		if err != nil {
			t.Fatal(err)
		}

		ch.DeleteCache(ctx, keyIncB)
		ch.DeleteCache(ctx, keyExcB)

		list = make([]*Data, 4)
		err = client.GetMulti(ctx, []datastore.Key{keyIncA, keyIncB, keyExcA, keyExcB}, list)
		if err != nil {
			t.Fatal(err)
		}
		if v := len(list); v != 4 {
			t.Fatalf("unexpected: %v", v)
		}
		if v := list[0].Name; v != "A1" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[1].Name; v != "A2" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[2].Name; v != "B1" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[3].Name; v != "B2" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.DeleteMulti(ctx, []datastore.Key{keyIncA, keyIncB, keyExcA, keyExcB})
		if err != nil {
			t.Fatal(err)
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=1, keys=[/DataA,111]
		after: PutMultiWithoutTx #1, len(keys)=1, keys=[/DataA,111]
		after: PutMultiWithoutTx #1, keys=[/DataA,111]
		before: PutMultiWithoutTx #1, keys=[/DataA,111]
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/DataA,111]
		after: GetMultiWithoutTx #2, len(keys)=1, keys=[/DataA,111]
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/DataA,111]
		after: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/DataA,111]
		before: PutMultiWithoutTx #4, len(keys)=1, keys=[/DataB,111]
		after: PutMultiWithoutTx #4, len(keys)=1, keys=[/DataB,111]
		after: PutMultiWithoutTx #4, keys=[/DataB,111]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataB,111 len(ps)=1
		before: PutMultiWithoutTx #4, keys=[/DataB,111]
		before: GetMultiWithoutTx #5, len(keys)=1, keys=[/DataB,111]
		dsmiddleware/localcache.GetMulti: len=1
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataB,111
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataB,111 len(ps)=1
		before: DeleteMultiWithoutTx #6, len(keys)=1, keys=[/DataB,111]
		after: DeleteMultiWithoutTx #5, len(keys)=1, keys=[/DataB,111]
		dsmiddleware/localcache.DeleteMulti: len=1
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataB,111
		before: PutMultiWithoutTx #7, len(keys)=2, keys=[/DataA,111, /DataB,111]
		after: PutMultiWithoutTx #6, len(keys)=2, keys=[/DataA,111, /DataB,111]
		after: PutMultiWithoutTx #6, keys=[/DataA,111, /DataB,111]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataB,111 len(ps)=1
		before: PutMultiWithoutTx #7, keys=[/DataA,111, /DataB,111]
		before: GetMultiWithoutTx #8, len(keys)=2, keys=[/DataA,111, /DataB,111]
		dsmiddleware/localcache.GetMulti: len=1
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataB,111
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataB,111 len(ps)=1
		after: GetMultiWithoutTx #7, len(keys)=1, keys=[/DataA,111]
		before: DeleteMultiWithoutTx #9, len(keys)=2, keys=[/DataA,111, /DataB,111]
		after: DeleteMultiWithoutTx #8, len(keys)=2, keys=[/DataA,111, /DataB,111]
		dsmiddleware/localcache.DeleteMulti: len=1
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataB,111
		before: PutMultiWithoutTx #10, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: PutMultiWithoutTx #9, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: PutMultiWithoutTx #9, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.SetMulti: len=2
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataB,111 len(ps)=1
		dsmiddleware/localcache.SetMulti: idx=1 key=/DataB,222 len(ps)=1
		before: PutMultiWithoutTx #10, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.DeleteCache: key=/DataA,222
		dsmiddleware/localcache.DeleteCache: key=/DataB,222
		before: GetMultiWithoutTx #11, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.GetMulti: len=2
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataB,111
		dsmiddleware/localcache.GetMulti: idx=1 key=/DataB,222
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataB,111 len(ps)=1
		dsmiddleware/localcache.GetMulti: idx=1, missed key=/DataB,222
		after: GetMultiWithoutTx #10, len(keys)=3, keys=[/DataA,111, /DataA,222, /DataB,222]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataB,222 len(ps)=1
		before: DeleteMultiWithoutTx #12, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: DeleteMultiWithoutTx #11, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.DeleteMulti: len=2
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataB,111
		dsmiddleware/localcache.DeleteMulti: idx=1 key=/DataB,222
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func LocalCache_WithKeyFilter(t *testing.T, ctx context.Context, client datastore.Client) {
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

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	ch := localcache.New(
		localcache.WithKeyFilter(func(ctx context.Context, key datastore.Key) bool {
			return key.ID() != 111
		}),
		localcache.WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
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

	{ // Put. dsmiddleware target.
		key := client.IDKey("DataA", 222, nil)
		objBefore := &Data{Name: "A"}
		_, err := client.Put(ctx, key, objBefore)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = client.Get(ctx, key, obj)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Name; v != "A" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.Delete(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. dsmiddleware ignored.
		key := client.IDKey("DataB", 111, nil)
		objBefore := &Data{Name: "B"}
		_, err := client.Put(ctx, key, objBefore)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = client.Get(ctx, key, obj)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Name; v != "B" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.Delete(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. dsmiddleware target & ignored.
		keyIgnore := client.IDKey("DataA", 111, nil)
		keyTarget := client.IDKey("DataB", 222, nil)

		list := []*Data{{Name: "A"}, {Name: "B"}}
		_, err := client.PutMulti(ctx, []datastore.Key{keyIgnore, keyTarget}, list)
		if err != nil {
			t.Fatal(err)
		}

		list = make([]*Data, 2)
		err = client.GetMulti(ctx, []datastore.Key{keyIgnore, keyTarget}, list)
		if err != nil {
			t.Fatal(err)
		}
		if v := len(list); v != 2 {
			t.Fatalf("unexpected: %v", v)
		}
		if v := list[0].Name; v != "A" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[1].Name; v != "B" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.DeleteMulti(ctx, []datastore.Key{keyIgnore, keyTarget})
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put. partially hit
		keyIgnoreA := client.IDKey("DataA", 111, nil)
		keyIgnoreB := client.IDKey("DataB", 111, nil)
		keyTargetA := client.IDKey("DataA", 222, nil)
		keyTargetB := client.IDKey("DataB", 222, nil)

		list := []*Data{{Name: "A1"}, {Name: "A2"}, {Name: "B1"}, {Name: "B2"}}
		_, err := client.PutMulti(ctx, []datastore.Key{keyIgnoreA, keyTargetA, keyIgnoreB, keyTargetB}, list)
		if err != nil {
			t.Fatal(err)
		}

		ch.DeleteCache(ctx, keyIgnoreA)
		ch.DeleteCache(ctx, keyTargetA)

		list = make([]*Data, 4)
		err = client.GetMulti(ctx, []datastore.Key{keyIgnoreA, keyTargetA, keyIgnoreB, keyTargetB}, list)
		if err != nil {
			t.Fatal(err)
		}
		if v := len(list); v != 4 {
			t.Fatalf("unexpected: %v", v)
		}
		if v := list[0].Name; v != "A1" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[1].Name; v != "A2" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[2].Name; v != "B1" {
			t.Errorf("unexpected: %v", v)
		}
		if v := list[3].Name; v != "B2" {
			t.Errorf("unexpected: %v", v)
		}

		err = client.DeleteMulti(ctx, []datastore.Key{keyIgnoreA, keyTargetA, keyIgnoreB, keyTargetB})
		if err != nil {
			t.Fatal(err)
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=1, keys=[/DataA,222]
		after: PutMultiWithoutTx #1, len(keys)=1, keys=[/DataA,222]
		after: PutMultiWithoutTx #1, keys=[/DataA,222]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataA,222 len(ps)=1
		before: PutMultiWithoutTx #1, keys=[/DataA,222]
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/DataA,222]
		dsmiddleware/localcache.GetMulti: len=1
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataA,222
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataA,222 len(ps)=1
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/DataA,222]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/DataA,222]
		dsmiddleware/localcache.DeleteMulti: len=1
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataA,222
		before: PutMultiWithoutTx #4, len(keys)=1, keys=[/DataB,111]
		after: PutMultiWithoutTx #3, len(keys)=1, keys=[/DataB,111]
		after: PutMultiWithoutTx #3, keys=[/DataB,111]
		before: PutMultiWithoutTx #4, keys=[/DataB,111]
		before: GetMultiWithoutTx #5, len(keys)=1, keys=[/DataB,111]
		after: GetMultiWithoutTx #4, len(keys)=1, keys=[/DataB,111]
		before: DeleteMultiWithoutTx #6, len(keys)=1, keys=[/DataB,111]
		after: DeleteMultiWithoutTx #5, len(keys)=1, keys=[/DataB,111]
		before: PutMultiWithoutTx #7, len(keys)=2, keys=[/DataA,111, /DataB,222]
		after: PutMultiWithoutTx #6, len(keys)=2, keys=[/DataA,111, /DataB,222]
		after: PutMultiWithoutTx #6, keys=[/DataA,111, /DataB,222]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataB,222 len(ps)=1
		before: PutMultiWithoutTx #7, keys=[/DataA,111, /DataB,222]
		before: GetMultiWithoutTx #8, len(keys)=2, keys=[/DataA,111, /DataB,222]
		dsmiddleware/localcache.GetMulti: len=1
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataB,222
		dsmiddleware/localcache.GetMulti: idx=0, hit key=/DataB,222 len(ps)=1
		after: GetMultiWithoutTx #7, len(keys)=1, keys=[/DataA,111]
		before: DeleteMultiWithoutTx #9, len(keys)=2, keys=[/DataA,111, /DataB,222]
		after: DeleteMultiWithoutTx #8, len(keys)=2, keys=[/DataA,111, /DataB,222]
		dsmiddleware/localcache.DeleteMulti: len=1
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataB,222
		before: PutMultiWithoutTx #10, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: PutMultiWithoutTx #9, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: PutMultiWithoutTx #9, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.SetMulti: len=2
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataA,222 len(ps)=1
		dsmiddleware/localcache.SetMulti: idx=1 key=/DataB,222 len(ps)=1
		before: PutMultiWithoutTx #10, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.DeleteCache: key=/DataA,111
		dsmiddleware/localcache.DeleteCache: key=/DataA,222
		before: GetMultiWithoutTx #11, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.GetMulti: len=2
		dsmiddleware/localcache.GetMulti: idx=0 key=/DataA,222
		dsmiddleware/localcache.GetMulti: idx=1 key=/DataB,222
		dsmiddleware/localcache.GetMulti: idx=0, missed key=/DataA,222
		dsmiddleware/localcache.GetMulti: idx=1, hit key=/DataB,222 len(ps)=1
		after: GetMultiWithoutTx #10, len(keys)=3, keys=[/DataA,111, /DataA,222, /DataB,111]
		dsmiddleware/localcache.SetMulti: len=1
		dsmiddleware/localcache.SetMulti: idx=0 key=/DataA,222 len(ps)=1
		before: DeleteMultiWithoutTx #12, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		after: DeleteMultiWithoutTx #11, len(keys)=4, keys=[/DataA,111, /DataA,222, /DataB,111, /DataB,222]
		dsmiddleware/localcache.DeleteMulti: len=2
		dsmiddleware/localcache.DeleteMulti: idx=0 key=/DataA,222
		dsmiddleware/localcache.DeleteMulti: idx=1 key=/DataB,222
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func LocalCache_FlushLocalCache(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	ch := localcache.New()
	client.AppendMiddleware(ch)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(ch)
	}()

	type Data struct {
		Name string
	}

	// Put. add to dsmiddleware.
	key := client.IDKey("Data", 111, nil)
	objBefore := &Data{Name: "Data"}
	_, err := client.Put(ctx, key, objBefore)
	if err != nil {
		t.Fatal(err)
	}

	if v := ch.HasCache(key); !v {
		t.Fatalf("unexpected: %v", v)
	}

	ch.FlushLocalCache()

	if v := ch.HasCache(key); v {
		t.Fatalf("unexpected: %v", v)
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

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	ch := localcache.New()
	client.AppendMiddleware(ch)
	defer func() {
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

	// setup. strategies are first in - first apply.

	bLog := dslog.NewLogger("before: ", logf)
	client.AppendMiddleware(bLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(bLog)
	}()

	ch := localcache.New()
	client.AppendMiddleware(ch)
	defer func() {
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

	key := client.NameKey("Data", "a", nil)

	// put to dsmiddleware
	_, err := client.Put(ctx, key, &Data{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	if v := ch.HasCache(key); !v {
		t.Fatalf("unexpected: %v", v)
	}

	{ // Rollback
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// don't put to dsmiddleware before commit
		key2 := client.NameKey("Data", "b", nil)
		_, err = tx.Put(key2, &Data{Name: "After"})
		if err != nil {
			t.Fatal(err)
		}
		if v := ch.HasCache(key2); v {
			t.Fatalf("unexpected: %v", v)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		// don't delete from dsmiddleware before commit
		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}
		if v := ch.HasCache(key); !v {
			t.Fatalf("unexpected: %v", v)
		}

		// rollback.
		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
		if v := ch.CacheLen(); v != 1 {
			t.Fatalf("unexpected: %v", v)
		}
	}

	{ // Commit
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		// don't put to dsmiddleware before commit
		key2 := client.IncompleteKey("Data", nil)
		pKey, err := tx.Put(key2, &Data{Name: "After"})
		if err != nil {
			t.Fatal(err)
		}
		if v := ch.CacheLen(); v != 1 {
			t.Fatalf("unexpected: %v", v)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		// don't delete from dsmiddleware before commit
		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}
		if v := ch.HasCache(key); !v {
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
		// commited, but don't put to dsmiddleware in tx.
		if v := ch.HasCache(key3); v {
			t.Fatalf("unexpected: %v", v)
		}

		if v := ch.CacheLen(); v != 0 {
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
