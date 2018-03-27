package rpcretry

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
	rpcretry "go.mercari.io/datastore/dsmiddleware/rpcretry"
	"go.mercari.io/datastore/testsuite"
	"google.golang.org/api/iterator"
)

var TestSuite = map[string]testsuite.Test{
	"RPCRetry_Basic":       RPCRetry_Basic,
	"RPCRetry_Transaction": RPCRetry_Transaction,
	"RPCRetry_AllocateIDs": RPCRetry_AllocateIDs,
	"RPCRetry_GetAll":      RPCRetry_GetAll,
	"RPCRetry_Count":       RPCRetry_Count,
}

func RPCRetry_Basic(t *testing.T, ctx context.Context, client datastore.Client) {
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

	rh := rpcretry.New(
		rpcretry.WithLogf(logf),
		rpcretry.WithMinBackoffDuration(1),
		rpcretry.WithRetryLimit(4),
	)
	client.AppendMiddleware(rh)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(rh)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	gm := &glitchEmulator{errCount: 3}
	client.AppendMiddleware(gm)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(gm)
	}()

	type Data struct {
		Name string
	}

	// Put.
	key := client.IDKey("Data", 111, nil)
	objBefore := &Data{Name: "Data"}
	_, err := client.Put(ctx, key, objBefore)
	if err != nil {
		t.Fatal(err)
	}

	// Get.
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

	expected := heredoc.Doc(`
		## Put.
		before: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #1, err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.PutMultiWithoutTx: err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,111, will be retry #1 after 1ns
		after: PutMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #2, err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.PutMultiWithoutTx: err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,111, will be retry #2 after 2ns
		after: PutMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #3, err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.PutMultiWithoutTx: err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,111, will be retry #3 after 4ns
		after: PutMultiWithoutTx #4, len(keys)=1, keys=[/Data,111]
		after: PutMultiWithoutTx #4, keys=[/Data,111]
		before: PutMultiWithoutTx #1, keys=[/Data,111]
		## Get.
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		after: GetMultiWithoutTx #5, len(keys)=1, keys=[/Data,111]
		after: GetMultiWithoutTx #5, err=error by *glitchEmulator: GetMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.GetMultiWithoutTx: err=error by *glitchEmulator: GetMultiWithoutTx, keys=/Data,111, will be retry #1 after 1ns
		after: GetMultiWithoutTx #6, len(keys)=1, keys=[/Data,111]
		after: GetMultiWithoutTx #6, err=error by *glitchEmulator: GetMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.GetMultiWithoutTx: err=error by *glitchEmulator: GetMultiWithoutTx, keys=/Data,111, will be retry #2 after 2ns
		after: GetMultiWithoutTx #7, len(keys)=1, keys=[/Data,111]
		after: GetMultiWithoutTx #7, err=error by *glitchEmulator: GetMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.GetMultiWithoutTx: err=error by *glitchEmulator: GetMultiWithoutTx, keys=/Data,111, will be retry #3 after 4ns
		after: GetMultiWithoutTx #8, len(keys)=1, keys=[/Data,111]
		## Delete.
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #9, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #9, err=error by *glitchEmulator: DeleteMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.DeleteMultiWithoutTx: err=error by *glitchEmulator: DeleteMultiWithoutTx, keys=/Data,111, will be retry #1 after 1ns
		after: DeleteMultiWithoutTx #10, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #10, err=error by *glitchEmulator: DeleteMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.DeleteMultiWithoutTx: err=error by *glitchEmulator: DeleteMultiWithoutTx, keys=/Data,111, will be retry #2 after 2ns
		after: DeleteMultiWithoutTx #11, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #11, err=error by *glitchEmulator: DeleteMultiWithoutTx, keys=/Data,111
		middleware/rpcretry.DeleteMultiWithoutTx: err=error by *glitchEmulator: DeleteMultiWithoutTx, keys=/Data,111, will be retry #3 after 4ns
		after: DeleteMultiWithoutTx #12, len(keys)=1, keys=[/Data,111]
	`)
	// strip `## FooBar` comment line
	expected = regexp.MustCompile("(?m)^##.*\n").ReplaceAllString(expected, "")

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func RPCRetry_Transaction(t *testing.T, ctx context.Context, client datastore.Client) {
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

	rh := rpcretry.New(
		rpcretry.WithLogf(logf),
		rpcretry.WithMinBackoffDuration(1),
		rpcretry.WithRetryLimit(2),
	)
	client.AppendMiddleware(rh)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(rh)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	gm := &glitchEmulator{errCount: 1}
	client.AppendMiddleware(gm)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(gm)
	}()

	// exec.

	type Data struct {
		Name string
	}

	key := client.NameKey("Data", "a", nil)

	// put
	_, err := client.Put(ctx, key, &Data{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}

	{ // Rollback
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		key2 := client.NameKey("Data", "b", nil)
		_, err = tx.Put(key2, &Data{Name: "After"})
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}

		// rollback.
		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
	}

	// reset
	gm.raised = nil

	{ // Commit
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		key2 := client.IncompleteKey("Data", nil)
		pKey, err := tx.Put(key2, &Data{Name: "After"})
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Delete(key)
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
	}

	var expected *regexp.Regexp
	{
		expectedPattern := heredoc.Doc(`
			## put
			before: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,a]
			after: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,a]
			after: PutMultiWithoutTx #1, err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,a
			middleware/rpcretry.PutMultiWithoutTx: err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,a, will be retry #1 after 1ns
			after: PutMultiWithoutTx #2, len(keys)=1, keys=[/Data,a]
			after: PutMultiWithoutTx #2, keys=[/Data,a]
			before: PutMultiWithoutTx #1, keys=[/Data,a]
			## Rollback
			### put
			before: PutMultiWithTx #2, len(keys)=1, keys=[/Data,b]
			after: PutMultiWithTx #3, len(keys)=1, keys=[/Data,b]
			after: PutMultiWithTx #3, err=error by *glitchEmulator: PutMultiWithTx, keys=/Data,b
			after: PutMultiWithTx #3, keys length mismatch len(keys)=1, len(pKeys)=0
			middleware/rpcretry.PutMultiWithTx: err=error by *glitchEmulator: PutMultiWithTx, keys=/Data,b, will be retry #1 after 1ns
			after: PutMultiWithTx #4, len(keys)=1, keys=[/Data,b]
			### get
			before: GetMultiWithTx #3, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #5, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #5, err=error by *glitchEmulator: GetMultiWithTx, keys=/Data,a
			middleware/rpcretry.GetMultiWithTx: err=error by *glitchEmulator: GetMultiWithTx, keys=/Data,a, will be retry #1 after 1ns
			after: GetMultiWithTx #6, len(keys)=1, keys=[/Data,a]
			### delete
			before: DeleteMultiWithTx #4, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #7, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #7, err=error by *glitchEmulator: DeleteMultiWithTx, keys=/Data,a
			middleware/rpcretry.DeleteMultiWithTx: err=error by *glitchEmulator: DeleteMultiWithTx, keys=/Data,a, will be retry #1 after 1ns
			after: DeleteMultiWithTx #8, len(keys)=1, keys=[/Data,a]
			### rollback
			before: PostRollback #5
			after: PostRollback #9
			## Commit
			### put
			before: PutMultiWithTx #6, len(keys)=1, keys=[/Data,0]
			after: PutMultiWithTx #10, len(keys)=1, keys=[/Data,0]
			after: PutMultiWithTx #10, err=error by *glitchEmulator: PutMultiWithTx, keys=/Data,0
			after: PutMultiWithTx #10, keys length mismatch len(keys)=1, len(pKeys)=0
			middleware/rpcretry.PutMultiWithTx: err=error by *glitchEmulator: PutMultiWithTx, keys=/Data,0, will be retry #1 after 1ns
			after: PutMultiWithTx #11, len(keys)=1, keys=[/Data,0]
			### get
			before: GetMultiWithTx #7, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #12, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #12, err=error by *glitchEmulator: GetMultiWithTx, keys=/Data,a
			middleware/rpcretry.GetMultiWithTx: err=error by *glitchEmulator: GetMultiWithTx, keys=/Data,a, will be retry #1 after 1ns
			after: GetMultiWithTx #13, len(keys)=1, keys=[/Data,a]
			### delete
			before: DeleteMultiWithTx #8, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #14, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #14, err=error by *glitchEmulator: DeleteMultiWithTx, keys=/Data,a
			middleware/rpcretry.DeleteMultiWithTx: err=error by *glitchEmulator: DeleteMultiWithTx, keys=/Data,a, will be retry #1 after 1ns
			after: DeleteMultiWithTx #15, len(keys)=1, keys=[/Data,a]
			### commit
			before: PostCommit #9 Put keys=[/Data,@####@]
			after: PostCommit #16 Put keys=[/Data,@####@]
		`)
		// strip `## FooBar` comment line
		expectedPattern = regexp.MustCompile("(?m)^##.*\n").ReplaceAllString(expectedPattern, "")
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

func RPCRetry_AllocateIDs(t *testing.T, ctx context.Context, client datastore.Client) {
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

	rh := rpcretry.New(
		rpcretry.WithLogf(logf),
		rpcretry.WithMinBackoffDuration(1),
		rpcretry.WithRetryLimit(2),
	)
	client.AppendMiddleware(rh)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(rh)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	gm := &glitchEmulator{errCount: 1}
	client.AppendMiddleware(gm)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(gm)
	}()

	// exec.

	keys, err := client.AllocateIDs(ctx, []datastore.Key{client.IncompleteKey("Data", nil)})
	if err != nil {
		t.Fatal(err)
	}

	if v := len(keys); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
	if v := keys[0].Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := keys[0].ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}

	expectedPattern := heredoc.Doc(`
		before: AllocateIDs #1, len(keys)=1, keys=[/Data,0]
		after: AllocateIDs #1, len(keys)=1, keys=[/Data,0]
		after: AllocateIDs #1, err=error by *glitchEmulator: AllocateIDs, keys=/Data,0
		middleware/rpcretry.AllocateIDs: err=error by *glitchEmulator: AllocateIDs, keys=/Data,0, will be retry #1 after 1ns
		after: AllocateIDs #2, len(keys)=1, keys=[/Data,0]
		after: AllocateIDs #2, keys=[/Data,@####@]
		before: AllocateIDs #1, keys=[/Data,@####@]
	`)
	// strip `## FooBar` comment line
	expectedPattern = regexp.MustCompile("(?m)^##.*\n").ReplaceAllString(expectedPattern, "")
	ss := strings.Split(expectedPattern, "@####@")
	var buf bytes.Buffer
	for idx, s := range ss {
		buf.WriteString(regexp.QuoteMeta(s))
		if idx != (len(ss) - 1) {
			buf.WriteString("[0-9]+")
		}
	}
	expected := regexp.MustCompile(buf.String())

	if v := strings.Join(logs, "\n") + "\n"; !expected.MatchString(v) {
		t.Errorf("unexpected: %v", v)
	}
}

func RPCRetry_GetAll(t *testing.T, ctx context.Context, client datastore.Client) {
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

	rh := rpcretry.New(
		rpcretry.WithLogf(logf),
		rpcretry.WithMinBackoffDuration(1),
		rpcretry.WithRetryLimit(2),
	)
	client.AppendMiddleware(rh)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(rh)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	gm := &glitchEmulator{errCount: 1}
	client.AppendMiddleware(gm)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(gm)
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
		## Put
		before: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,#1, /Data,#2, /Data,#3
		middleware/rpcretry.PutMultiWithoutTx: err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,#1, /Data,#2, /Data,#3, will be retry #1 after 1ns
		after: PutMultiWithoutTx #2, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #2, keys=[/Data,#1, /Data,#2, /Data,#3]
		before: PutMultiWithoutTx #1, keys=[/Data,#1, /Data,#2, /Data,#3]
		## Run & Next
		before: Run #2, q=v1:Data&or=-Name
		after: Run #3, q=v1:Data&or=-Name
		before: Next #3, q=v1:Data&or=-Name
		after: Next #4, q=v1:Data&or=-Name
		after: Next #4, key=/Data,#3
		before: Next #3, key=/Data,#3
		before: Next #4, q=v1:Data&or=-Name
		after: Next #5, q=v1:Data&or=-Name
		after: Next #5, key=/Data,#2
		before: Next #4, key=/Data,#2
		before: Next #5, q=v1:Data&or=-Name
		after: Next #6, q=v1:Data&or=-Name
		after: Next #6, key=/Data,#1
		before: Next #5, key=/Data,#1
		before: Next #6, q=v1:Data&or=-Name
		after: Next #7, q=v1:Data&or=-Name
		after: Next #7, err=no more items in iterator
		before: Next #6, err=no more items in iterator
		## GetAll
		before: GetAll #7, q=v1:Data&or=-Name
		after: GetAll #8, q=v1:Data&or=-Name
		after: GetAll #8, err=error by *glitchEmulator: GetAll, keys=
		middleware/rpcretry.GetAll: err=error by *glitchEmulator: GetAll, keys=, will be retry #1 after 1ns
		after: GetAll #9, q=v1:Data&or=-Name
		after: GetAll #9, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
		before: GetAll #7, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
	`)
	// strip `## FooBar` comment line
	expected = regexp.MustCompile("(?m)^##.*\n").ReplaceAllString(expected, "")

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func RPCRetry_Count(t *testing.T, ctx context.Context, client datastore.Client) {
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

	rh := rpcretry.New(
		rpcretry.WithLogf(logf),
		rpcretry.WithMinBackoffDuration(1),
		rpcretry.WithRetryLimit(2),
	)
	client.AppendMiddleware(rh)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(rh)
	}()

	aLog := dslog.NewLogger("after: ", logf)
	client.AppendMiddleware(aLog)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(aLog)
	}()

	gm := &glitchEmulator{errCount: 1}
	client.AppendMiddleware(gm)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(gm)
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

	q := client.NewQuery("Data")

	cnt, err := client.Count(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	if cnt != size {
		t.Errorf("unexpected: %v", cnt)
	}

	expected := heredoc.Doc(`
		## Put
		before: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,#1, /Data,#2, /Data,#3
		middleware/rpcretry.PutMultiWithoutTx: err=error by *glitchEmulator: PutMultiWithoutTx, keys=/Data,#1, /Data,#2, /Data,#3, will be retry #1 after 1ns
		after: PutMultiWithoutTx #2, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #2, keys=[/Data,#1, /Data,#2, /Data,#3]
		before: PutMultiWithoutTx #1, keys=[/Data,#1, /Data,#2, /Data,#3]
		## Count
		before: Count #2, q=v1:Data
		after: Count #3, q=v1:Data
		after: Count #3, err=error by *glitchEmulator: Count, keys=
		middleware/rpcretry.Count: err=error by *glitchEmulator: Count, keys=, will be retry #1 after 1ns
		after: Count #4, q=v1:Data
		after: Count #4, ret=3
		before: Count #2, ret=3
	`)
	// strip `## FooBar` comment line
	expected = regexp.MustCompile("(?m)^##.*\n").ReplaceAllString(expected, "")

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}
