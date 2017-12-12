package fishbone

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/MakeNowJust/heredoc"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/dslog"
	"go.mercari.io/datastore/dsmiddleware/fishbone"
	"go.mercari.io/datastore/testsuite"
	"google.golang.org/api/iterator"
)

var TestSuite = map[string]testsuite.Test{
	"FishBone_QueryWithoutTx": FishBone_QueryWithoutTx,
	"FishBone_QueryWithTx":    FishBone_QueryWithTx,
}

func init() {
	testsuite.MergeTestSuite(TestSuite)
}

func FishBone_QueryWithoutTx(t *testing.T, ctx context.Context, client datastore.Client) {
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

	m := fishbone.New()
	client.AppendMiddleware(m)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(m)
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
	q = client.NewQuery("Data").Order("-Name")
	list = nil
	keys, err = client.GetAll(ctx, q, &list)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(list); v != size {
		t.Errorf("unexpected: %v", v)
	}
	for idx, obj := range list {
		if v := obj.Name; v == "" || v != keys[idx].Name() {
			t.Errorf("unexpected: %v", v)
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,#1, /Data,#2, /Data,#3]
		after: PutMultiWithoutTx #1, keys=[/Data,#1, /Data,#2, /Data,#3]
		before: PutMultiWithoutTx #1, keys=[/Data,#1, /Data,#2, /Data,#3]
		before: Run #2, q=v1:Data&or=-Name
		after: Run #2, q=v1:Data&or=-Name&k=t
		before: Next #3, q=v1:Data&or=-Name
		after: Next #3, q=v1:Data&or=-Name
		after: Next #3, key=/Data,#3
		before: GetMultiWithoutTx #4, len(keys)=1, keys=[/Data,#3]
		after: GetMultiWithoutTx #4, len(keys)=1, keys=[/Data,#3]
		before: Next #3, key=/Data,#3
		before: Next #5, q=v1:Data&or=-Name
		after: Next #5, q=v1:Data&or=-Name
		after: Next #5, key=/Data,#2
		before: GetMultiWithoutTx #6, len(keys)=1, keys=[/Data,#2]
		after: GetMultiWithoutTx #6, len(keys)=1, keys=[/Data,#2]
		before: Next #5, key=/Data,#2
		before: Next #7, q=v1:Data&or=-Name
		after: Next #7, q=v1:Data&or=-Name
		after: Next #7, key=/Data,#1
		before: GetMultiWithoutTx #8, len(keys)=1, keys=[/Data,#1]
		after: GetMultiWithoutTx #8, len(keys)=1, keys=[/Data,#1]
		before: Next #7, key=/Data,#1
		before: Next #9, q=v1:Data&or=-Name
		after: Next #9, q=v1:Data&or=-Name
		after: Next #9, err=no more items in iterator
		before: Next #9, err=no more items in iterator
		before: GetAll #10, q=v1:Data&or=-Name
		after: GetAll #10, q=v1:Data&or=-Name&k=t
		after: GetAll #10, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
		before: GetMultiWithoutTx #11, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
		after: GetMultiWithoutTx #11, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
		before: GetAll #10, len(keys)=3, keys=[/Data,#3, /Data,#2, /Data,#1]
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func FishBone_QueryWithTx(t *testing.T, ctx context.Context, client datastore.Client) {
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

	m := fishbone.New()
	client.AppendMiddleware(m)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(m)
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

	parentKey := client.NameKey("Parent", "p", nil)
	keys := make([]datastore.Key, size)
	list := make([]*Data, size)
	for i := 0; i < size; i++ {
		keys[i] = client.NameKey("Data", fmt.Sprintf("#%d", i+1), parentKey)
		list[i] = &Data{
			Name: fmt.Sprintf("#%d", i+1),
		}
	}
	_, err := client.PutMulti(ctx, keys, list)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
	}()

	q := client.NewQuery("Data").Order("-Name").Ancestor(parentKey).Transaction(tx)

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
	q = client.NewQuery("Data").Order("-Name").Ancestor(parentKey).Transaction(tx)
	list = nil
	keys, err = client.GetAll(ctx, q, &list)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(list); v != size {
		t.Errorf("unexpected: %v", v)
	}
	for idx, obj := range list {
		if v := obj.Name; v == "" || v != keys[idx].Name() {
			t.Errorf("unexpected: %v", v)
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=3, keys=[/Parent,p/Data,#1, /Parent,p/Data,#2, /Parent,p/Data,#3]
		after: PutMultiWithoutTx #1, len(keys)=3, keys=[/Parent,p/Data,#1, /Parent,p/Data,#2, /Parent,p/Data,#3]
		after: PutMultiWithoutTx #1, keys=[/Parent,p/Data,#1, /Parent,p/Data,#2, /Parent,p/Data,#3]
		before: PutMultiWithoutTx #1, keys=[/Parent,p/Data,#1, /Parent,p/Data,#2, /Parent,p/Data,#3]
		before: Run #2, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Run #2, q=v1:Data&a=/Parent,p&t=t&or=-Name&k=t
		before: Next #3, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #3, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #3, key=/Parent,p/Data,#3
		before: GetMultiWithTx #4, len(keys)=1, keys=[/Parent,p/Data,#3]
		after: GetMultiWithTx #4, len(keys)=1, keys=[/Parent,p/Data,#3]
		before: Next #3, key=/Parent,p/Data,#3
		before: Next #5, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #5, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #5, key=/Parent,p/Data,#2
		before: GetMultiWithTx #6, len(keys)=1, keys=[/Parent,p/Data,#2]
		after: GetMultiWithTx #6, len(keys)=1, keys=[/Parent,p/Data,#2]
		before: Next #5, key=/Parent,p/Data,#2
		before: Next #7, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #7, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #7, key=/Parent,p/Data,#1
		before: GetMultiWithTx #8, len(keys)=1, keys=[/Parent,p/Data,#1]
		after: GetMultiWithTx #8, len(keys)=1, keys=[/Parent,p/Data,#1]
		before: Next #7, key=/Parent,p/Data,#1
		before: Next #9, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #9, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: Next #9, err=no more items in iterator
		before: Next #9, err=no more items in iterator
		before: GetAll #10, q=v1:Data&a=/Parent,p&t=t&or=-Name
		after: GetAll #10, q=v1:Data&a=/Parent,p&t=t&or=-Name&k=t
		after: GetAll #10, len(keys)=3, keys=[/Parent,p/Data,#3, /Parent,p/Data,#2, /Parent,p/Data,#1]
		before: GetMultiWithTx #11, len(keys)=3, keys=[/Parent,p/Data,#3, /Parent,p/Data,#2, /Parent,p/Data,#1]
		after: GetMultiWithTx #11, len(keys)=3, keys=[/Parent,p/Data,#3, /Parent,p/Data,#2, /Parent,p/Data,#1]
		before: GetAll #10, len(keys)=3, keys=[/Parent,p/Data,#3, /Parent,p/Data,#2, /Parent,p/Data,#1]
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}
