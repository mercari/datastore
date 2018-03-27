package dslog

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
	"go.mercari.io/datastore/testsuite"
	"google.golang.org/api/iterator"
)

var TestSuite = map[string]testsuite.Test{
	"DsLog_Basic":       DsLog_Basic,
	"DsLog_Query":       DsLog_Query,
	"DsLog_Transaction": DsLog_Transaction,
}

func init() {
	testsuite.MergeTestSuite(TestSuite)
}

func DsLog_Basic(t *testing.T, ctx context.Context, client datastore.Client) {
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
	logger := dslog.NewLogger("log: ", logf)

	client.AppendMiddleware(logger)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(logger)
	}()

	type Data struct {
		Name string
	}

	key := client.IDKey("Data", 111, nil)
	newKey, err := client.Put(ctx, key, &Data{Name: "Data"})
	if err != nil {
		t.Fatal(err)
	}

	err = client.Delete(ctx, newKey)
	if err != nil {
		t.Fatal(err)
	}

	entity := &Data{}
	err = client.Get(ctx, newKey, entity)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}

	keys, err := client.AllocateIDs(ctx, []datastore.Key{
		client.IncompleteKey("TestA", nil),
		client.IncompleteKey("TestB", client.IDKey("Parent", 123, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 2 {
		t.Errorf("unexpected: %v", v)
	}

	var expected *regexp.Regexp
	{
		expectedPattern := heredoc.Doc(`
			log: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
			log: PutMultiWithoutTx #1, keys=[/Data,111]
			log: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
			log: GetMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
			log: GetMultiWithoutTx #3, err=datastore: no such entity
			log: AllocateIDs #4, len(keys)=2, keys=[/TestA,0, /Parent,123/TestB,0]
			log: AllocateIDs #4, keys=[/TestA,@####@, /Parent,123/TestB,@####@]
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

func DsLog_Query(t *testing.T, ctx context.Context, client datastore.Client) {
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
	logger := dslog.NewLogger("log: ", logf)

	client.AppendMiddleware(logger)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(logger)
	}()

	type Data struct {
		Name string
	}

	const size = 10

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

	// Count
	cnt, err = client.Count(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if cnt != size {
		t.Errorf("unexpected: %v", cnt)
	}

	expected := heredoc.Doc(`
		log: PutMultiWithoutTx #1, len(keys)=10, keys=[/Data,#1, /Data,#2, /Data,#3, /Data,#4, /Data,#5, /Data,#6, /Data,#7, /Data,#8, /Data,#9, /Data,#10]
		log: PutMultiWithoutTx #1, keys=[/Data,#1, /Data,#2, /Data,#3, /Data,#4, /Data,#5, /Data,#6, /Data,#7, /Data,#8, /Data,#9, /Data,#10]
		log: Run #2, q=v1:Data&or=-Name
		log: Next #3, q=v1:Data&or=-Name
		log: Next #3, key=/Data,#9
		log: Next #4, q=v1:Data&or=-Name
		log: Next #4, key=/Data,#8
		log: Next #5, q=v1:Data&or=-Name
		log: Next #5, key=/Data,#7
		log: Next #6, q=v1:Data&or=-Name
		log: Next #6, key=/Data,#6
		log: Next #7, q=v1:Data&or=-Name
		log: Next #7, key=/Data,#5
		log: Next #8, q=v1:Data&or=-Name
		log: Next #8, key=/Data,#4
		log: Next #9, q=v1:Data&or=-Name
		log: Next #9, key=/Data,#3
		log: Next #10, q=v1:Data&or=-Name
		log: Next #10, key=/Data,#2
		log: Next #11, q=v1:Data&or=-Name
		log: Next #11, key=/Data,#10
		log: Next #12, q=v1:Data&or=-Name
		log: Next #12, key=/Data,#1
		log: Next #13, q=v1:Data&or=-Name
		log: Next #13, err=no more items in iterator
		log: GetAll #14, q=v1:Data&or=-Name
		log: GetAll #14, len(keys)=10, keys=[/Data,#9, /Data,#8, /Data,#7, /Data,#6, /Data,#5, /Data,#4, /Data,#3, /Data,#2, /Data,#10, /Data,#1]
		log: Count #15, q=v1:Data&or=-Name
		log: Count #15, ret=10
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func DsLog_Transaction(t *testing.T, ctx context.Context, client datastore.Client) {
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
	logger := dslog.NewLogger("log: ", logf)

	client.AppendMiddleware(logger)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveMiddleware(logger)
	}()

	type Data struct {
		Name string
	}

	key := client.NameKey("Data", "a", nil)
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

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
	}

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

		commit, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}

		key3 := commit.Key(pKey)
		if v := key3.Name(); v != key2.Name() {
			t.Errorf("unexpected: %v", v)
		}
	}

	var expected *regexp.Regexp
	{
		expectedPattern := heredoc.Doc(`
			log: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,a]
			log: PutMultiWithoutTx #1, keys=[/Data,a]
			log: PutMultiWithTx #2, len(keys)=1, keys=[/Data,b]
			log: GetMultiWithTx #3, len(keys)=1, keys=[/Data,a]
			log: DeleteMultiWithTx #4, len(keys)=1, keys=[/Data,a]
			log: PostRollback #5
			log: PutMultiWithTx #6, len(keys)=1, keys=[/Data,0]
			log: GetMultiWithTx #7, len(keys)=1, keys=[/Data,a]
			log: DeleteMultiWithTx #8, len(keys)=1, keys=[/Data,a]
			log: PostCommit #9 Put keys=[/Data,@####@]
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
