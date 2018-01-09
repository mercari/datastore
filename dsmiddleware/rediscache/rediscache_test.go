package rediscache

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/garyburd/redigo/redis"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/dslog"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
	"go.mercari.io/datastore/internal/testutils"
	"google.golang.org/api/iterator"
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

func TestRedisCache_Basic(t *testing.T) {
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

	dial, err := net.Dial("tcp", os.Getenv("REDIS_HOST")+":"+os.Getenv("REDIS_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	defer dial.Close()
	conn := redis.NewConn(dial, 100*time.Millisecond, 100*time.Millisecond)
	defer conn.Close()
	ch := New(
		conn,
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		_, err := conn.Do("FLUSHALL")
		if err != nil {
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
	_, err = client.Put(ctx, key, objBefore)
	if err != nil {
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
		dsmiddleware/rediscache.SetMulti: incoming len=1
		dsmiddleware/rediscache.SetMulti: len=1
		before: PutMultiWithoutTx #1, keys=[/Data,111]
		dsmiddleware/rediscache.GetMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: hit=1 miss=0
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/rediscache.GetMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: hit=1 miss=0
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/rediscache.DeleteMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: hit=0 miss=1
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func TestRedisCache_BasicWithoutExpire(t *testing.T) {
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

	dial, err := net.Dial("tcp", os.Getenv("REDIS_HOST")+":"+os.Getenv("REDIS_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	defer dial.Close()
	conn := redis.NewConn(dial, 100*time.Millisecond, 100*time.Millisecond)
	defer conn.Close()
	ch := New(
		conn,
		WithExpireDuration(0),
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		_, err := conn.Do("FLUSHALL")
		if err != nil {
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
	_, err = client.Put(ctx, key, objBefore)
	if err != nil {
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
		dsmiddleware/rediscache.SetMulti: incoming len=1
		dsmiddleware/rediscache.SetMulti: len=1
		before: PutMultiWithoutTx #1, keys=[/Data,111]
		dsmiddleware/rediscache.GetMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: hit=1 miss=0
		before: GetMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/rediscache.GetMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: hit=1 miss=0
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		dsmiddleware/rediscache.DeleteMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: incoming len=1
		dsmiddleware/rediscache.GetMulti: hit=0 miss=1
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func TestRedisCache_Query(t *testing.T) {
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

	dial, err := net.Dial("tcp", os.Getenv("REDIS_HOST")+":"+os.Getenv("REDIS_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	defer dial.Close()
	conn := redis.NewConn(dial, 100*time.Millisecond, 100*time.Millisecond)
	defer conn.Close()
	ch := New(
		conn,
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		_, err := conn.Do("FLUSHALL")
		if err != nil {
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

	const size = 3

	keys := make([]datastore.Key, size)
	list := make([]*Data, size)
	for i := 0; i < size; i++ {
		keys[i] = client.NameKey("Data", fmt.Sprintf("#%d", i+1), nil)
		list[i] = &Data{
			Name: fmt.Sprintf("#%d", i+1),
		}
	}
	_, err = client.PutMulti(ctx, keys, list)
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
		dsmiddleware/rediscache.SetMulti: incoming len=3
		dsmiddleware/rediscache.SetMulti: len=3
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

func TestRedisCache_Transaction(t *testing.T) {
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

	dial, err := net.Dial("tcp", os.Getenv("REDIS_HOST")+":"+os.Getenv("REDIS_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	defer dial.Close()
	conn := redis.NewConn(dial, 100*time.Millisecond, 100*time.Millisecond)
	defer conn.Close()
	ch := New(
		conn,
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		_, err := conn.Do("FLUSHALL")
		if err != nil {
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

	key := client.NameKey("Data", "a", nil)

	// put to cache
	_, err = client.Put(ctx, key, &Data{Name: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	hit, err := inCache(ctx, ch, key)
	if err != nil {
		t.Fatal(err)
	} else if v := hit; !v {
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
		hit, err = inCache(ctx, ch, key2)
		if err != nil {
			t.Fatal(err)
		} else if v := hit; v {
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
		hit, err = inCache(ctx, ch, key)
		if err != nil {
			t.Fatal(err)
		} else if v := hit; !v {
			t.Fatalf("unexpected: %v", v)
		}

		// rollback.
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

		// don't put to cache before commit
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

		// don't delete from cache before commit
		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}
		hit, err = inCache(ctx, ch, key)
		if err != nil {
			t.Fatal(err)
		} else if v := hit; !v {
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
		hit, err = inCache(ctx, ch, key3)
		if err != nil {
			t.Fatal(err)
		} else if v := hit; v {
			t.Fatalf("unexpected: %v", v)
		}
	}

	var expected *regexp.Regexp
	{
		expectedPattern := heredoc.Doc(`
			before: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,a]
			after: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,a]
			after: PutMultiWithoutTx #1, keys=[/Data,a]
			dsmiddleware/rediscache.SetMulti: incoming len=1
			dsmiddleware/rediscache.SetMulti: len=1
			before: PutMultiWithoutTx #1, keys=[/Data,a]
			dsmiddleware/rediscache.GetMulti: incoming len=1
			dsmiddleware/rediscache.GetMulti: hit=1 miss=0
			before: PutMultiWithTx #2, len(keys)=1, keys=[/Data,b]
			after: PutMultiWithTx #2, len(keys)=1, keys=[/Data,b]
			dsmiddleware/rediscache.GetMulti: incoming len=1
			dsmiddleware/rediscache.GetMulti: hit=0 miss=1
			before: GetMultiWithTx #3, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #3, len(keys)=1, keys=[/Data,a]
			before: DeleteMultiWithTx #4, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #4, len(keys)=1, keys=[/Data,a]
			dsmiddleware/rediscache.GetMulti: incoming len=1
			dsmiddleware/rediscache.GetMulti: hit=1 miss=0
			before: PostRollback #5
			after: PostRollback #5
			before: PutMultiWithTx #6, len(keys)=1, keys=[/Data,0]
			after: PutMultiWithTx #6, len(keys)=1, keys=[/Data,0]
			before: GetMultiWithTx #7, len(keys)=1, keys=[/Data,a]
			after: GetMultiWithTx #7, len(keys)=1, keys=[/Data,a]
			before: DeleteMultiWithTx #8, len(keys)=1, keys=[/Data,a]
			after: DeleteMultiWithTx #8, len(keys)=1, keys=[/Data,a]
			dsmiddleware/rediscache.GetMulti: incoming len=1
			dsmiddleware/rediscache.GetMulti: hit=1 miss=0
			before: PostCommit #9 Put keys=[/Data,@####@]
			dsmiddleware/rediscache.DeleteMulti: incoming len=3
			after: PostCommit #9 Put keys=[/Data,@####@]
			dsmiddleware/rediscache.GetMulti: incoming len=1
			dsmiddleware/rediscache.GetMulti: hit=0 miss=1
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

func TestRedisCache_MultiError(t *testing.T) {
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

	dial, err := net.Dial("tcp", os.Getenv("REDIS_HOST")+":"+os.Getenv("REDIS_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	defer dial.Close()
	conn := redis.NewConn(dial, 100*time.Millisecond, 100*time.Millisecond)
	defer conn.Close()
	ch := New(
		conn,
		WithLogger(logf),
	)
	client.AppendMiddleware(ch)
	defer func() {
		_, err := conn.Do("FLUSHALL")
		if err != nil {
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

	_, err = client.PutMulti(ctx, keys, list)
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
			// Delete entity where out of redis scope
			// delete entity id=3, 6, 9
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
		dsmiddleware/rediscache.SetMulti: incoming len=10
		dsmiddleware/rediscache.SetMulti: len=10
		before: PutMultiWithoutTx #1, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		dsmiddleware/rediscache.DeleteMulti: incoming len=1
		before: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,3]
		after: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,3]
		dsmiddleware/rediscache.DeleteMulti: incoming len=1
		dsmiddleware/rediscache.DeleteMulti: incoming len=1
		before: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,6]
		after: DeleteMultiWithoutTx #3, len(keys)=1, keys=[/Data,6]
		dsmiddleware/rediscache.DeleteMulti: incoming len=1
		before: DeleteMultiWithoutTx #4, len(keys)=1, keys=[/Data,9]
		after: DeleteMultiWithoutTx #4, len(keys)=1, keys=[/Data,9]
		dsmiddleware/rediscache.DeleteMulti: incoming len=1
		before: GetMultiWithoutTx #5, len(keys)=10, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5, /Data,6, /Data,7, /Data,8, /Data,9, /Data,10]
		dsmiddleware/rediscache.GetMulti: incoming len=10
		dsmiddleware/rediscache.GetMulti: hit=5 miss=5
		after: GetMultiWithoutTx #5, len(keys)=5, keys=[/Data,2, /Data,4, /Data,6, /Data,8, /Data,10]
		after: GetMultiWithoutTx #5, err=datastore: no such entity
		dsmiddleware/rediscache.SetMulti: incoming len=4
		dsmiddleware/rediscache.SetMulti: len=4
		before: GetMultiWithoutTx #5, err=datastore: no such entity
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}
