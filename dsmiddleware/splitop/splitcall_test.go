package splitop

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/MakeNowJust/heredoc"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/dslog"
	"go.mercari.io/datastore/internal/testutils"
)

func TestSplitCall_Basic(t *testing.T) {
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

	ch := New(
		WithLogger(logf),
		WithSplitThreshold(3),
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

	// Put
	var keys []datastore.Key
	var entities []*Data
	for i := 1; i <= 5; i++ {
		keys = append(keys, client.IDKey("Data", int64(i), nil))
		entities = append(entities, &Data{Name: fmt.Sprintf("Data%d", i)})
	}
	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatal(err)
	}

	// Get
	result := make([]*Data, len(keys))
	err = client.GetMulti(ctx, keys, result)
	if err != nil {
		t.Fatal(err)
	}

	for idx, obj := range result {
		if v := obj.Name; v != fmt.Sprintf("Data%d", keys[idx].ID()) {
			t.Errorf("unexpected: %v", v)
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=5, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5]
		after: PutMultiWithoutTx #1, len(keys)=5, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5]
		after: PutMultiWithoutTx #1, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5]
		before: PutMultiWithoutTx #1, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5]
		before: GetMultiWithoutTx #2, len(keys)=5, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5]
		process 5 keys
		process [0, 3) range keys
		after: GetMultiWithoutTx #2, len(keys)=3, keys=[/Data,1, /Data,2, /Data,3]
		process [3, 5) range keys
		after: GetMultiWithoutTx #3, len(keys)=2, keys=[/Data,4, /Data,5]
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}

func TestSplitCall_HasNoSuchEntity(t *testing.T) {
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

	ch := New(
		WithLogger(logf),
		WithSplitThreshold(3),
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

	// Put
	var keys []datastore.Key
	{
		var putKeys []datastore.Key
		var entities []*Data
		for i := 1; i <= 5; i++ {
			key := client.IDKey("Data", int64(i), nil)
			keys = append(keys, key)

			switch key.ID() {
			case 1, 3, 5:
				putKeys = append(putKeys, key)
				entities = append(entities, &Data{Name: fmt.Sprintf("Data%d", key.ID())})
			}
		}
		_, err := client.PutMulti(ctx, putKeys, entities)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get
	result := make([]*Data, len(keys))
	err := client.GetMulti(ctx, keys, result)
	if mErr, ok := err.(datastore.MultiError); !ok {
		t.Fatal(err)
	} else {
		for idx, err := range mErr {
			id := keys[idx].ID()
			switch id {
			case 1, 3, 5:
				if err != nil {
					t.Errorf("unexpected error content: %d, %v", id, err)
				}
			default:
				if err != datastore.ErrNoSuchEntity {
					t.Errorf("unexpected error content: %d, %v", id, err)
				}
			}
		}
	}

	for idx, obj := range result {
		if obj == nil {
			continue
		}
		if v := obj.Name; v != fmt.Sprintf("Data%d", keys[idx].ID()) {
			t.Errorf("unexpected: %v", v)
		}
	}

	expected := heredoc.Doc(`
		before: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,1, /Data,3, /Data,5]
		after: PutMultiWithoutTx #1, len(keys)=3, keys=[/Data,1, /Data,3, /Data,5]
		after: PutMultiWithoutTx #1, keys=[/Data,1, /Data,3, /Data,5]
		before: PutMultiWithoutTx #1, keys=[/Data,1, /Data,3, /Data,5]
		before: GetMultiWithoutTx #2, len(keys)=5, keys=[/Data,1, /Data,2, /Data,3, /Data,4, /Data,5]
		process 5 keys
		process [0, 3) range keys
		after: GetMultiWithoutTx #2, len(keys)=3, keys=[/Data,1, /Data,2, /Data,3]
		after: GetMultiWithoutTx #2, err=datastore: no such entity
		process [3, 5) range keys
		after: GetMultiWithoutTx #3, len(keys)=2, keys=[/Data,4, /Data,5]
		after: GetMultiWithoutTx #3, err=datastore: no such entity
		before: GetMultiWithoutTx #2, err=datastore: no such entity (and 1 other error)
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}
