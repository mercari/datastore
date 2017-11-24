package dslog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/MakeNowJust/heredoc"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/testutils"
)

func TestDsLog_Basic(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	var logs []string
	logf := func(ctx context.Context, format string, args ...interface{}) {
		t.Logf(format, args...)
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	logger := NewLogger("log: ", logf)

	client.AppendCacheStrategy(logger)
	defer func() {
		// stop logging before cleanUp func called.
		client.RemoveCacheStrategy(logger)
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

	expected := heredoc.Doc(`
		log: PutMultiWithoutTx #1, len(keys)=1, keys=[/Data,111]
		log: PutMultiWithoutTx #1, keys=[/Data,111]
		log: DeleteMultiWithoutTx #2, len(keys)=1, keys=[/Data,111]
		log: GetMultiWithoutTx #3, len(keys)=1, keys=[/Data,111]
		log: GetMultiWithoutTx #3, err=datastore: no such entity
	`)

	if v := strings.Join(logs, "\n") + "\n"; v != expected {
		t.Errorf("unexpected: %v", v)
	}
}
