package testutils

import (
	"context"
	"strings"
	"testing"

	"github.com/favclip/testerator"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/aedatastore"
	"go.mercari.io/datastore/clouddatastore"
)

var EmitCleanUpLog = false

func SetupCloudDatastore(t *testing.T) (context.Context, datastore.Client, func()) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return ctx, client, func() {
		defer client.Close()

		q := client.NewQuery("__kind__").KeysOnly()
		keys, err := client.GetAll(ctx, q, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) == 0 {
			return
		}

		kinds := make([]string, 0, len(keys))
		for _, key := range keys {
			kinds = append(kinds, key.Name())
		}

		if EmitCleanUpLog {
			t.Logf("remove %s", strings.Join(kinds, ", "))
		}

		for _, kind := range kinds {

			cnt := 0
			for {
				q := client.NewQuery(kind).Limit(1000).KeysOnly()
				keys, err := client.GetAll(ctx, q, nil)
				if err != nil {
					t.Fatal(err)
				}
				err = client.DeleteMulti(ctx, keys)
				if err != nil {
					t.Fatal(err)
				}

				cnt += len(keys)

				if len(keys) != 1000 {
					if EmitCleanUpLog {
						t.Logf("remove %s entity: %d", kind, cnt)
					}
					break
				}
			}
		}
	}
}

func SetupAEDatastore(t *testing.T) (context.Context, datastore.Client, func()) {
	_, ctx, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}

	client, err := aedatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return ctx, client, func() { testerator.SpinDown() }
}
