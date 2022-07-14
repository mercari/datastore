package testutilsv1

import (
	"context"
	"testing"

	testeratorv2 "github.com/favclip/testerator/v2"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/aedatastore"
)

// SetupAEDatastore returns AEDatastore clients and function for cleaning.
func SetupAEDatastore(t *testing.T) (context.Context, datastore.Client, func()) {
	_, ctx, err := testeratorv2.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}

	client, err := aedatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return ctx, client, func() { testeratorv2.SpinDown() }
}
