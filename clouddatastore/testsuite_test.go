package clouddatastore

import (
	"context"
	"testing"

	"go.mercari.io/datastore/testsuite"
	_ "go.mercari.io/datastore/testsuite/favcliptools"
	_ "go.mercari.io/datastore/testsuite/realworld/tbf"
	"google.golang.org/api/iterator"
)

func cleanUp() error {
	ctx := context.Background()
	client, err := FromContext(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	q := client.NewQuery("__kind__").KeysOnly()
	iter := client.Run(ctx, q)
	var kinds []string
	for {
		key, err := iter.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		kinds = append(kinds, key.Name())
	}

	for _, kind := range kinds {
		q := client.NewQuery(kind).KeysOnly()
		keys, err := client.GetAll(ctx, q, nil)
		if err != nil {
			return err
		}
		err = client.DeleteMulti(ctx, keys)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestCloudDatastoreTestSuite(t *testing.T) {
	ctx := context.Background()
	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			defer cleanUp()

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			ctx = testsuite.WrapCloudFlag(ctx)
			test(t, ctx, datastore)
		})
	}
}
