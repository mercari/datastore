package clouddatastore_test

import (
	"context"
	"fmt"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
	"go.mercari.io/datastore/internal/testutils"
)

const ProjectID = "datastore-wrapper"

func ExampleFromContext() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(
		ctx,
		datastore.WithProjectID(ProjectID),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	defer testutils.CleanUpAllEntities(ctx, client)

	type Data struct {
		Name string
	}

	key := client.IncompleteKey("Data", nil)
	entity := &Data{Name: "mercari"}
	key, err = client.Put(ctx, key, entity)
	if err != nil {
		panic(err)
	}

	entity = &Data{}
	err = client.Get(ctx, key, entity)
	if err != nil {
		panic(err)
	}

	fmt.Println(entity.Name)
	// Output: mercari
}
