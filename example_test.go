package datastore

import (
	"context"
	"fmt"
)

func Example_clientGet() {
	ctx := context.Background()
	client, err := FromContext(ctx)
	if err != nil {
		panic(err)
	}

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
}
