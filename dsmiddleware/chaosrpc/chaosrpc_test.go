package chaosrpc

import (
	"math/rand"
	"testing"

	"go.mercari.io/datastore/internal/testutils"
)

func TestChaosRPC_CheckRaiseError(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		Name string
	}

	// Put.
	key := client.IDKey("Data", 111, nil)
	objBefore := &Data{Name: "Data"}
	_, err := client.Put(ctx, key, objBefore)
	if err != nil {
		t.Fatal(err)
	}

	ch := New(rand.NewSource(100))
	client.AppendMiddleware(ch)
	defer func() {
		// stop chaos before cleanUp func called.
		client.RemoveMiddleware(ch)
	}()

	// Get.
	catchErr := false
	for i := 0; i < 100; i++ {
		objAfter := &Data{}
		err = client.Get(ctx, key, objAfter)
		if err != nil {
			t.Logf("#%d catch err=%s", i+1, err.Error())
			catchErr = true
		}
	}
	if !catchErr {
		t.Errorf("unexpected: %v", catchErr)
	}
}
