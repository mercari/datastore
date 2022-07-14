package boom_test

import (
	"context"
	"fmt"

	"go.mercari.io/datastore/v2"
	"go.mercari.io/datastore/v2/aedatastore"
	"go.mercari.io/datastore/v2/boom"
	"go.mercari.io/datastore/v2/clouddatastore"
	"go.mercari.io/datastore/v2/dsmiddleware/aememcache"
	"go.mercari.io/datastore/v2/dsmiddleware/localcache"
	"go.mercari.io/datastore/v2/internal/testutils"
	"google.golang.org/appengine/v2"
	"google.golang.org/appengine/v2/aetest"
)

const ProjectID = "datastore-wrapper"

func appengineContext() (ctx context.Context, cancelFn func() error) {
	inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true, SuppressDevAppServerLog: true})
	if err != nil {
		panic(err)
	}
	cancelFn = inst.Close
	r, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		panic(err)
	}
	ctx = appengine.NewContext(r)

	return
}

func ExampleBoom() {
	ctx := context.Background()
	// of-course, you can use aedatastore instead of clouddatastore!
	client, err := clouddatastore.FromContext(
		ctx,
		datastore.WithProjectID(ProjectID),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	defer testutils.CleanUpAllEntities(ctx, client)

	bm := boom.FromClient(ctx, client)

	type Data struct {
		ID   int64 `datastore:"-" boom:"id"`
		Name string
	}

	key, err := bm.Put(&Data{Name: "mercari"})
	if err != nil {
		panic(err)
	}
	if key.ID() == 0 {
		panic("unexpected state")
	}

	obj := &Data{ID: key.ID()}
	err = bm.Get(obj)
	if err != nil {
		panic(err)
	}

	fmt.Println(obj.Name)

	// Output: mercari
}

func ExampleBoom_kind() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	defer testutils.CleanUpAllEntities(ctx, client)

	bm := boom.FromClient(ctx, client)

	type Payment struct {
		Kind   string `datastore:"-" boom:"kind,pay"`
		ID     int64  `datastore:"-" boom:"id"`
		Amount int
	}

	key, err := bm.Put(&Payment{
		Amount: 100,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(key.Kind())

	key, err = bm.Put(&Payment{
		Kind:   "支払い",
		Amount: 100,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(key.Kind())

	// Output: pay
	// 支払い
}

func ExampleBoom_parent() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	defer testutils.CleanUpAllEntities(ctx, client)

	bm := boom.FromClient(ctx, client)

	type Post struct {
		ID      string `datastore:"-" boom:"id"`
		Content string
	}

	type Comment struct {
		ParentKey datastore.Key `datastore:"-" boom:"parent"`
		ID        int64         `datastore:"-" boom:"id"`
		Message   string
	}

	key, err := bm.Put(&Post{
		ID:      "foobar",
		Content: "post!",
	})
	if err != nil {
		panic(err)
	}

	key, err = bm.Put(&Comment{
		ParentKey: key,
		ID:        1,
		Message:   "comment!",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(key.String())

	// Output: /Post,foobar/Comment,1
}

func ExampleBoom_withCache() {
	ctx, cancelFn := appengineContext()
	go cancelFn()

	client, err := aedatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// add cache layer likes goon!
	client.AppendMiddleware(localcache.New(
		localcache.WithLogger(func(ctx context.Context, format string, args ...interface{}) {
			fmt.Println(fmt.Sprintf(format, args...))
		}),
	))
	client.AppendMiddleware(aememcache.New(
		aememcache.WithLogger(func(ctx context.Context, format string, args ...interface{}) {
			fmt.Println(fmt.Sprintf(format, args...))
		}),
	))

	bm := boom.FromClient(ctx, client)

	type Data struct {
		ID  string `datastore:"-" boom:"id"`
		Str string
	}

	_, err = bm.Put(&Data{
		ID:  "test",
		Str: "foobar",
	})
	if err != nil {
		panic(err)
	}

	err = bm.Get(&Data{ID: "test"})
	if err != nil {
		panic(err)
	}

	// Output: dsmiddleware/aememcache.SetMulti: incoming len=1
	// dsmiddleware/aememcache.SetMulti: len=1
	// dsmiddleware/localcache.SetMulti: len=1
	// dsmiddleware/localcache.SetMulti: idx=0 key=/Data,test len(ps)=1
	// dsmiddleware/localcache.GetMulti: len=1
	// dsmiddleware/localcache.GetMulti: idx=0 key=/Data,test
	// dsmiddleware/localcache.GetMulti: idx=0, hit key=/Data,test len(ps)=1
}
