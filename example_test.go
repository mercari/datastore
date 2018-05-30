package datastore_test

import (
	"context"
	"fmt"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
)

func Example_clientGet() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
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
	// Output: mercari
}

func Example_batch() {
	ctx := context.Background()
	cli, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	type Comment struct {
		Message string
	}
	type Post struct {
		Content    string
		CommentIDs []int64    `json:"-"`
		Comments   []*Comment `datastore:"-"`
	}

	// preparing entities
	for i := 0; i < 4; i++ {
		post := &Post{Content: fmt.Sprintf("post #%d", i+1)}
		key, err := cli.Put(ctx, cli.IncompleteKey("Post", nil), post)
		if err != nil {
			panic(err)
		}

		for j := 0; j < 5; j++ {
			comment := &Comment{Message: fmt.Sprintf("comment #%d", j+1)}
			cKey, err := cli.Put(ctx, cli.IncompleteKey("Comment", nil), comment)
			if err != nil {
				panic(err)
			}

			post.CommentIDs = append(post.CommentIDs, cKey.ID())
		}
		_, err = cli.Put(ctx, key, post)
		if err != nil {
			panic(err)
		}
	}

	// start fetching...
	posts := make([]*Post, 0)
	_, err = cli.GetAll(ctx, cli.NewQuery("Post").Order("Content"), &posts)
	if err != nil {
		panic(err)
	}

	// Let's batch get!
	bt := cli.Batch()

	for _, post := range posts {
		comments := make([]*Comment, 0)
		for _, id := range post.CommentIDs {
			comment := &Comment{}
			bt.Get(cli.IDKey("Comment", id, nil), comment, nil)
			comments = append(comments, comment)
		}
		post.Comments = comments
	}

	err = bt.Exec(ctx)
	if err != nil {
		panic(err)
	}

	// check result
	for _, post := range posts {
		fmt.Println("Post", post.Content)
		for _, comment := range post.Comments {
			fmt.Println("Comment", comment.Message)
		}
	}

	// Output:
	// Post post #1
	// Comment comment #1
	// Comment comment #2
	// Comment comment #3
	// Comment comment #4
	// Comment comment #5
	// Post post #2
	// Comment comment #1
	// Comment comment #2
	// Comment comment #3
	// Comment comment #4
	// Comment comment #5
	// Post post #3
	// Comment comment #1
	// Comment comment #2
	// Comment comment #3
	// Comment comment #4
	// Comment comment #5
	// Post post #4
	// Comment comment #1
	// Comment comment #2
	// Comment comment #3
	// Comment comment #4
	// Comment comment #5
}

func Example_batchWithBatchErrHandler() {
	ctx := context.Background()
	cli, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	type Comment struct {
		Message string
	}

	// preparing entities...
	// Put ID: 2, 4 into Datastore.
	var keys []datastore.Key
	for i := 1; i <= 5; i++ {
		key := cli.IDKey("Comment", int64(i), nil)
		keys = append(keys, key)

		comment := &Comment{Message: fmt.Sprintf("comment #%d", i)}
		if i%2 == 0 {
			_, err = cli.Put(ctx, key, comment)
			if err != nil {
				panic(err)
			}
		}
	}

	// Let's batch get!
	bt := cli.Batch()

	var comments []*Comment
	for _, key := range keys {
		comment := &Comment{}

		bt.Get(key, comment, func(err error) error {
			if err == datastore.ErrNoSuchEntity {
				// ignore ErrNoSuchEntity
				return nil
			} else if err != nil {
				return err
			}

			comments = append(comments, comment)

			return nil
		})
	}

	err = bt.Exec(ctx)
	if err != nil {
		panic(err)
	}

	// check result
	for _, comment := range comments {
		fmt.Println("Comment", comment.Message)
	}

	// Output:
	// Comment comment #2
	// Comment comment #4
}

var _ datastore.PropertyTranslator = UserID(0)
var _ datastore.PropertyTranslator = UserIDs(nil)

type UserID int64
type UserIDs []UserID

type contextClient struct{}

func (id UserID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	// When putting to Datastore, convert to datastore.Key
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey("User", int64(id), nil)
	return key, nil
}

func (id UserID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	// When getting from Datastore, convert from datastore.Key
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return UserID(key.ID()), nil
}

func (ids UserIDs) ToPropertyValue(ctx context.Context) (interface{}, error) {
	// When putting to Datastore, convert to []datastore.Key
	client := ctx.Value(contextClient{}).(datastore.Client)
	keys := make([]datastore.Key, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, client.IDKey("User", int64(id), nil))
	}
	return keys, nil
}

func (ids UserIDs) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	// When getting from Datastore, convert from datastore.Key
	keys, ok := p.Value.([]datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	newIDs := make(UserIDs, 0, len(keys))
	for _, key := range keys {
		newIDs = append(ids, UserID(key.ID()))
	}
	return newIDs, nil
}

func ExamplePropertyTranslator() {
	ctx := context.Background()
	cli, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	ctx = context.WithValue(ctx, contextClient{}, cli)

	// Each fields are saved as datastore.Key and [] datastore.Key on Datastore.
	type Group struct {
		OwnerID   UserID
		MemberIDs UserIDs
	}

	entity, err := datastore.SaveEntity(
		ctx, cli.IncompleteKey("Group", nil),
		&Group{
			OwnerID:   1,
			MemberIDs: UserIDs{1, 2, 3},
		},
	)
	if err != nil {
		panic(err)
	}

	if key, ok := entity.Properties[0].Value.(datastore.Key); !ok {
		panic("unexpected state")
	} else {
		fmt.Println("OwnerID", key.ID())
	}
	if keys, ok := entity.Properties[1].Value.([]datastore.Key); !ok {
		panic("unexpected state")
	} else {
		for _, key := range keys {
			fmt.Println("MemberID", key.ID())
		}
	}

	// Output: OwnerID 1
	// MemberID 1
	// MemberID 2
	// MemberID 3
}
