/*
Package boom handles the troublesome processing of datastore.Key automatically.
It calculates the Kind, ID, and Name from the name of struct and tags.


Key handling

boom beautifully maps keys and objects, making it easier to write code.
It is easier to collect information on one object than to treat Key and Entity separately.
Handling is very easy if Key has only ID or Name.
It just adds boom:"id" to the tag, as in the following code:

	type Post struct {
		ID		int64 `datastore:"-" boom:"id"`
		Content	string
	}

When Put the value of ID field is uses as the ID of Key, and when Get ID value of Key is set in ID field.
This allows you to break code to think or write about Key in your program.

Kind of Key is also calculated automatically.
By default, the name of the passed struct is the Kind name.
In the example of the previous code, 'Post' becomes Kind.
If you want to explicitly specify, boom:"kind" is given to the tag.

In the case of the following code, the Kind name will be 'pay' if there is no value in the Kind field.
If you have some value in the Kind field, that value will be the Kind name.

	type Payment struct {
		Kind	string	`datastore:"-" boom:"kind,pay"`
		ID		int64	`datastore:"-" boom:"id"`
		Amount	int
	}

As for ParentKey, there is also a means to ease it.
boom:"parent" is given to the tag, field value is used as ParentKey.


For goon user

boom has a considerable API compatibility with goom.

There is a difference in behavior when Put under transaction.
Cloud Datastore does not assign the ID immediately before the Commit.
go.mercari.io/datastore following the Cloud Datastore specification will behave similarly even if the back end is AppEngine Datastore.
Therefore, if you need to use ID before Commit after Putting, you need to call AllocateID yourself beforehand.

Also, boom does not have any mechanism about the cache.
Because, it should be done with middleware on go.mercari.io/datastore.
Simple and nice, do not you?
*/
package boom // import "go.mercari.io/datastore/boom"
