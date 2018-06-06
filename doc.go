/*
Package datastore has an abstract representation of (AppEngine | Cloud) Datastore.

repository https://github.com/mercari/datastore

Let's read https://cloud.google.com/datastore/docs/ or https://cloud.google.com/appengine/docs/standard/go/datastore/ .
You should also check https://godoc.org/cloud.google.com/go/datastore or https://godoc.org/google.golang.org/appengine/datastore as datastore original library.

Japanese version https://github.com/mercari/datastore/blob/master/doc_ja.go


Basic usage

Please see https://godoc.org/go.mercari.io/datastore/clouddatastore or https://godoc.org/go.mercari.io/datastore/aedatastore .
Create a Client using the FromContext function of each package.

Later in this document, notes on migration from each package are summarized.
Please see also there.

This package is based on the newly designed Cloud Datastore API.
We are introducing flatten tags that only exist in Cloud Datastore, we need to be careful when migrating from AE Datastore.
Details will be described later.
If you are worried, you may have a clue to the solution at https://godoc.org/go.mercari.io/datastore/clouddatastore .


The purpose of this package

This package has three main objectives.

	1. Provide a middleware layer, and reduce the code that are not directly related to application value.
	2. AppEngine, Cloud, to provide the same interface for both Datastore.
	3. Enable batch processing for Single Get, Signle Put, etc.


Middleware layer

We are forced to make functions that are not directly related to the value of the application for speed, stability and operation.
Such functions can be abstracted and used as middleware.

Let's think about this case.
Put Entity to Datastore and set it to Memcache or Redis.
Next, when getting from Datastore, Get from Memcache first, Get it again from Datastore if it fails.
It is very troublesome to provide these operations for all Kind and all Entity operations.
However, if the middleware intervenes with all Datastore RPCs, you can transparently process without affecting the application code.

As another case, RPC sometimes fails.
If it fails, the process often succeeds simply by retrying.
For easy RET retry with all RPCs, it is better to implement it as middleware.

Please refer to https://godoc.org/go.mercari.io/datastore/dsmiddleware if you want to know the middleware already provided.


Provide the same interface between AppEngine and Cloud Datastore

The same interface is provided for AppEngine Datastore and Cloud Datastore.
These two are compatible, you can run it with exactly the same code after creating the Client.

For example, you can use AE Datastore in a production environment and Cloud Datastore Emulator in UnitTest.
If you can avoid goapp, tests may be faster and IDE may be more vulnerable to debugging.
You can also read data from the local environment via Cloud Datastore for systems running on AE Datastore.

Caution.
Although the storage bodies of RPCs of AE Datastore and Cloud Datastore are shared, there is a difference in expressiveness at the API level.
Please carefully read the data written in AE Datastore carelessly on Cloud Datastore and do not update it.
It may become impossible to read from the API of AE Datastore side.
About this, we have not strictly tested.


Batch processing

The operation of Datastore has very little latency with respect to RPC's network.
When acquiring 10 entities it means that GetMulti one time is better than getting 10 times using loops.
However, we are not good at putting together multiple processes at once.
Suppose, for example, you want to query on Post Kind, use the list of Comment IDs of the resulting Post, and get a list of Comments.
For example, you can query Post Kind and get a list of Post.
In addition, consider using CommentIDs of Post and getting a list of Comment.
This is enough Query + 1 GetMulti is enough if you write very clever code.
However, after acquiring the data, it is necessary to link the Comment list with the appropriate Post.
On the other hand, you can easily write a code that throws a query once and then GetMulti the Comment as many as Post.
In summary, it is convenient to have Put or Get queued, and there is a mechanism to execute it collectively later.

Batch() is it!
You can find the example at https://godoc.org/go.mercari.io/datastore/#pkg-examples .


Boom replacing goon

I love goon.
So I made https://godoc.org/go.mercari.io/datastore/boom which can be used in conjunction with this package.


How to migrate to this library

Here's an overview of what you need to do to migrate your existing code.

	replace *datastore.Key to datastore.Key.
	replace *datastore.Query to datastore.Query.
	replace *datastore.Iterator to datastore.Iterator.

from AE Datastore

	import go.mercari.io/datastore and go.mercari.io/datastore/aedatastore both.
	rewrite those using functions of datastore package to FromContext function and Client method calls.
	replace err.(appengine.MultiError) to err.(datastore.MultiError) .
	Stop using appengine.BlobKey and replace with string.
	replace google.golang.org/appengine/datastore.Done to google.golang.org/api/iterator.Done .
	replace key.IntID() to key.ID() .
	replace key.StringID() to key.Name() .
	When nesting a struct, apply `datastore:", flatten "` to the corresponding field.
	Delete datastore.TransactionOptions, it is not supported.
	If using google.golang.org/appengine/datastore , replace to go.mercari.io/datastore .

from Cloud Datastore

	import go.mercari.io/datastore and go.mercari.io/datastore/clouddatastore .
	rewrite those using functions of datastore package to FromContext function and Client method calls.
	replace *datastore.Commit to datastore.Commit .
	If using cloud.google.com/go/datastore , replace to go.mercari.io/datastore .

from goon to boom

	replace *goon.Goon to *boom.Boom .
	replace goon.FromContext(ctx) to ds, _ := aedatastore.FromContext(ctx); boom.FromClient(ctx, ds) .
*/
package datastore // import "go.mercari.io/datastore"
