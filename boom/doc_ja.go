package boom

/*
Package boomは、面倒なdatastore.Keyに関する処理を自動的に処理してくれます。
Kind名やID, Nameをstructの名前やタグから計算してくれます。


Keyのハンドリング

boomはKeyとオブジェクトを綺麗にマッピングし、コードを書くのを楽にします。
KeyとEntityとなるオブジェクトをバラバラに扱うのは大変なので、オブジェクトにKeyの情報を持たせます。
KeyがIDかNameのみを持つ場合、扱いは非常に簡単です。
次のコードのように、 boom:"id" をタグに付与するだけです。

	type Post struct {
		ID		int64 `datastore:"-" boom:"id"`
		Content	string
	}

これで、Putする時はIDフィールドの値がKeyのIDとして、Getする時はKeyのIDの値がIDフィールドにセットされます。
これにより、プログラム中でKeyの面倒をみる必要はなくなります。

KeyのKindについても、自動的に計算されます。
デフォルトでは渡したstructの名前がKind名になります。
さっきのコードの例だと Post がKindになります。
明示的に指定したい場合、 boom:"kind" をタグに付与します。
次のコードの場合、Kind名はKindフィールドに何も値を入れていない場合、 pay になります。
Kindフィールドになにか値を入れていた場合は、その値がKind名になります。

	type Payment struct {
		Kind	string	`datastore:"-" boom:"kind,pay"`
		ID		int64	`datastore:"-" boom:"id"`
		Amount	int
	}

ParentKeyについても、楽をする手段を用意してあります。
boom:"parent" をタグに付与すると、フィールドのKeyがParentKeyとして利用されます。


For goon user

boomはgoomとかなりのAPIの互換性があります。

トランザクション下でのPut時の振る舞いに差があります。
Cloud DatastoreではPutしてもCommitされるまでKeyのIDが採番されません。
Cloud Datastoreの仕様を踏襲している go.mercari.io/datastore では、バックエンドがAppEngine Datastoreであってもこれと同様の振る舞いをします。
よって、Putした後、Commitより前にIDを使う必要があれば、事前に自分でAllocateIDをしておく必要があります。

また、boomはキャッシュ周りの仕組みを何も持っていません。
それは、 go.mercari.io/datastore が持つミドルウェアで行うべきものだからです。
シンプルでいいでしょ？
*/
