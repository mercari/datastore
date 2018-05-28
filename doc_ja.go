package datastore // import "go.mercari.io/datastore"

/*
Package datastore は、(AppEngine|Cloud) Datastoreの抽象表現を持ちます。
https://cloud.google.com/datastore/docs/ もしくは https://cloud.google.com/appengine/docs/standard/go/datastore/ をよく読みましょう。
また、オリジナルのライブラリとして https://godoc.org/cloud.google.com/go/datastore もしくは https://godoc.org/google.golang.org/appengine/datastore も確認すると良いでしょう。


基本的な使い方

https://godoc.org/go.mercari.io/datastore/clouddatastore か https://godoc.org/go.mercari.io/datastore/aedatastore を見てください。
各パッケージのFromContextを使って Client を作成します。

このドキュメントの後半に、各パッケージから移行する際の注意点をまとめてあります。
そちらも御覧ください。

このライブラリはより設計が新しいCloud DatastoreのAPIをベースにしています。
Cloud Datastoreにしかない flatten タグも導入しているので、AE Datastoreから移行する際は注意が必要です。
詳細は後述します。
困ったら https://godoc.org/go.mercari.io/datastore/clouddatastore を見ると解決の糸口があるかもしれません。


本パッケージの目的は3つあります。
	1. ミドルウェア層を提供し、アプリケーションの価値とは直接関係がない処理を書く必要を減らす
	2. AppEngine, Cloud、両方のDatastoreに対して同一のインタフェースを提供する。
	3. Single Get, Signle Putなどをバッチ処理化する。


ミドルウェア層

アプリケーションの価値とは直接関係のない、速度や安定性、運用のための機能が必要になる場合があります。
そういった機能はミドルウェアとして抽象化し、利用することができます。

例えばこんなケースはどうでしょうか。
DatastoreにEntityをPutしたら、MemcacheやRedisにSetしておく。
次にDatastoreからGetするときはまずMemcacheなどからGetし、無ければDatastoreから改めてGetする。
これらをすべてのKind、すべてのEntityの操作で行うのは、ひどく面倒です。
しかし、全てのDatastoreとのRPCに介入し、統一的に処理を差し込めるミドルウェアであれば、アプリケーションから見えないところでこの処理を行うことができます。

別のケースとして、RPCにありがちなこととして処理が稀に失敗することがあります。
失敗したら単にリトライするだけで処理が成功する場合も多いです。
これも、全ての処理で簡単に行うには、ミドルウェアでリトライ処理をかけるのが適しています。

既に用意されているミドルウェアが知りたい場合は https://godoc.org/go.mercari.io/datastore/dsmiddleware を参照してください。


AppEngineとCloudで同一のインタフェースを提供する

AppEngine DatastoreとCloud Datastoreに対して、同一のインタフェースが提供されています。
この2つは互換性があり、Clientを作った後は全く同様のコードで動かすことができます。

例えば本番環境ではAE Datastoreを使って、UnitTestではCloud Datastore Emulatorを使うということもできます。
goappを避けることができれば、テストが高速になったりIDEからデバッグの支援が受けやすくなったりするかもしれません。
また、AE Datastoreで運用しているシステムについて、ローカル環境からCloud Datastoreを介してデータを読み込むこともできるでしょう。

注意点として、AE DatastoreとCloud DatastoreのRPCのストレージ本体は共有されていますが、APIレベルでの表現力には差があります。
うかつにAE Datastoreで書いたデータをCloud Datastoreで読んで、変更して、更新しないようにしてください。
AE Datastore側のAPIから読み取れなくなる可能性があります。
これについて、我々は厳密にはテストを行っていません。


Signle Get, Signle Putのバッチ処理化

Datastoreの操作にはRPCのためのネットワークに関するレイテンシがほんの少しあります。
10個のEntityを取得するとき、ループして10回Getするよりも1回のGetMultiのほうがより良いということです。
ところが、我々は複数の処理を1回にまとめるのが苦手です。
例えば、Post KindにQueryを投げて、得られたPostが持っているComment IDのリストを使ってCommentのリストをGetしたいとします。
これは、ちゃんとしたコードを書けば1回のQuery+1回のGetMultiで十分ですが、Commentのリストを適切なPostと紐付ける作業が待っています。
一方、1回のQuery+Postの数だけCommentをGetMultiするコードは簡単に書けるでしょう。
ここで、PutやGetをキューに入れておいて、あとでまとめて実行してくれる仕組みがあると都合がよさそうです。

これを実現したのが Batch() です。
https://godoc.org/go.mercari.io/datastore/#pkg-examples に例があります。


goonを置き換えるboom

私はgoonが好きです。
ですので、本ライブラリと組み合わせて使える https://godoc.org/go.mercari.io/datastore/boom を作りました。


本ライブラリへの移行方法（AE, Cloud共通）

	*datastore.Key を datastore.Key に置き換える。
	*datastore.Query を datastore.Query に置き換える。
	*datastore.Iterator を datastore.Iterator に置き換える。

AE Datastoreからの移行

	go.mercari.io/datastore と go.mercari.io/datastore/aedatastore をimportする。
	datastoreパッケージの関数を使っているものをFromContextとClientのメソッド呼び出しに書き換える。
	err.(appengine.MultiError) を err.(datastore.MultiError) に置き換える。
	appengine.BlobKey を使うのをやめ、stringに置き換える。
	google.golang.org/appengine/datastore.Done を google.golang.org/api/iterator.Done に置き換える。
	key.IntID() を key.ID() に置き換える。
	key.StringID() を key.Name() に置き換える。
	structをネストさせている場合、該当フィールドに `datastore:",flatten"` を適用する。
	datastore.TransactionOptions はサポートされないので削除する。
	google.golang.org/appengine/datastore をimportしている箇所がないかチェックし、あれば go.mercari.io/datastore に置き換える。

Cloud Datastoreからの移行

	go.mercari.io/datastore と go.mercari.io/datastore/clouddatastore をimportする。
	datastoreパッケージの関数を使っているものをFromContextとClientのメソッド呼び出しに書き換える。
	*datastore.Commit を datastore.Commit に置き換える。
	cloud.google.com/go/datastore をimportしている箇所がないかチェックし、あれば go.mercari.io/datastore に置き換える。

goonからboomへの移行

	*goon.Goon を *boom.Boom に置き換える。
	goon.FromContext(ctx) を ds, _ := aedatastore.FromContext(ctx); boom.FromClient(ctx, ds) に置き換える。
*/
