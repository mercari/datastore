/*
Package fishbone automatically rewrites the behavior based on KeysOnly + Get by Key when Run or GetAll Query, contributing to reducing the amount of charge.
If you use Run or GetAll with Query, you will be charged for Small Operations + Entity Reads as you retrieve all Entities from Datastore.
We decompose this automatically, set it to KeysOnly and get Entity from cache in Run or GetAll method.

Why fishbone?

https://www.google.co.jp/search?q=%E9%AD%9A%E3%81%AE%E9%A3%9F%E3%81%B9%E6%96%B9+%E8%83%8C%E9%AA%A8&tbm=isch

Recommend: don't use this middleware in production.
You should implement KeysOnly + GetMulti strategy in YOUR application.
Because, SingleGet is executed every time when Iterator#Next called. It's too slow even memcache access.
*/
package fishbone // import "go.mercari.io/datastore/dsmiddleware/fishbone"

// TODO アプリケーション側で実装する場合のベストプラクティス的コードをどこかに作成してリンクを置く
