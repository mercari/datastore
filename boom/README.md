# boom

BOOM HEADSHOT!

[goon](https://github.com/mjibson/goon) likes interface for [go.mercari.io/datastore](https://github.com/mercari/datastore).

boom doesn't have cache layer.
It will be coming to `go.mercari.io/datastore` package.

boom doing only `struct → key` and `key → struct` mapping.

## Important Notice

There are incompatible behaviors in the following points.

* `*boom.Transaction#Put` will not set ID immediate.
    * It will be set after `boom.Transaction#Commit`.
    * If you want to get ID/Name asap, You should use `datastore.Client#AllocateIDs`.

## TODO

* namespace
