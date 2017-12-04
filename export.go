package datastore

import "encoding/gob"

// TODO hide LoadEntity project outside
var LoadEntity = loadEntity
var SaveEntity = saveEntity

func init() {
	gob.Register(&Entity{})
	gob.Register(GeoPoint{})
}
