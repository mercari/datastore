package datastore

import (
	"encoding/gob"
	"time"
)

// LoadEntity to dst struct.
var LoadEntity = loadEntity

// SaveEntity convert key & struct to *Entity.
var SaveEntity = saveEntity

func init() {
	gob.Register(time.Time{})
	gob.Register(&Entity{})
	gob.Register(GeoPoint{})
	gob.Register([]interface{}{})
}
