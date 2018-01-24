package datastore

import (
	"encoding/gob"
	"time"
)

var LoadEntity = loadEntity
var SaveEntity = saveEntity

func init() {
	gob.Register(time.Time{})
	gob.Register(&Entity{})
	gob.Register(GeoPoint{})
	gob.Register([]interface{}{})
}
