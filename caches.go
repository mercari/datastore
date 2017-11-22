package datastore

import "context"

type CacheOperation int

type CacheStrategy interface {
	PutMulti(info *CacheInfo, keys []Key, psList []PropertyList) ([]Key, []PropertyList, error)
	GetMulti(info *CacheInfo, keys []Key) ([]Key, []PropertyList, error)
	DeleteMulti(info *CacheInfo, keys []Key) error
	GetAll(info *CacheInfo, q Query, qDump *QueryDump, psList []PropertyList) ([]Key, []PropertyList, error)
	Next(info *CacheInfo, q Query, qDump *QueryDump, ps PropertyList) (Key, PropertyList, error)
}

type CacheInfo struct {
	Context     context.Context
	Client      Client
	Transaction Transaction
	Next        CacheStrategy
}

type QueryDump struct {
	Ancestor            Key
	EventualConsistency bool
	Namespace           string
	Transaction         Transaction
	Filter              []*QueryFilterCondition
	Order               []string
	Project             []string
	Distinct            bool
	KeysOnly            bool
	Limit               int
	Offset              int
	Start               Cursor
	End                 Cursor
}

type QueryFilterCondition struct {
	Filter string
	Value  interface{}
}
