package datastore

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Middleware hooks to the Datastore's RPC and It can modify arguments and return values.
// see https://godoc.org/go.mercari.io/datastore/dsmiddleware
type Middleware interface {
	// AllocateIDs intercepts AllocateIDs operation.
	AllocateIDs(info *MiddlewareInfo, keys []Key) ([]Key, error)
	// PutMultiWithoutTx intercepts PutMulti without Transaction operation.
	PutMultiWithoutTx(info *MiddlewareInfo, keys []Key, psList []PropertyList) ([]Key, error)
	// PutMultiWithTx intercepts PutMulti with Transaction operation.
	PutMultiWithTx(info *MiddlewareInfo, keys []Key, psList []PropertyList) ([]PendingKey, error)
	// GetMultiWithoutTx intercepts GetMulti without Transaction operation.
	GetMultiWithoutTx(info *MiddlewareInfo, keys []Key, psList []PropertyList) error
	// GetMultiWithTx intercepts GetMulti with Transaction operation.
	GetMultiWithTx(info *MiddlewareInfo, keys []Key, psList []PropertyList) error
	// DeleteMultiWithoutTx intercepts DeleteMulti without Transaction operation.
	DeleteMultiWithoutTx(info *MiddlewareInfo, keys []Key) error
	// DeleteMultiWithTx intercepts DeleteMulti with Transaction operation.
	DeleteMultiWithTx(info *MiddlewareInfo, keys []Key) error
	// PostCommit will kicked after Transaction commit.
	PostCommit(info *MiddlewareInfo, tx Transaction, commit Commit) error
	// PostRollback will kicked after Transaction rollback.
	PostRollback(info *MiddlewareInfo, tx Transaction) error
	// Run intercepts Run query operation.
	Run(info *MiddlewareInfo, q Query, qDump *QueryDump) Iterator
	// GetAll intercepts GetAll operation.
	GetAll(info *MiddlewareInfo, q Query, qDump *QueryDump, psList *[]PropertyList) ([]Key, error)
	// Next intercepts Next operation.
	Next(info *MiddlewareInfo, q Query, qDump *QueryDump, iter Iterator, ps *PropertyList) (Key, error)
	// Count intercepts Count operation.
	Count(info *MiddlewareInfo, q Query, qDump *QueryDump) (int, error)
}

// MiddlewareInfo provides RPC's processing state.
type MiddlewareInfo struct {
	Context     context.Context
	Client      Client
	Transaction Transaction
	Next        Middleware
}

// QueryDump provides information of executed query.
type QueryDump struct {
	Kind                string
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

func (dump *QueryDump) String() string {
	// generate keys that are unique for queries
	// TODO ProjectID...?

	b := bytes.NewBufferString("v1:") // encoding format version
	b.WriteString(dump.Kind)

	if dump.Ancestor != nil {
		b.WriteString("&a=")
		b.WriteString(dump.Ancestor.String())
	}

	if dump.EventualConsistency {
		b.WriteString("&e=t")
	}

	if dump.Namespace != "" {
		b.WriteString("&n=")
		b.WriteString(dump.Namespace)
	}

	if dump.Transaction != nil {
		b.WriteString("&t=t")
	}

	if l := len(dump.Filter); l != 0 {
		b.WriteString("&f=")
		for idx, f := range dump.Filter {
			b.WriteString(f.Filter)
			b.WriteString(fmt.Sprintf("%+v", f.Value))
			if (idx + 1) != l {
				b.WriteString("|")
			}
		}
	}
	if l := len(dump.Order); l != 0 {
		b.WriteString("&or=")
		b.WriteString(strings.Join(dump.Order, "|"))
	}
	if l := len(dump.Project); l != 0 {
		b.WriteString("&p=")
		b.WriteString(strings.Join(dump.Project, "|"))
	}
	if dump.Distinct {
		b.WriteString("&d=t")
	}
	if dump.KeysOnly {
		b.WriteString("&k=t")
	}
	if dump.Limit != 0 {
		b.WriteString("&l=")
		b.WriteString(strconv.Itoa(dump.Limit))
	}
	if dump.Offset != 0 {
		b.WriteString("&o=")
		b.WriteString(strconv.Itoa(dump.Offset))
	}
	if dump.Start != nil {
		b.WriteString("&s=")
		b.WriteString(dump.Start.String())
	}
	if dump.End != nil {
		b.WriteString("&e=")
		b.WriteString(dump.End.String())
	}

	return b.String()
}

// QueryFilterCondition provides information of filter of query.
type QueryFilterCondition struct {
	Filter string
	Value  interface{}
}
