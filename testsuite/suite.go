package testsuite

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
)

type Test func(t *testing.T, ctx context.Context, client datastore.Client)

var TestSuite = map[string]Test{
	"Batch_Put":                                   Batch_Put,
	"Batch_PutWithCustomErrHandler":               Batch_PutWithCustomErrHandler,
	"Batch_Get":                                   Batch_Get,
	"Batch_GetWithCustomErrHandler":               Batch_GetWithCustomErrHandler,
	"Batch_Delete":                                Batch_Delete,
	"Batch_DeleteWithCustomErrHandler":            Batch_DeleteWithCustomErrHandler,
	"PutAndGet":                                   PutAndGet,
	"PutAndGet_TimeTime":                          PutAndGet_TimeTime,
	"PutAndDelete":                                PutAndDelete,
	"PutAndGet_ObjectHasObjectSlice":              PutAndGet_ObjectHasObjectSlice,
	"PutAndGet_ObjectHasObjectSliceWithFlatten":   PutAndGet_ObjectHasObjectSliceWithFlatten,
	"PutEntityType":                               PutEntityType,
	"PutAndGetNilKey":                             PutAndGetNilKey,
	"PutAndGetNilKeySlice":                        PutAndGetNilKeySlice,
	"PutInterface":                                PutInterface,
	"PutAndGetPropertyList":                       PutAndGetPropertyList,
	"PutAndGetMultiPropertyListSlice":             PutAndGetMultiPropertyListSlice,
	"PutAndGetBareStruct":                         PutAndGetBareStruct,
	"PutAndGetMultiBareStruct":                    PutAndGetMultiBareStruct,
	"GeoPoint_PutAndGet":                          GeoPoint_PutAndGet,
	"GobDecode":                                   GobDecode,
	"Key_Equal":                                   Key_Equal,
	"Key_Incomplete":                              Key_Incomplete,
	"Key_PutAndGet":                               Key_PutAndGet,
	"Namespace_PutAndGet":                         Namespace_PutAndGet,
	"Namespace_PutAndGetWithTx":                   Namespace_PutAndGetWithTx,
	"Namespace_Query":                             Namespace_Query,
	"PLS_Basic":                                   PLS_Basic,
	"KL_Basic":                                    KL_Basic,
	"PropertyTranslater_PutAndGet":                PropertyTranslater_PutAndGet,
	"Filter_PropertyTranslaterMustError":          Filter_PropertyTranslaterMustError,
	"Query_Count":                                 Query_Count,
	"Query_GetAll":                                Query_GetAll,
	"Query_Cursor":                                Query_Cursor,
	"Query_NextByPropertyList":                    Query_NextByPropertyList,
	"Query_GetAllByPropertyListSlice":             Query_GetAllByPropertyListSlice,
	"Filter_Basic":                                Filter_Basic,
	"Filter_PropertyTranslater":                   Filter_PropertyTranslater,
	"Transaction_Commit":                          Transaction_Commit,
	"Transaction_Rollback":                        Transaction_Rollback,
	"Transaction_JoinAncesterQuery":               Transaction_JoinAncesterQuery,
	"RunInTransaction_Commit":                     RunInTransaction_Commit,
	"RunInTransaction_Rollback":                   RunInTransaction_Rollback,
	"TransactionBatch_Put":                        TransactionBatch_Put,
	"TransactionBatch_PutWithCustomErrHandler":    TransactionBatch_PutWithCustomErrHandler,
	"TransactionBatch_PutAndAllocateIDs":          TransactionBatch_PutAndAllocateIDs,
	"TransactionBatch_Get":                        TransactionBatch_Get,
	"TransactionBatch_GetWithCustomErrHandler":    TransactionBatch_GetWithCustomErrHandler,
	"TransactionBatch_Delete":                     TransactionBatch_Delete,
	"TransactionBatch_DeleteWithCustomErrHandler": TransactionBatch_DeleteWithCustomErrHandler,
}

func MergeTestSuite(suite map[string]Test) {
	for key, spec := range suite {
		_, ok := TestSuite[key]
		if ok {
			panic("duplicate spec name")
		}
		TestSuite[key] = spec
	}
}

type contextAE struct{}

func WrapAEFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextAE{}, true)
}

func IsAEDatastoreClient(ctx context.Context) bool {
	return ctx.Value(contextAE{}) != nil
}

type contextCloud struct{}

func WrapCloudFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextCloud{}, true)
}

func IsCloudDatastoreClient(ctx context.Context) bool {
	return ctx.Value(contextCloud{}) != nil
}
