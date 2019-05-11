package testsuite

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
)

// Test represents a test function for Datastore testing.
type Test func(ctx context.Context, t *testing.T, client datastore.Client)

// TestSuite contains all the test cases that this package provides.
var TestSuite = map[string]Test{
	"Batch_Put":                                   batchPut,
	"Batch_PutWithCustomErrHandler":               batchPutWithCustomErrHandler,
	"Batch_Get":                                   batchGet,
	"Batch_GetWithCustomErrHandler":               batchGetWithCustomErrHandler,
	"Batch_Delete":                                batchDelete,
	"Batch_DeleteWithCustomErrHandler":            batchDeleteWithCustomErrHandler,
	"PutAndGet":                                   putAndGet,
	"PutAndGet_TimeTime":                          putAndGetTimeTime,
	"PutAndDelete":                                putAndDelete,
	"PutAndGet_ObjectHasObjectSlice":              putAndGetObjectHasObjectSlice,
	"PutAndGet_ObjectHasObjectSliceWithFlatten":   putAndGetObjectHasObjectSliceWithFlatten,
	"PutEntityType":                               putEntityType,
	"PutAndGet_NilKey":                            putAndGetNilKey,
	"PutAndGet_NilKeySlice":                       putAndGetNilKeySlice,
	"PutInterface":                                putInterface,
	"PutAndGet_PropertyList":                      putAndGetPropertyList,
	"PutAndGet_MultiPropertyListSlice":            putAndGetMultiPropertyListSlice,
	"PutAndGet_BareStruct":                        putAndGetBareStruct,
	"PutAndGet_MultiBareStruct":                   putAndGetMultiBareStruct,
	"GeoPoint_PutAndGet":                          geoPointPutAndGet,
	"GobDecode":                                   gobDecode,
	"Key_Equal":                                   keyEqual,
	"Key_Incomplete":                              keyIncomplete,
	"Key_PutAndGet":                               keyPutAndGet,
	"Namespace_PutAndGet":                         namespacePutAndGet,
	"Namespace_PutAndGetWithTx":                   namespacePutAndGetWithTx,
	"Namespace_Query":                             namespaceQuery,
	"PLS_Basic":                                   plsBasic,
	"KL_Basic":                                    klBasic,
	"PropertyTranslater_PutAndGet":                propertyTranslaterPutAndGet,
	"Filter_PropertyTranslaterMustError":          filterPropertyTranslaterMustError,
	"Query_Count":                                 queryCount,
	"Query_GetAll":                                queryGetAll,
	"Query_Cursor":                                queryCursor,
	"Query_NextByPropertyList":                    queryNextByPropertyList,
	"Query_GetAllByPropertyListSlice":             queryGetAllByPropertyListSlice,
	"Filter_Basic":                                filterBasic,
	"Filter_PropertyTranslater":                   filterPropertyTranslater,
	"Filter_PropertyTranslaterWithOriginalTypes":  filterPropertyTranslaterWithOriginalTypes,
	"Transaction_Commit":                          transactionCommit,
	"Transaction_Rollback":                        transactionRollback,
	"Transaction_CommitAndRollback":               transactionCommitAndRollback,
	"Transaction_JoinAncesterQuery":               transactionJoinAncesterQuery,
	"RunInTransaction_Commit":                     runInTransactionCommit,
	"RunInTransaction_Rollback":                   runInTransactionRollback,
	"TransactionBatch_Put":                        transactionBatchPut,
	"TransactionBatch_PutWithCustomErrHandler":    transactionBatchPutWithCustomErrHandler,
	"TransactionBatch_PutAndAllocateIDs":          transactionBatchPutAndAllocateIDs,
	"TransactionBatch_Get":                        transactionBatchGet,
	"TransactionBatch_GetWithCustomErrHandler":    transactionBatchGetWithCustomErrHandler,
	"TransactionBatch_Delete":                     transactionBatchDelete,
	"TransactionBatch_DeleteWithCustomErrHandler": transactionBatchDeleteWithCustomErrHandler,

	"CheckIssue59": checkIssue59,
}

// MergeTestSuite into this package's TestSuite.
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

// WrapAEFlag add AEDatastore marker into context. use with IsAEDatastoreClient function.
func WrapAEFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextAE{}, true)
}

// IsAEDatastoreClient returns whether the context is used for AEDatastore.
func IsAEDatastoreClient(ctx context.Context) bool {
	return ctx.Value(contextAE{}) != nil
}

type contextCloud struct{}

// WrapCloudFlag add CloudDatastore marker into context. use with IsCloudDatastoreClient function.
func WrapCloudFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextCloud{}, true)
}

// IsCloudDatastoreClient returns whether the context is used for CloudDatastore.
func IsCloudDatastoreClient(ctx context.Context) bool {
	return ctx.Value(contextCloud{}) != nil
}
