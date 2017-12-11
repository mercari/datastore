package aedatastore

import (
	"testing"

	"github.com/favclip/testerator"
	_ "github.com/favclip/testerator/datastore"
	_ "github.com/favclip/testerator/memcache"

	"go.mercari.io/datastore/cache/aememcache"
	"go.mercari.io/datastore/testsuite"
	_ "go.mercari.io/datastore/testsuite/cache/dslog"
	_ "go.mercari.io/datastore/testsuite/cache/fishbone"
	_ "go.mercari.io/datastore/testsuite/cache/localcache"
	_ "go.mercari.io/datastore/testsuite/favcliptools"
	_ "go.mercari.io/datastore/testsuite/realworld/recursive_batch"
	_ "go.mercari.io/datastore/testsuite/realworld/tbf"
)

func TestAEDatastoreTestSuite(t *testing.T) {

	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			_, ctx, err := testerator.SpinUp()
			if err != nil {
				t.Fatal(err.Error())
			}
			defer testerator.SpinDown()

			ctx = testsuite.WrapAEFlag(ctx)

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			test(t, ctx, datastore)
		})
	}
}

func TestAEDatastoreWithAEMemcacheTestSuite(t *testing.T) {

	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			// Skip the failure that happens when you firstly appended another cache layer.
			switch name {
			case
				"LocalCache_Basic",
				"LocalCache_WithIncludeKinds",
				"LocalCache_WithExcludeKinds",
				"LocalCache_WithKeyFilter",
				"FishBone_QueryWithoutTx":
				t.SkipNow()
			}

			_, ctx, err := testerator.SpinUp()
			if err != nil {
				t.Fatal(err.Error())
			}
			defer testerator.SpinDown()

			ctx = testsuite.WrapAEFlag(ctx)

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}

			ch := aememcache.New()
			datastore.AppendCacheStrategy(ch)

			test(t, ctx, datastore)
		})
	}
}
