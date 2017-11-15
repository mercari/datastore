package aedatastore

import (
	"testing"

	"go.mercari.io/datastore/testsuite"
	_ "go.mercari.io/datastore/testsuite/favcliptools"
	_ "go.mercari.io/datastore/testsuite/realworld/tbf"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
)

func TestAEDatastoreTestSuite(t *testing.T) {

	for name, test := range testsuite.TestSuite {
		t.Run(name, func(t *testing.T) {
			inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true})
			if err != nil {
				t.Fatal(err.Error())
			}
			defer inst.Close()
			r, err := inst.NewRequest("GET", "/", nil)
			if err != nil {
				t.Fatal(err.Error())
			}
			ctx := appengine.NewContext(r)
			ctx = testsuite.WrapAEFlag(ctx)

			datastore, err := FromContext(ctx)
			if err != nil {
				t.Fatal(err)
			}
			test(t, ctx, datastore)
		})
	}
}
