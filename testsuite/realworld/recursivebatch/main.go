package recursivebatch

import (
	"context"
	"fmt"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/boom"
	"go.mercari.io/datastore/testsuite"
)

// TestSuite contains all the test cases that this package provides.
var TestSuite = map[string]testsuite.Test{
	"RealWorld_RecursiveBatch": recursiveBatch,
}

func init() {
	testsuite.MergeTestSuite(TestSuite)
}

var _ datastore.PropertyLoadSaver = &depth1{}
var _ datastore.PropertyLoadSaver = &depth2{}

type depth1 struct {
	ID         int64     `boom:"id"`
	Depth2IDs  []int64   `json:"-"`
	Depth2List []*depth2 `datastore:"-"`
}

type depth2 struct {
	ID         int64     `boom:"id"`
	Depth3IDs  []int64   `json:"-"`
	Depth3List []*depth3 `datastore:"-"`
}

type depth3 struct {
	ID   int64  `boom:"id"`
	Name string ``
}

func (d *depth1) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, d, ps)
	if err != nil {
		return err
	}

	bt := extractBoomBatch(ctx)

	d.Depth2List = make([]*depth2, 0, len(d.Depth2IDs))
	for _, depth2ID := range d.Depth2IDs {
		d2 := &depth2{
			ID: depth2ID,
		}
		bt.Get(d2, nil)
		d.Depth2List = append(d.Depth2List, d2)
	}

	return nil
}

func (d *depth1) Save(ctx context.Context) ([]datastore.Property, error) {
	d.Depth2IDs = make([]int64, 0, len(d.Depth2List))
	for _, d2 := range d.Depth2List {
		d.Depth2IDs = append(d.Depth2IDs, d2.ID)
	}

	return datastore.SaveStruct(ctx, d)
}

func (d *depth2) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, d, ps)
	if err != nil {
		return err
	}

	bt := extractBoomBatch(ctx)

	d.Depth3List = make([]*depth3, 0, len(d.Depth3IDs))
	for _, depth3ID := range d.Depth3IDs {
		d3 := &depth3{
			ID: depth3ID,
		}
		bt.Get(d3, nil)
		d.Depth3List = append(d.Depth3List, d3)
	}

	return nil
}

func (d *depth2) Save(ctx context.Context) ([]datastore.Property, error) {
	d.Depth3IDs = make([]int64, 0, len(d.Depth3List))
	for _, d3 := range d.Depth3List {
		d.Depth3IDs = append(d.Depth3IDs, d3.ID)
	}

	return datastore.SaveStruct(ctx, d)
}

type contextBoomBatch struct{}

func extractBoomBatch(ctx context.Context) *boom.Batch {
	return ctx.Value(contextBoomBatch{}).(*boom.Batch)
}

func recursiveBatch(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	bm := boom.FromClient(ctx, client)
	bt := bm.Batch()
	ctx = context.WithValue(ctx, contextBoomBatch{}, bt)
	bm.Context = ctx

	const size = 5

	// make test data
	for i := 1; i <= size; i++ {
		d1 := &depth1{
			ID: int64(i),
		}
		for j := 1; j <= size; j++ {
			d2 := &depth2{
				ID: int64(i*1000 + j),
			}
			for k := 1; k <= size; k++ {
				d3 := &depth3{
					ID:   int64(i*1000000 + j*1000 + k),
					Name: fmt.Sprintf("#%d", i*1000000+j*1000+k),
				}
				bt.Put(d3, nil)
				d2.Depth3List = append(d2.Depth3List, d3)
			}
			bt.Put(d2, nil)
			d1.Depth2List = append(d1.Depth2List, d2)
		}
		bt.Put(d1, nil)
	}
	err := bt.Exec()
	if err != nil {
		t.Fatal(err)
	}

	// get test data
	list := make([]*depth1, 0, size)
	for i := 1; i <= size; i++ {
		d1 := &depth1{
			ID: int64(i),
		}
		bt.Get(d1, nil)
		list = append(list, d1)
	}
	err = bt.Exec()
	if err != nil {
		t.Fatal(err)
	}

	if v := len(list); v != size {
		t.Errorf("unexpected: %v", v)
	}
	for idx1, d1 := range list {
		if v := d1.ID; v != int64(idx1+1) {
			t.Errorf("unexpected: %v", v)
		}

		if v := len(d1.Depth2List); v != size {
			t.Errorf("unexpected: %v", v)
		}
		for idx2, d2 := range d1.Depth2List {
			if v := d2.ID; v != d1.ID*1000+int64(idx2+1) {
				t.Errorf("unexpected: %v", v)
			}

			if v := len(d2.Depth3List); v != size {
				t.Errorf("unexpected: %v", v)
			}
			for idx3, d3 := range d2.Depth3List {
				if v := d3.ID; v != d2.ID*1000+int64(idx3+1) {
					t.Errorf("unexpected: %v", v)
					t.Errorf("unexpected: %v", d1.ID*1000000+d2.ID*1000+int64(idx3+1))
					t.Errorf("unexpected: %v", d1.ID)
					t.Errorf("unexpected: %v", d2.ID)
				}
				if v := d3.Name; v != fmt.Sprintf("#%d", d3.ID) {
					t.Errorf("unexpected: %v", v)
				}
			}
		}
	}
}
