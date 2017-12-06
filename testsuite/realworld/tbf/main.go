package tbf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/testsuite"
	netcontext "golang.org/x/net/context"
	"google.golang.org/appengine"
)

var TestSuite = map[string]testsuite.Test{
	"RealWorld_TBF": RealWorld_TBF,
}

func init() {
	testsuite.MergeTestSuite(TestSuite)
}

type contextClient struct{}
type contextBatch struct{}

func timeNow() time.Time {
	l, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		panic(err)
	}
	return time.Date(2017, 11, 8, 10, 11, 12, 13, l)
}

func RealWorld_TBF(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// それぞれのCircleに4枚の画像を持たせて、Circleを10件Getしたい。RPCは Query 1回 + BatchGet 1回 の合計2回がいい

	// clientは複数作ってぶん回すとどっかでブロックされて処理が返ってこなくなるので使いまわす
	ctx = context.WithValue(ctx, contextClient{}, client)
	// batchは再利用可能
	batch := client.Batch()
	ctx = context.WithValue(ctx, contextBatch{}, batch)

	rpcCount := 0
	inMemcacheTestSuite := false
	if testsuite.IsAEDatastoreClient(ctx) {
		// checking rpc count when testing in ae.
		ctx = appengine.WithAPICallFunc(ctx, func(ctx netcontext.Context, service, method string, in, out proto.Message) error {
			t.Log(service, method)
			if service == "datastore_v3" {
				rpcCount++
			}
			if service == "memcache" {
				// if memcache service called, this test in the TestAEDatastoreWithAEMemcacheTestSuite.
				inMemcacheTestSuite = true
			}

			return appengine.APICall(ctx, service, method, in, out)
		})
	}

	const circleLimit = 10
	const imageLimit = 4

	// Prepare entities
	for i := 0; i < circleLimit; i++ {
		// NOTE Don't use client.AllocateIDs for JSON format consistency
		circleID := CircleID(1000000 + 10000*i)
		circleKey := circleID.ToKey(client)

		circle := &Circle{
			ID:   circleID,
			Name: fmt.Sprintf("サークル #%d", i+1),
		}
		for j := 0; j < imageLimit; j++ {
			// NOTE Don't use client.AllocateIDs for JSON format consistency
			imageID := ImageID(circleKey.ID() + 1000 + int64(10*j))
			imageKey := imageID.ToKey(client)

			image := &Image{
				ID:            imageID,
				OwnerCircleID: circleID,
				GCSPath:       fmt.Sprintf("%d/%d.jpg", circleKey.ID(), imageKey.ID()),
			}
			batch.Put(imageKey, image, nil)

			circle.ImageIDs = append(circle.ImageIDs, image.ID)
			circle.Images = append(circle.Images, image)
		}

		batch.Put(circleKey, circle, nil)
	}
	err := batch.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if testsuite.IsAEDatastoreClient(ctx) && !inMemcacheTestSuite {
		if rpcCount != 1 {
			t.Errorf("unexpected: %v", rpcCount)
		}
	}

	// fetch entities
	rpcCount = 0
	q := client.NewQuery(kindCircle)
	var circleList []*Circle
	_, err = client.GetAll(ctx, q, &circleList)
	if err != nil {
		t.Fatal(err)
	}
	err = batch.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if testsuite.IsAEDatastoreClient(ctx) && !inMemcacheTestSuite {
		if rpcCount != 2 {
			t.Errorf("unexpected: %v", rpcCount)
		}
	}

	if v := len(circleList); v != circleLimit {
		t.Errorf("unexpected: %v", v)
	}
	for _, circle := range circleList {
		if v := len(circle.Images); v != imageLimit {
			t.Errorf("unexpected: %v", v)
		}
		for _, image := range circle.Images {
			if v := image.GCSPath; !strings.HasPrefix(v, fmt.Sprintf("%d/", circle.ID)) {
				t.Errorf("unexpected: %v", v)
			}
		}
	}

	{ // obj <-> JSON
		b, err := json.MarshalIndent(circleList, "", "  ")
		if err != nil {
			t.Fatal(err)
		}

		const filePath = "../testsuite/realworld/tbf/realworld-tbf.json"
		expected, err := ioutil.ReadFile(filePath)
		if err != nil {
			err = ioutil.WriteFile(filePath, b, 0644)
			if err != nil {
				t.Fatal(err)
			}
			expected = b
		}

		if !bytes.Equal(b, expected) {
			t.Fatalf("unexpected json format. hint: rm %s", filePath)
		}

		var newCircleList []*Circle
		err = json.Unmarshal(b, &newCircleList)
		if err != nil {
			t.Fatal(err)
		}

		if v := len(newCircleList); v != circleLimit {
			t.Errorf("unexpected: %v", v)
		}
		for _, circle := range newCircleList {
			if v := len(circle.Images); v != imageLimit {
				t.Errorf("unexpected: %v", v)
			}
			for _, image := range circle.Images {
				if v := image.GCSPath; !strings.HasPrefix(v, fmt.Sprintf("%d/", circle.ID)) {
					t.Errorf("unexpected: %v", v)
				}
			}
		}
	}

	{ // Naked Get
		q := client.NewQuery(kindImage)
		var pss []datastore.PropertyList
		_, err = client.GetAll(ctx, q, &pss)
		if err != nil {
			t.Fatal(err)
		}

		if v := len(pss); v != circleLimit*imageLimit {
			t.Errorf("unexpected: %v", v)
		}
		for _, ps := range pss {
			for _, p := range ps {
				if !strings.HasSuffix(p.Name, "ID") {
					continue
				}

				_, ok := p.Value.(datastore.Key)
				if !ok {
					t.Errorf("unexpected: %T", p.Value)
				}
			}
		}
	}
}
