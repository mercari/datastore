package testsuite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mercari.io/datastore"
	"google.golang.org/api/iterator"
)

func Query_Count(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Str: "A"})
	if err != nil {
		t.Fatal(err)
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Str: "B"})
	if err != nil {
		t.Fatal(err)
	}
	key3, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Str: "B"})
	if err != nil {
		t.Fatal(err)
	}

	t.Log(key1, key2, key3)

	q := client.NewQuery("Data").Filter("Str =", "B")
	count, err := client.Count(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	if count != 2 {
		t.Errorf("unexpected: %v", count)
	}
}

func Query_GetAll(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str   string
		Order int
	}

	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Str: "A", Order: 1})
	if err != nil {
		t.Fatal(err)
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Str: "B", Order: 2})
	if err != nil {
		t.Fatal(err)
	}
	key3, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Str: "B", Order: 3})
	if err != nil {
		t.Fatal(err)
	}

	t.Log(key1, key2, key3)

	{
		client.GetMulti(ctx, []datastore.Key{key1, key2, key3}, []interface{}{&Data{}, &Data{}, &Data{}})
	}

	q := client.NewQuery("Data").Filter("Str =", "B").Order("Order")
	var dataList []*Data
	keys, err := client.GetAll(ctx, q, &dataList)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(keys); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}

	if v := dataList[0].Order; v != 2 {
		t.Errorf("unexpected: %v", v)
	}
	if v := dataList[1].Order; v != 3 {
		t.Errorf("unexpected: %v", v)
	}
}

func Query_Cursor(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	{
		var keys []datastore.Key
		var entities []*Data
		for i := 0; i < 100; i++ {
			keys = append(keys, client.IncompleteKey("Data", nil))
			entities = append(entities, &Data{Str: fmt.Sprintf("#%d", i+1)})
		}
		var err error
		_, err = client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatal(err)
		}
	}

	var cur datastore.Cursor
	var err error
	var startCur datastore.Cursor
	var endCur datastore.Cursor

	var dataList []*Data
	const limit = 3
outer:
	for {
		q := client.NewQuery("Data").Order("Str").Limit(limit)
		if cur != nil {
			q = q.Start(cur)
		}
		it := client.Run(ctx, q)

		count := 0
		for {
			obj := &Data{}
			_, err := it.Next(obj)
			if err == iterator.Done {
				break
			} else if err != nil {
				t.Fatal(err)
			}

			dataList = append(dataList, obj)
			count++
		}
		if count != limit {
			break
		}

		cur, err = it.Cursor()
		if err != nil {
			t.Fatal(err)
		}
		if cur.String() == "" {
			break outer
		}
		if startCur == nil {
			startCur = cur
		} else if endCur == nil {
			endCur = cur
		}
	}

	if v := len(dataList); v != 100 {
		t.Errorf("unexpected: %v", v)
	}

	q := client.NewQuery("Data").Order("Str").Limit(limit)
	q = q.Start(startCur).End(endCur).KeysOnly()
	newKeys, err := client.GetAll(ctx, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(newKeys); v != limit {
		t.Errorf("unexpected: %v", v)
	}
}

func Query_NextByPropertyList(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Name string
	}

	_, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery("Data")

	{ // passed datastore.PropertyList, would be error.
		iter := client.Run(ctx, q)

		var ps datastore.PropertyList
		_, err = iter.Next(ps)
		if err == nil {
			t.Fatal(err)
		}
	}
	{ // ok! *datastore.PropertyList
		iter := client.Run(ctx, q)

		var ps datastore.PropertyList
		_, err = iter.Next(&ps)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func Query_GetAllByPropertyListSlice(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Name string
	}

	_, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery("Data")
	var psList []datastore.PropertyList

	// passed []datastore.PropertyList, would be error.
	_, err = client.GetAll(ctx, q, psList)
	if err == nil {
		t.Fatal(err)
	}

	// ok! *[]datastore.PropertyList
	psList = nil
	_, err = client.GetAll(ctx, q, &psList)
	if err != nil {
		t.Fatal(err)
	}
}

func Filter_Basic(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Int      int
		Bool     bool
		String   string
		Float    float64
		Key      datastore.Key
		GeoPoint datastore.GeoPoint
		Time     time.Time
	}

	now := time.Now()

	obj1 := &Data{
		Int:      1,
		Bool:     true,
		String:   "1",
		Float:    1.1,
		Key:      client.IDKey("Test", 1, nil),
		GeoPoint: datastore.GeoPoint{Lat: 1.1, Lng: 1.2},
		Time:     now,
	}
	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), obj1)
	if err != nil {
		t.Fatal(err)
	}

	obj2 := &Data{
		Int:      2,
		Bool:     false,
		String:   "2",
		Float:    2.2,
		Key:      client.IDKey("Test", 2, nil),
		GeoPoint: datastore.GeoPoint{Lat: 2.1, Lng: 2.2},
		Time:     now.Add(1 * time.Hour),
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), obj2)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(key1, key2)

	expects := []struct {
		Name  string
		Value interface{}
	}{
		{"Int", 1},
		{"Bool", true},
		{"String", "1"},
		{"Float", 1.1},
		{"Key", client.IDKey("Test", 1, nil)},
		{"GeoPoint", datastore.GeoPoint{Lat: 1.1, Lng: 1.2}},
		{"Time", now},
	}

	for _, expect := range expects {
		t.Logf("expect: %#v", expect)
		filterStr := fmt.Sprintf("%s =", expect.Name)
		{ // Count
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			cnt, err := client.Count(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if cnt != 1 {
				t.Errorf("unexpected: %v", cnt)
			}
		}
		{ // GetAll
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			var list []*Data
			keys, err := client.GetAll(ctx, q, &list)
			if err != nil {
				t.Fatal(err)
			}
			if v := len(keys); v != 1 {
				t.Fatalf("unexpected: %v", v)
			}
			if v := keys[0]; v.ID() != key1.ID() {
				t.Errorf("unexpected: %v", v)
			}
			if v := len(list); v != 1 {
				t.Errorf("unexpected: %v", v)
			}
		}
		{ // Run
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			iter := client.Run(ctx, q)
			cnt := 0
			for {
				obj := &Data{}
				key, err := iter.Next(obj)
				if err == iterator.Done {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				cnt++
				if v := key; v.ID() != key1.ID() {
					t.Errorf("unexpected: %v", v)
				}
			}
			if cnt != 1 {
				t.Errorf("unexpected: %v", cnt)
			}
		}
	}
}

func Filter_PropertyTranslater(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		UserID   UserID
		UnixTime UnixTime
	}

	now := time.Now()

	obj1 := &Data{
		UserID:   UserID(1),
		UnixTime: UnixTime(now),
	}
	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), obj1)
	if err != nil {
		t.Fatal(err)
	}

	obj2 := &Data{
		UserID:   UserID(2),
		UnixTime: UnixTime(now.Add(1 * time.Hour)),
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), obj2)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(key1, key2)

	expects := []struct {
		Name  string
		Value interface{}
	}{
		{"UserID", UserID(1)},
		{"UnixTime", UnixTime(now)},
	}

	for _, expect := range expects {
		t.Logf("expect: %#v", expect)
		filterStr := fmt.Sprintf("%s =", expect.Name)
		{ // Count
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			cnt, err := client.Count(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if cnt != 1 {
				t.Errorf("unexpected: %v", cnt)
			}
		}
		{ // GetAll
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			var list []*Data
			keys, err := client.GetAll(ctx, q, &list)
			if err != nil {
				t.Fatal(err)
			}
			if v := len(keys); v != 1 {
				t.Fatalf("unexpected: %v", v)
			}
			if v := keys[0]; v.ID() != key1.ID() {
				t.Errorf("unexpected: %v", v)
			}
			if v := len(list); v != 1 {
				t.Errorf("unexpected: %v", v)
			}
		}
		{ // Run
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			iter := client.Run(ctx, q)
			cnt := 0
			for {
				obj := &Data{}
				key, err := iter.Next(obj)
				if err == iterator.Done {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				cnt++
				if v := key; v.ID() != key1.ID() {
					t.Errorf("unexpected: %v", v)
				}
			}
			if cnt != 1 {
				t.Errorf("unexpected: %v", cnt)
			}
		}
	}
}

func Filter_PropertyTranslaterWithOriginalTypes(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		UserID   UserID
		UnixTime UnixTime
	}

	now := time.Now()

	obj1 := &Data{
		UserID:   UserID(1),
		UnixTime: UnixTime(now),
	}
	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), obj1)
	if err != nil {
		t.Fatal(err)
	}

	obj2 := &Data{
		UserID:   UserID(2),
		UnixTime: UnixTime(now.Add(1 * time.Hour)),
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), obj2)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(key1, key2)

	expects := []struct {
		Name  string
		Value interface{}
	}{
		{"UserID", client.IDKey("User", 1, nil)},
		{"UnixTime", now},
	}

	for _, expect := range expects {
		t.Logf("expect: %#v", expect)
		filterStr := fmt.Sprintf("%s =", expect.Name)
		{ // Count
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			cnt, err := client.Count(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if cnt != 1 {
				t.Errorf("unexpected: %v", cnt)
			}
		}
		{ // GetAll
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			var list []*Data
			keys, err := client.GetAll(ctx, q, &list)
			if err != nil {
				t.Fatal(err)
			}
			if v := len(keys); v != 1 {
				t.Fatalf("unexpected: %v", v)
			}
			if v := keys[0]; v.ID() != key1.ID() {
				t.Errorf("unexpected: %v", v)
			}
			if v := len(list); v != 1 {
				t.Errorf("unexpected: %v", v)
			}
		}
		{ // Run
			q := client.NewQuery("Data").Filter(filterStr, expect.Value)
			iter := client.Run(ctx, q)
			cnt := 0
			for {
				obj := &Data{}
				key, err := iter.Next(obj)
				if err == iterator.Done {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				cnt++
				if v := key; v.ID() != key1.ID() {
					t.Errorf("unexpected: %v", v)
				}
			}
			if cnt != 1 {
				t.Errorf("unexpected: %v", cnt)
			}
		}
	}
}

var _ datastore.PropertyTranslator = (*MustReturnsError)(nil)

type MustReturnsError int

func (_ MustReturnsError) ToPropertyValue(ctx context.Context) (interface{}, error) {
	return nil, errors.New("ERROR!")
}

func (_ MustReturnsError) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	return nil, errors.New("ERROR!")
}

func Filter_PropertyTranslaterMustError(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		TMP int
	}

	{ // Count
		q := client.NewQuery("Data").Filter("TMP =", MustReturnsError(1))
		_, err := client.Count(ctx, q)
		if err == nil || err.Error() != "ERROR!" {
			t.Fatal(err)
		}
	}
	{ // GetAll
		q := client.NewQuery("Data").Filter("TMP =", MustReturnsError(1))
		var list []*Data
		_, err := client.GetAll(ctx, q, &list)
		if err == nil || err.Error() != "ERROR!" {
			t.Fatal(err)
		}
	}
	{ // Run
		q := client.NewQuery("Data").Filter("TMP =", MustReturnsError(1))
		iter := client.Run(ctx, q)
		obj := &Data{}
		_, err := iter.Next(obj)
		if err == nil || err.Error() != "ERROR!" {
			t.Fatal(err)
		}
	}
}
