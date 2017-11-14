package aeprodtest

import (
	"net/http"

	"google.golang.org/appengine"
	aedatastore "google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"go.mercari.io/datastore"
	_ "go.mercari.io/datastore/aedatastore"
)

func init() {
	// Put Entity via aedatastore
	http.HandleFunc("/api/test1", func(w http.ResponseWriter, r *http.Request) {
		ctx := appengine.NewContext(r)

		type Inner struct {
			A string
			B string
		}

		type Data struct {
			Slice []Inner
		}

		key := aedatastore.NewIncompleteKey(ctx, "AETest", nil)
		_, err := aedatastore.Put(ctx, key, &Data{
			Slice: []Inner{
				Inner{A: "A1", B: "B1"},
				Inner{A: "A2", B: "B2"},
				Inner{A: "A3", B: "B3"},
			},
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Warningf(ctx, "error: %v", err)
			return
		}

		w.WriteHeader(200)
	})

	// Put Entity via datastore
	http.HandleFunc("/api/test2", func(w http.ResponseWriter, r *http.Request) {
		ctx := appengine.NewContext(r)

		ds, err := datastore.FromContext(ctx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Warningf(ctx, "error: %v", err)
			return
		}

		type Inner struct {
			A string
			B string
		}

		type Data struct {
			Slice []Inner
		}

		key := ds.IncompleteKey("AETest", nil)
		_, err = ds.Put(ctx, key, &Data{
			Slice: []Inner{
				Inner{A: "A1", B: "B1"},
				Inner{A: "A2", B: "B2"},
				Inner{A: "A3", B: "B3"},
			},
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Warningf(ctx, "error: %v", err)
			return
		}

		w.WriteHeader(200)
	})

	// Put Entity via datastore
	http.HandleFunc("/api/test3", func(w http.ResponseWriter, r *http.Request) {
		ctx := appengine.NewContext(r)

		ds, err := datastore.FromContext(ctx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Warningf(ctx, "error: %v", err)
			return
		}

		type Inner struct {
			A string
			B string
		}

		type Data struct {
			Slice []Inner `datastore:",flatten"`
		}

		key := ds.IncompleteKey("AETest", nil)
		_, err = ds.Put(ctx, key, &Data{
			Slice: []Inner{
				Inner{A: "A1", B: "B1"},
				Inner{A: "A2", B: "B2"},
				Inner{A: "A3", B: "B3"},
			},
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Warningf(ctx, "error: %v", err)
			return
		}

		w.WriteHeader(200)
	})
}
