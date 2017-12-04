package main // import "go.mercari.io/datastore/cmd/dsmigrator"

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"log"

	"io/ioutil"

	"go.mercari.io/datastore/migrator"
)

var (
	rewrite = flag.Bool("r", false, "TODO")

	packageNameAE          = flag.String("package-name-ae", "appengine", "TODO")
	packageNameAEDatastore = flag.String("package-name-ae-datastore", "datastore", "TODO")
	packageNameGoon        = flag.String("package-name-goon", "goon", "TODO")
	packageNameBoom        = flag.String("package-name-boom", "boom", "TODO")
	packageNameContext     = flag.String("package-name-context", "context", "TODO")
	clientVarName          = flag.String("client-var-name", "client", "TODO")
	contextVarName         = flag.String("context-var-name", "ctx", "TODO")
	queryVarName           = flag.String("query-var-name", "q", "TODO")
	txVarName              = flag.String("tx-var-name", "tx", "TODO")
	commitVarName          = flag.String("commit-var-name", "commit", "TODO")
	goonVarName            = flag.String("goon-var-name", "g", "TODO")
	boomVarName            = flag.String("boom-var-name", "bm", "TODO")
	goonTxName             = flag.String("goon-tx-name", "tg", "TODO")
)

func main() {
	log.SetPrefix("dsmigrator: ")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("args required")
	}

	w := &migrator.Walker{
		PackageNameAE:          *packageNameAE,
		PackageNameAEDatastore: *packageNameAEDatastore,
		PackageNameGoon:        *packageNameGoon,
		PackageNameBoom:        *packageNameBoom,
		PackageNameContext:     *packageNameContext,
		ClientVarName:          *clientVarName,
		ContextVarName:         *contextVarName,
		QueryVarName:           *queryVarName,
		TxVarName:              *txVarName,
		CommitVarName:          *commitVarName,
		GoonVarName:            *goonVarName,
		BoomVarName:            *boomVarName,
		GoonTxName:             *goonTxName,
	}

	for _, targetFile := range args {
		fset, f, err := migrator.MigrateFile(w, targetFile)
		if err != nil {
			log.Fatal(err)
		}

		var buf bytes.Buffer
		err = format.Node(&buf, fset, f)
		if err != nil {
			log.Fatal(err)
		}

		if *rewrite {
			err = ioutil.WriteFile(targetFile, buf.Bytes(), 0644)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			fmt.Println(buf.String())
		}
	}
}
