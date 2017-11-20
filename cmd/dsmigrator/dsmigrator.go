package main // import "go.mercari.io/datastore/cmd/dsmigrator"

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"log"

	"go.mercari.io/datastore/migrator"
)

func main() {
	log.SetPrefix("dsmigrator: ")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("args required")
	}

	fset, fs, err := migrator.Main(args)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range fs {
		// 組み替えたASTを文字列に変換する
		buf := bytes.NewBufferString("")
		err = format.Node(buf, fset, f)
		if err != nil {
			log.Fatal(err)
		}

		// TODO 元ファイルを上書きする
		fmt.Println(buf.String())
	}
}
