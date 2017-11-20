package migrator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"

	"github.com/pkg/errors"
	"golang.org/x/tools/go/ast/astutil"
)

type Walker struct {
	fset *token.FileSet
	f    *ast.File

	packageNameAE          string
	packageNameAEDatastore string
	packageNameGoon        string
	packageNameBoom        string
	packageContext         string
	useIterator            bool

	clientVarName  string
	contextVarName string
	queryVarName   string
	txVarName      string
	commitVarName  string
	goonVarName    string
	boomVarName    string
	goonTxName     string

	hasAEDatastorePackage bool
	hasGoonPackage        bool

	willModify map[ast.Node]func(c *astutil.Cursor) bool
}

func Main(targetFiles []string) (*token.FileSet, []*ast.File, error) {
	fset := token.NewFileSet()
	fs := make([]*ast.File, 0, len(targetFiles))
	for _, targetFile := range targetFiles {
		f, err := MigrateFile(fset, targetFile)
		if err != nil {
			return nil, nil, err
		}
		fs = append(fs, f)
	}

	return fset, fs, nil
}

func MigrateFile(fset *token.FileSet, targetFile string) (*ast.File, error) {
	// 指定されたファイルをASTに変換する
	b, err := ioutil.ReadFile(targetFile)
	if err != nil {
		return nil, errors.Wrapf(err, "ioutil.ReadFile: %s", targetFile)
	}

	f, err := parser.ParseFile(fset, targetFile, b, parser.ParseComments)
	if err != nil {
		return nil, errors.Wrapf(err, "parser.ParseFile")
	}

	// TODO
	//   * datastore.* を呼ぶ箇所があったら、clientを探して使う
	//       clientがなかったらctxを探す
	//         clientVarName, err := datastore.FromContext(ctx) を呼ぶ
	//         err はめんどいので if err != nil { panic(err) /* TODO */ } とする
	w := &Walker{
		fset:                   fset,
		f:                      f,
		packageNameAE:          "appengine", // TODO
		packageNameAEDatastore: "datastore", // TODO
		packageNameGoon:        "goon",      // TODO
		packageNameBoom:        "boom",      // TODO
		packageContext:         "context",   // TODO
		clientVarName:          "client",
		contextVarName:         "ctx",
		queryVarName:           "q",
		txVarName:              "tx",
		commitVarName:          "commit",
		goonVarName:            "g",
		boomVarName:            "bm",
		goonTxName:             "tg",
		willModify:             make(map[ast.Node]func(c *astutil.Cursor) bool),
	}

	{
		aew := &AEWalker{
			Walker: w,
		}

		// TODO context使ってる箇所解析したい…
		// go/types
		// https://qiita.com/tenntenn/items/beea3bd019ba92b4d62a

		if astutil.UsesImport(f, "google.golang.org/appengine/datastore") {
			w.hasAEDatastorePackage = true
		}
		astutil.RewriteImport(fset, f, "google.golang.org/appengine/datastore", "go.mercari.io/datastore")

		astutil.Apply(f, aew.RewriteSignature, nil)
		astutil.Apply(f, aew.RewriteIdent, nil)
		astutil.Apply(f, aew.InsertMissingLink, aew.InsertMissingLink)
	}
	{
		gw := &GoonWalker{
			Walker: w,
		}
		if astutil.UsesImport(f, "github.com/mjibson/goon") {
			w.hasGoonPackage = true
		}
		astutil.RewriteImport(fset, f, "github.com/mjibson/goon", "go.mercari.io/datastore/boom")

		astutil.Apply(f, gw.RewriteSignature, nil)
		astutil.Apply(f, gw.RewriteIdent, nil)
	}
	astutil.Apply(f, w.ApplyModifier, nil)

	if w.useIterator {
		astutil.AddImport(fset, f, "google.golang.org/api/iterator")
	}
	astutil.AddImport(fset, f, w.packageContext)

	return f, nil
}

func (walker *Walker) ApplyModifier(c *astutil.Cursor) bool {
	if f, ok := walker.willModify[c.Node()]; ok {
		return f(c)
	}

	return true
}
