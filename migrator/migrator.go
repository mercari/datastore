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

	PackageNameAE          string
	PackageNameAEDatastore string
	PackageNameGoon        string
	PackageNameBoom        string
	PackageNameContext     string
	UseIterator            bool

	ClientVarName  string
	ContextVarName string
	QueryVarName   string
	TxVarName      string
	CommitVarName  string
	GoonVarName    string
	BoomVarName    string
	GoonTxName     string

	hasAEDatastorePackage bool
	hasGoonPackage        bool

	willModify map[ast.Node]func(c *astutil.Cursor) bool
}

func MigrateFile(w *Walker, targetFile string) (*token.FileSet, *ast.File, error) {
	// 指定されたファイルをASTに変換する
	b, err := ioutil.ReadFile(targetFile)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "ioutil.ReadFile: %s", targetFile)
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, targetFile, b, parser.ParseComments)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "parser.ParseFile")
	}

	// TODO
	//   * datastore.* を呼ぶ箇所があったら、clientを探して使う
	//       clientがなかったらctxを探す
	//         ClientVarName, err := datastore.FromContext(ctx) を呼ぶ
	//         err はめんどいので if err != nil { panic(err) /* TODO */ } とする
	// TODO 初期化サボらない
	w.fset = fset
	w.f = f
	w.hasAEDatastorePackage = false
	w.hasGoonPackage = false
	w.willModify = make(map[ast.Node]func(c *astutil.Cursor) bool)

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

	if w.UseIterator {
		astutil.AddImport(fset, f, "google.golang.org/api/iterator")
	}
	astutil.AddImport(fset, f, w.PackageNameContext)

	return fset, f, nil
}

func (walker *Walker) ApplyModifier(c *astutil.Cursor) bool {
	if f, ok := walker.willModify[c.Node()]; ok {
		return f(c)
	}

	return true
}
