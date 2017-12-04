package migrator

import (
	"go/ast"
	"go/token"
	"strconv"

	"golang.org/x/tools/go/ast/astutil"
)

type AEWalker struct {
	*Walker
}

// TODO IntID, StringID

func (walker *AEWalker) isDatastoreNewIncompleteKeyIdent(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.PackageNameAEDatastore {
		return false
	}
	if selectorExpr.Sel.Name != "NewIncompleteKey" {
		return false
	}

	return true
}

func (walker *AEWalker) isDatastoreNewKeyIdent(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.PackageNameAEDatastore {
		return false
	}
	if selectorExpr.Sel.Name != "NewKey" {
		return false
	}

	return true
}

func (walker *AEWalker) isDatastoreSaveOrLoadStruct(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.PackageNameAEDatastore {
		return false
	}
	if selectorExpr.Sel.Name != "SaveStruct" && selectorExpr.Sel.Name != "LoadStruct" {
		return false
	}

	return true
}

func (walker *AEWalker) isDatastoreRunInTransaction(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.PackageNameAEDatastore {
		return false
	}
	if selectorExpr.Sel.Name != "RunInTransaction" {
		return false
	}

	return true
}

func (walker *AEWalker) isDatastoreRunInTransactionStmt(node ast.Node) bool {
	assignStmt, ok := node.(*ast.AssignStmt)
	if !ok {
		return false
	}
	if len(assignStmt.Lhs) != 1 || len(assignStmt.Rhs) != 1 {
		return false
	}

	callExpr, ok := assignStmt.Rhs[0].(*ast.CallExpr)
	if !ok {
		return false
	}

	return walker.isDatastoreRunInTransaction(callExpr.Fun)
}

func (walker *AEWalker) isTxNoCtxExpr(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.TxVarName {
		return false
	}
	switch selectorExpr.Sel.Name {
	case "Put", "PutMulti", "Get", "GetMulti", "Delete", "DeleteMulti":
		return true
	default:
		return false
	}
}

func (walker *AEWalker) isQueryGetAll(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.QueryVarName {
		return false
	}
	if selectorExpr.Sel.Name != "GetAll" {
		return false
	}

	return true
}

func (walker *AEWalker) isQueryRun(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.QueryVarName {
		return false
	}
	if selectorExpr.Sel.Name != "Run" {
		return false
	}

	return true
}

func (walker *AEWalker) RewriteSignature(c *astutil.Cursor) bool {
	if callExpr, ok := c.Parent().(*ast.CallExpr); ok {
		switch c.Name() {
		case "Args":
			switch {
			case walker.isDatastoreNewIncompleteKeyIdent(callExpr.Fun):
				if ident, ok := c.Node().(*ast.Ident); ok {
					// remove unnecessary ctx argument
					if ident.Name == walker.ContextVarName && callExpr.Args[0] == c.Node() {
						c.Delete()
					}
				}

			case walker.isDatastoreNewKeyIdent(callExpr.Fun):
				if ident, ok := c.Node().(*ast.Ident); ok {
					// remove unnecessary ctx argument
					if callExpr.Args[0] == c.Node() && ident.Name == walker.ContextVarName {
						c.Delete()
					}
				} else if lit, ok := c.Node().(*ast.BasicLit); ok {
					// NewKey rename to IDKey or NameKey
					if callExpr.Args[1] == c.Node() && lit.Kind == token.STRING && lit.Value == strconv.Quote("") {
						// this pattern is ID key
						c.Delete()
						walker.willModify[callExpr.Fun.(*ast.SelectorExpr).Sel] = func(c *astutil.Cursor) bool {
							c.Replace(ast.NewIdent("IDKey"))
							return true
						}

					} else if callExpr.Args[2] == c.Node() && lit.Kind == token.INT && lit.Value == "0" {
						// this pattern is Name key
						c.Delete()
						walker.willModify[callExpr.Fun.(*ast.SelectorExpr).Sel] = func(c *astutil.Cursor) bool {
							c.Replace(ast.NewIdent("NameKey"))
							return true
						}
					}
				}

			case walker.isDatastoreRunInTransaction(callExpr.Fun):
				// delete 3rd argument
				if callExpr.Args[2] == c.Node() {
					c.Delete()
				}

			case walker.isTxNoCtxExpr(callExpr.Fun):
				// remove unnecessary ctx argument
				if ident, ok := c.Node().(*ast.Ident); ok {
					if callExpr.Args[0] == c.Node() && ident.Name == walker.ContextVarName {
						c.Delete()
					}
				}

			case walker.isDatastoreSaveOrLoadStruct(callExpr.Fun):
				// add ctx to 1st argument
				c.InsertBefore(ast.NewIdent(walker.ContextVarName))

			case walker.isQueryGetAll(callExpr.Fun), walker.isQueryRun(callExpr.Fun):
				// add q to 2nd argument
				if callExpr.Args[0] == c.Node() {
					c.InsertAfter(ast.NewIdent(walker.QueryVarName))
				}
			}
		}
	}
	if walker.isDatastoreRunInTransactionStmt(c.Node()) {
		assignStmt := c.Node().(*ast.AssignStmt)
		walker.willModify[assignStmt.Lhs[0]] = func(c *astutil.Cursor) bool {
			c.InsertBefore(ast.NewIdent(walker.CommitVarName))
			return true
		}
		callExpr := assignStmt.Rhs[0].(*ast.CallExpr)
		funcLit := callExpr.Args[1].(*ast.FuncLit)
		funcArg := funcLit.Type.Params.List[0]
		walker.willModify[funcArg.Names[0]] = func(c *astutil.Cursor) bool {
			c.Replace(ast.NewIdent(walker.TxVarName))
			return true
		}
		walker.willModify[funcArg.Type] = func(c *astutil.Cursor) bool {
			txType := &ast.SelectorExpr{
				X:   ast.NewIdent(walker.PackageNameAEDatastore),
				Sel: ast.NewIdent("Transaction"),
			}
			c.Replace(txType)
			return true
		}

		newF := astutil.Apply(c.Node(), walker.RewriteIdentInRunInTransaction, nil)
		c.Replace(newF)
	}

	return true
}

func (walker *AEWalker) RewriteIdentInRunInTransaction(c *astutil.Cursor) bool {
	// datastore.Put to tx.Put etc...

	if selectorExpr, ok := c.Parent().(*ast.SelectorExpr); ok {
		switch c.Name() {
		case "X":
			if sel, ok := c.Node().(*ast.Ident); ok {
				if sel.Name == walker.PackageNameAEDatastore {
					switch selectorExpr.Sel.Name {
					case "Put", "PutMulti", "Get", "GetMulti", "Delete", "DeleteMulti", "NewQuery":
						c.Replace(ast.NewIdent(walker.TxVarName))
					}
				}
			}
		}
	}

	return true
}

func (walker *AEWalker) RewriteIdent(c *astutil.Cursor) bool {
	// TODO q.GetAll(ctx, &list) → client.GetAll(ctx, q, &list) について考える…
	//   型見ないとどうにもならんのでは？？
	//   q.Run(ctx) → client.Run(ctx, q) も同様
	// TODO 変換の優先順位みたいなのつけて複数パスで難しい奴から順番にやらないと辛いかも…？

	if selectorExpr, ok := c.Parent().(*ast.SelectorExpr); ok {
		switch c.Name() {
		case "X":
			if sel, ok := c.Node().(*ast.Ident); ok {
				if sel.Name == walker.PackageNameAE && selectorExpr.Sel.Name == "MultiError" {
					c.Replace(ast.NewIdent(walker.PackageNameAEDatastore))

				} else if sel.Name == walker.PackageNameAE && selectorExpr.Sel.Name == "GeoPoint" {
					c.Replace(ast.NewIdent(walker.PackageNameAEDatastore))

				} else if sel.Name == walker.PackageNameAEDatastore && selectorExpr.Sel.Name == "Done" {
					c.Replace(ast.NewIdent("iterator"))
					walker.UseIterator = true

				} else if sel.Name == walker.PackageNameAEDatastore {
					switch selectorExpr.Sel.Name {
					case "Put", "PutMulti", "Get", "GetMulti", "Delete", "DeleteMulti", "NewQuery":
						c.Replace(ast.NewIdent(walker.ClientVarName))
					case "NewIncompleteKey":
						c.Replace(ast.NewIdent(walker.ClientVarName))
					case "NewKey":
						c.Replace(ast.NewIdent(walker.ClientVarName))
					case "RunInTransaction":
						// TODO 複雑な変換が必要… *ast.AssignStmt から捕まえないとダメそう
						// 1. RunInTransactionの返り値が2つに増える 1つ目は無視
						// 2. RunInTransactionの第二引数を消す
						// 3. 引数として渡す関数の第一引数を tx datastore.Transaction に変更
						//   ネストした構造の場合もあるので変数名は変えないほうが無難…？
						//   第一引数のctxをreceiverにするのが楽なのかもしれない
						// 4. client.Get 系を tx.Get に 第一引数のctxも削除
						c.Replace(ast.NewIdent(walker.ClientVarName))
					}
				} else if sel.Name == walker.QueryVarName {
					switch selectorExpr.Sel.Name {
					case "GetAll", "Run":
						c.Replace(ast.NewIdent(walker.ClientVarName))
					}
				}
			}
		case "Sel":
			if left, ok := selectorExpr.X.(*ast.Ident); ok {
				if left.Name == walker.ClientVarName {
					if right, ok := c.Node().(*ast.Ident); ok {
						switch right.Name {
						case "NewIncompleteKey":
							c.Replace(ast.NewIdent("IncompleteKey"))
						}
					}
				}
			}
		}
	} else if starExpr, ok := c.Node().(*ast.StarExpr); ok {
		if sel, ok := starExpr.X.(*ast.SelectorExpr); ok {
			if ident, ok := sel.X.(*ast.Ident); ok {
				// TODO なんだっけこれ…
				if ident.Name == walker.PackageNameAEDatastore {
					c.Replace(sel)
				}
			}
		}
	}

	if ident, ok := c.Node().(*ast.Ident); ok {
		// TODO ちょっと強火すぎる気もする
		switch ident.Name {
		case "IntID":
			c.Replace(ast.NewIdent("ID"))
		case "StringID":
			c.Replace(ast.NewIdent("Name"))
		}
	}

	return true
}

func (walker *AEWalker) InsertMissingLink(c *astutil.Cursor) bool {
	// this function called twice (pre, post)
	// TODO なんか client とか ctx とかの自動挿入をやりたいという気持ち

	return true
}
