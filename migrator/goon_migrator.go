package migrator

import (
	"go/ast"

	"golang.org/x/tools/go/ast/astutil"
)

type GoonWalker struct {
	*Walker
}

func (walker *GoonWalker) isGoonGoonType(expr ast.Expr) bool {
	starExpr, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}

	selectExpr, ok := starExpr.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	ident, ok := selectExpr.X.(*ast.Ident)
	if !ok {
		return false
	}
	if ident.Name != walker.PackageNameGoon {
		return false
	}

	if selectExpr.Sel.Name != "Goon" {
		return false
	}

	return true
}

func (walker *GoonWalker) isBoomBoomType(expr ast.Expr) bool {
	starExpr, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}

	selectExpr, ok := starExpr.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	ident, ok := selectExpr.X.(*ast.Ident)
	if !ok {
		return false
	}
	if ident.Name != walker.PackageNameBoom {
		return false
	}

	if selectExpr.Sel.Name != "Boom" {
		return false
	}

	return true
}

func (walker *GoonWalker) isGoonFromContextStmt(node ast.Node) bool {
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

	return walker.isGoonFromContext(callExpr.Fun)
}

func (walker *GoonWalker) isGoonFromContext(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.PackageNameGoon {
		return false
	}
	if selectorExpr.Sel.Name != "FromContext" {
		return false
	}

	return true
}

func (walker *GoonWalker) isGoonRunInTransaction(node ast.Node) bool {
	selectorExpr, ok := node.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if ident, ok := selectorExpr.X.(*ast.Ident); !ok {
		return false
	} else if ident.Name != walker.GoonVarName {
		return false
	}
	if selectorExpr.Sel.Name != "RunInTransaction" {
		return false
	}

	return true
}

func (walker *GoonWalker) isGoonRunInTransactionStmt(node ast.Node) bool {
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

	return walker.isGoonRunInTransaction(callExpr.Fun)
}

func (walker *GoonWalker) RewriteSignature(c *astutil.Cursor) bool {
	if callExpr, ok := c.Parent().(*ast.CallExpr); ok {
		switch c.Name() {
		case "Args":
			switch {
			case walker.isGoonRunInTransaction(callExpr.Fun):
				// delete 2nd argument
				if callExpr.Args[1] == c.Node() {
					c.Delete()
				}
			}
		}
	}

	if walker.isGoonFromContextStmt(c.Node()) {
		assignStmt := c.Node().(*ast.AssignStmt)
		walker.willModify[assignStmt.Lhs[0]] = func(c *astutil.Cursor) bool {
			c.Replace(ast.NewIdent(walker.BoomVarName))
			c.InsertAfter(ast.NewIdent("_"))
			return true
		}
	}
	if walker.isGoonRunInTransactionStmt(c.Node()) {
		assignStmt := c.Node().(*ast.AssignStmt)
		walker.willModify[assignStmt.Lhs[0]] = func(c *astutil.Cursor) bool {
			c.InsertBefore(ast.NewIdent(walker.CommitVarName))
			return true
		}
		callExpr := assignStmt.Rhs[0].(*ast.CallExpr)
		funcLit := callExpr.Args[0].(*ast.FuncLit)
		funcArg := funcLit.Type.Params.List[0]
		walker.willModify[funcArg.Names[0]] = func(c *astutil.Cursor) bool {
			ident := c.Node().(*ast.Ident)
			if ident.Name == walker.GoonTxName {
				c.Replace(ast.NewIdent(walker.TxVarName))
			}

			return true
		}
		walker.willModify[funcArg.Type] = func(c *astutil.Cursor) bool {
			txType := &ast.StarExpr{
				X: &ast.SelectorExpr{
					X:   ast.NewIdent(walker.PackageNameBoom),
					Sel: ast.NewIdent("Transaction"),
				},
			}
			c.Replace(txType)
			return true
		}

		astutil.Apply(funcLit.Body, walker.RewriteIdentInRunInTransaction, nil)
	}

	return true
}

func (walker *GoonWalker) RewriteIdentInRunInTransaction(c *astutil.Cursor) bool {
	if selectorExpr, ok := c.Parent().(*ast.SelectorExpr); ok {
		switch c.Name() {
		case "X":
			if sel, ok := c.Node().(*ast.Ident); ok {
				if sel.Name == walker.GoonTxName {
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

func (walker *GoonWalker) RewriteIdent(c *astutil.Cursor) bool {
	if selectorExpr, ok := c.Parent().(*ast.SelectorExpr); ok {
		switch c.Name() {
		case "X":
			if ident, ok := c.Node().(*ast.Ident); ok {
				if ident.Name == walker.GoonVarName {
					switch selectorExpr.Sel.Name {
					case "Put", "PutMulti", "Get", "GetMulti", "Delete", "DeleteMulti":
						c.Replace(ast.NewIdent(walker.BoomVarName))
					case "GetAll", "Count", "Run":
						c.Replace(ast.NewIdent(walker.BoomVarName))
					case "Key", "KeyError", "Kind":
						c.Replace(ast.NewIdent(walker.BoomVarName))
					case "RunInTransaction":
						c.Replace(ast.NewIdent(walker.BoomVarName))
					case "Context":
						c.Replace(ast.NewIdent(walker.BoomVarName))
					}
				} else if ident.Name == walker.QueryVarName {
					switch selectorExpr.Sel.Name {
					case "GetAll", "Run":
						c.Replace(ast.NewIdent(walker.BoomVarName))
					}
				} else if ident.Name == walker.PackageNameGoon {
					c.Replace(ast.NewIdent(walker.PackageNameBoom))
				}
			}
		case "Sel":
			if x, ok := selectorExpr.X.(*ast.Ident); ok {
				if sel, ok := c.Node().(*ast.Ident); ok {
					if (x.Name == walker.PackageNameGoon || x.Name == walker.PackageNameBoom) && sel.Name == "Goon" {
						c.Replace(ast.NewIdent("Boom"))
					}
				}
			}
		}
	} else if ident, ok := c.Node().(*ast.Ident); ok {
		if ident.Name == walker.GoonVarName {
			if obj := ident.Obj; obj != nil && obj.Kind == ast.Var {
				if field, ok := obj.Decl.(*ast.Field); ok && (walker.isGoonGoonType(field.Type) || walker.isBoomBoomType(field.Type)) {
					c.Replace(ast.NewIdent(walker.BoomVarName))
				}
			}
		}
	}

	return true
}
