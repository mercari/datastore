package migrator

import (
	"go/ast"

	"golang.org/x/tools/go/ast/astutil"
)

type GoonWalker struct {
	*Walker
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
	} else if ident.Name != walker.packageNameGoon {
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
	} else if ident.Name != walker.goonVarName {
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
			c.Replace(ast.NewIdent(walker.boomVarName))
			c.InsertAfter(ast.NewIdent("_"))
			return true
		}
	}
	if walker.isGoonRunInTransactionStmt(c.Node()) {
		assignStmt := c.Node().(*ast.AssignStmt)
		walker.willModify[assignStmt.Lhs[0]] = func(c *astutil.Cursor) bool {
			c.InsertBefore(ast.NewIdent(walker.commitVarName))
			return true
		}
		callExpr := assignStmt.Rhs[0].(*ast.CallExpr)
		funcLit := callExpr.Args[0].(*ast.FuncLit)
		funcArg := funcLit.Type.Params.List[0]
		walker.willModify[funcArg.Names[0]] = func(c *astutil.Cursor) bool {
			ident := c.Node().(*ast.Ident)
			if ident.Name == walker.goonTxName {
				c.Replace(ast.NewIdent(walker.txVarName))
			}

			return true
		}
		walker.willModify[funcArg.Type] = func(c *astutil.Cursor) bool {
			txType := &ast.StarExpr{
				X: &ast.SelectorExpr{
					X:   ast.NewIdent(walker.packageNameBoom),
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
				if sel.Name == walker.goonTxName {
					switch selectorExpr.Sel.Name {
					case "Put", "PutMulti", "Get", "GetMulti", "Delete", "DeleteMulti", "NewQuery":
						c.Replace(ast.NewIdent(walker.txVarName))
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
				if ident.Name == walker.goonVarName {
					switch selectorExpr.Sel.Name {
					case "Put", "PutMulti", "Get", "GetMulti", "Delete", "DeleteMulti":
						c.Replace(ast.NewIdent(walker.boomVarName))
					case "GetAll", "Count", "Run":
						c.Replace(ast.NewIdent(walker.boomVarName))
					case "Key", "KeyError", "Kind":
						c.Replace(ast.NewIdent(walker.boomVarName))
					case "RunInTransaction":
						c.Replace(ast.NewIdent(walker.boomVarName))
					}
				} else if ident.Name == walker.queryVarName {
					switch selectorExpr.Sel.Name {
					case "GetAll", "Run":
						c.Replace(ast.NewIdent(walker.boomVarName))
					}
				} else if ident.Name == walker.packageNameGoon {
					c.Replace(ast.NewIdent(walker.packageNameBoom))
				}
			}
		case "Sel":
			if x, ok := selectorExpr.X.(*ast.Ident); ok {
				if sel, ok := c.Node().(*ast.Ident); ok {
					if x.Name == walker.packageNameGoon && sel.Name == "Goon" {
						c.Replace(ast.NewIdent("Boom"))
					}
				}
			}
		}
	}

	return true
}
