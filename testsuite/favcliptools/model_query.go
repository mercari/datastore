// Code generated by qbg -output model_query.go -usedatastorewrapper .; DO NOT EDIT

package favcliptools

import (
	"go.mercari.io/datastore"
)

// Plugin supply hook point for query constructions.
type Plugin interface {
	Init(typeName string)
	Ancestor(ancestor datastore.Key)
	KeysOnly()
	Start(cur datastore.Cursor)
	Offset(offset int)
	Limit(limit int)
	Filter(name, op string, value interface{})
	Asc(name string)
	Desc(name string)
}

// Plugger supply Plugin component.
type Plugger interface {
	Plugin() Plugin
}

// UserQueryBuilder build query for User.
type UserQueryBuilder struct {
	q        datastore.Query
	plugin   Plugin
	ID       *UserQueryProperty
	Name     *UserQueryProperty
	MentorID *UserQueryProperty
}

// UserQueryProperty has property information for UserQueryBuilder.
type UserQueryProperty struct {
	bldr *UserQueryBuilder
	name string
}

// NewUserQueryBuilder create new UserQueryBuilder.
func NewUserQueryBuilder(client datastore.Client) *UserQueryBuilder {
	return NewUserQueryBuilderWithKind(client, "User")
}

// NewUserQueryBuilderWithKind create new UserQueryBuilder with specific kind.
func NewUserQueryBuilderWithKind(client datastore.Client, kind string) *UserQueryBuilder {
	q := client.NewQuery(kind)
	bldr := &UserQueryBuilder{q: q}
	bldr.ID = &UserQueryProperty{
		bldr: bldr,
		name: "__key__",
	}
	bldr.Name = &UserQueryProperty{
		bldr: bldr,
		name: "Name",
	}
	bldr.MentorID = &UserQueryProperty{
		bldr: bldr,
		name: "MentorID",
	}

	if plugger, ok := interface{}(bldr).(Plugger); ok {
		bldr.plugin = plugger.Plugin()
		bldr.plugin.Init("User")
	}

	return bldr
}

// Ancestor sets parent key to ancestor query.
func (bldr *UserQueryBuilder) Ancestor(parentKey datastore.Key) *UserQueryBuilder {
	bldr.q = bldr.q.Ancestor(parentKey)
	if bldr.plugin != nil {
		bldr.plugin.Ancestor(parentKey)
	}
	return bldr
}

// KeysOnly sets keys only option to query.
func (bldr *UserQueryBuilder) KeysOnly() *UserQueryBuilder {
	bldr.q = bldr.q.KeysOnly()
	if bldr.plugin != nil {
		bldr.plugin.KeysOnly()
	}
	return bldr
}

// Start setup to query.
func (bldr *UserQueryBuilder) Start(cur datastore.Cursor) *UserQueryBuilder {
	bldr.q = bldr.q.Start(cur)
	if bldr.plugin != nil {
		bldr.plugin.Start(cur)
	}
	return bldr
}

// Offset setup to query.
func (bldr *UserQueryBuilder) Offset(offset int) *UserQueryBuilder {
	bldr.q = bldr.q.Offset(offset)
	if bldr.plugin != nil {
		bldr.plugin.Offset(offset)
	}
	return bldr
}

// Limit setup to query.
func (bldr *UserQueryBuilder) Limit(limit int) *UserQueryBuilder {
	bldr.q = bldr.q.Limit(limit)
	if bldr.plugin != nil {
		bldr.plugin.Limit(limit)
	}
	return bldr
}

// Query returns *datastore.Query.
func (bldr *UserQueryBuilder) Query() datastore.Query {
	return bldr.q
}

// Filter with op & value.
func (p *UserQueryProperty) Filter(op string, value interface{}) *UserQueryBuilder {
	switch op {
	case "<=":
		p.LessThanOrEqual(value)
	case ">=":
		p.GreaterThanOrEqual(value)
	case "<":
		p.LessThan(value)
	case ">":
		p.GreaterThan(value)
	case "=":
		p.Equal(value)
	default:
		p.bldr.q = p.bldr.q.Filter(p.name+" "+op, value) // error raised by native query
	}
	if p.bldr.plugin != nil {
		p.bldr.plugin.Filter(p.name, op, value)
	}
	return p.bldr
}

// LessThanOrEqual filter with value.
func (p *UserQueryProperty) LessThanOrEqual(value interface{}) *UserQueryBuilder {
	p.bldr.q = p.bldr.q.Filter(p.name+" <=", value)
	if p.bldr.plugin != nil {
		p.bldr.plugin.Filter(p.name, "<=", value)
	}
	return p.bldr
}

// GreaterThanOrEqual filter with value.
func (p *UserQueryProperty) GreaterThanOrEqual(value interface{}) *UserQueryBuilder {
	p.bldr.q = p.bldr.q.Filter(p.name+" >=", value)
	if p.bldr.plugin != nil {
		p.bldr.plugin.Filter(p.name, ">=", value)
	}
	return p.bldr
}

// LessThan filter with value.
func (p *UserQueryProperty) LessThan(value interface{}) *UserQueryBuilder {
	p.bldr.q = p.bldr.q.Filter(p.name+" <", value)
	if p.bldr.plugin != nil {
		p.bldr.plugin.Filter(p.name, "<", value)
	}
	return p.bldr
}

// GreaterThan filter with value.
func (p *UserQueryProperty) GreaterThan(value interface{}) *UserQueryBuilder {
	p.bldr.q = p.bldr.q.Filter(p.name+" >", value)
	if p.bldr.plugin != nil {
		p.bldr.plugin.Filter(p.name, ">", value)
	}
	return p.bldr
}

// Equal filter with value.
func (p *UserQueryProperty) Equal(value interface{}) *UserQueryBuilder {
	p.bldr.q = p.bldr.q.Filter(p.name+" =", value)
	if p.bldr.plugin != nil {
		p.bldr.plugin.Filter(p.name, "=", value)
	}
	return p.bldr
}

// Asc order.
func (p *UserQueryProperty) Asc() *UserQueryBuilder {
	p.bldr.q = p.bldr.q.Order(p.name)
	if p.bldr.plugin != nil {
		p.bldr.plugin.Asc(p.name)
	}
	return p.bldr
}

// Desc order.
func (p *UserQueryProperty) Desc() *UserQueryBuilder {
	p.bldr.q = p.bldr.q.Order("-" + p.name)
	if p.bldr.plugin != nil {
		p.bldr.plugin.Desc(p.name)
	}
	return p.bldr
}
