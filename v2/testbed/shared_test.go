package testbed

var _ EntityInterface = &PutInterfaceTest{}

type EntityInterface interface {
	Kind() string
	ID() string
}

type PutInterfaceTest struct {
	kind string
	id   string
}

func (e *PutInterfaceTest) Kind() string {
	return e.kind
}
func (e *PutInterfaceTest) ID() string {
	return e.id
}
