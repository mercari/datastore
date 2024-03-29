// Copyright 2014 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"go.mercari.io/datastore/v2/internal/c/fields"
)

// SuppressErrFieldMismatch when this flag is true.
// If you want to align (AE|Cloud) Datastore's default behavior, set false.
var SuppressErrFieldMismatch = true

var _ PropertyLoadSaver = (*PropertyList)(nil)

// Entities with more than this many indexed properties will not be saved.
// const maxIndexedProperties = 20000

// Property is a name/value pair plus some metadata. A datastore entity's
// contents are loaded and saved as a sequence of Properties. Each property
// name must be unique within an entity.
type Property struct {
	// Name is the property name.
	Name string
	// Value is the property value. The valid types are:
	//	- int64
	//	- bool
	//	- string
	//	- float64
	//	- Key
	//	- time.Time (retrieved as local time)
	//	- GeoPoint
	//	- []byte (up to 1 megabyte in length)
	//	- *Entity (representing a nested struct)
	// Value can also be:
	//	- []interface{} where each element is one of the above types
	// This set is smaller than the set of valid struct field types that the
	// datastore can load and save. A Value's type must be explicitly on
	// the list above; it is not sufficient for the underlying type to be
	// on that list. For example, a Value of "type myInt64 int64" is
	// invalid. Smaller-width integers and floats are also invalid. Again,
	// this is more restrictive than the set of valid struct field types.
	//
	// A Value will have an opaque type when loading entities from an index,
	// such as via a projection query. Load entities into a struct instead
	// of a PropertyLoadSaver when using a projection query.
	//
	// A Value may also be the nil interface value; this is equivalent to
	// Python's None but not directly representable by a Go struct. Loading
	// a nil-valued property into a struct will set that field to the zero
	// value.
	Value interface{}
	// NoIndex is whether the datastore cannot index this property.
	// If NoIndex is set to false, []byte and string values are limited to
	// 1500 bytes.
	NoIndex bool
}

// An Entity is the value type for a nested struct.
// This type is only used for a Property's Value.
type Entity struct {
	Key        Key
	Properties []Property
}

// PropertyLoadSaver can be converted from and to a slice of Properties.
type PropertyLoadSaver interface {
	Load(ctx context.Context, ps []Property) error
	Save(ctx context.Context) ([]Property, error)
}

// KeyLoader can store a Key.
type KeyLoader interface {
	// PropertyLoadSaver is embedded because a KeyLoader
	// must also always implement PropertyLoadSaver.
	PropertyLoadSaver
	LoadKey(ctx context.Context, k Key) error
}

// PropertyList converts a []Property to implement PropertyLoadSaver.
type PropertyList []Property

// Load loads all of the provided properties into l.
// It does not first reset *l to an empty slice.
func (l *PropertyList) Load(ctx context.Context, p []Property) error {
	*l = append(*l, p...)
	return nil
}

// Save saves all of l's properties as a slice of Properties.
func (l *PropertyList) Save(ctx context.Context) ([]Property, error) {
	return *l, nil
}

// validPropertyName returns whether name consists of one or more valid Go
// identifiers joined by ".".
func validPropertyName(name string) bool {
	if name == "" {
		return false
	}
	for _, s := range strings.Split(name, ".") {
		if s == "" {
			return false
		}
		first := true
		for _, c := range s {
			if first {
				first = false
				if c != '_' && !unicode.IsLetter(c) {
					return false
				}
			} else {
				if c != '_' && !unicode.IsLetter(c) && !unicode.IsDigit(c) {
					return false
				}
			}
		}
	}
	return true
}

// parseTag interprets datastore struct field tags
func parseTag(t reflect.StructTag) (name string, keep bool, other interface{}, err error) {
	s := t.Get("datastore")
	parts := strings.Split(s, ",")
	if parts[0] == "-" && len(parts) == 1 {
		return "", false, nil, nil
	}
	if parts[0] != "" && !validPropertyName(parts[0]) {
		err = fmt.Errorf("datastore: struct tag has invalid property name: %q", parts[0])
		return "", false, nil, err
	}

	var opts saveOpts
	if len(parts) > 1 {
		for _, p := range parts[1:] {
			switch p {
			case "flatten":
				opts.flatten = true
			case "omitempty":
				opts.omitEmpty = true
			case "noindex":
				opts.noIndex = true
			default:
				err = fmt.Errorf("datastore: struct tag has invalid option: %q", p)
				return "", false, nil, err
			}
		}
		other = opts
	}
	return parts[0], true, other, nil
}

func validateType(t reflect.Type) error {
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("datastore: validate called with non-struct type %s", t)
	}

	return validateChildType(t, "", false, false, map[reflect.Type]bool{})
}

// validateChildType is a recursion helper func for validateType
func validateChildType(t reflect.Type, fieldName string, flatten, prevSlice bool, prevTypes map[reflect.Type]bool) error {
	if prevTypes[t] {
		return nil
	}
	prevTypes[t] = true

	switch t.Kind() {
	case reflect.Slice:
		if flatten && prevSlice {
			return fmt.Errorf("datastore: flattening nested structs leads to a slice of slices: field %q", fieldName)
		}
		return validateChildType(t.Elem(), fieldName, flatten, true, prevTypes)
	case reflect.Struct:
		if t == typeOfTime || t == typeOfGeoPoint {
			return nil
		}

		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)

			// If a named field is unexported, ignore it. An anonymous
			// unexported field is processed, because it may contain
			// exported fields, which are visible.
			exported := (f.PkgPath == "")
			if !exported && !f.Anonymous {
				continue
			}

			_, keep, other, err := parseTag(f.Tag)
			// Handle error from parseTag now instead of later (in cache.Fields call).
			if err != nil {
				return err
			}
			if !keep {
				continue
			}
			if other != nil {
				opts := other.(saveOpts)
				flatten = flatten || opts.flatten
			}
			if err := validateChildType(f.Type, f.Name, flatten, prevSlice, prevTypes); err != nil {
				return err
			}
		}
	case reflect.Ptr:
		if t == typeOfKey {
			return nil
		}
		return validateChildType(t.Elem(), fieldName, flatten, prevSlice, prevTypes)
	}
	return nil
}

// isLeafType determines whether or not a type is a 'leaf type'
// and should not be recursed into, but considered one field.
func isLeafType(t reflect.Type) bool {
	return t == typeOfTime || t == typeOfGeoPoint
}

// structCache collects the structs whose fields have already been calculated.
var structCache = fields.NewCache(parseTag, validateType, isLeafType)

// structPLS adapts a struct to be a PropertyLoadSaver.
type structPLS struct {
	v     reflect.Value
	codec fields.List
}

// newStructPLS returns a structPLS, which implements the
// PropertyLoadSaver interface, for the struct pointer p.
func newStructPLS(p interface{}) (*structPLS, error) {
	v := reflect.ValueOf(p)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return nil, ErrInvalidEntityType
	}
	v = v.Elem()
	f, err := structCache.Fields(v.Type())
	if err != nil {
		return nil, err
	}
	return &structPLS{v, f}, nil
}

// LoadStruct loads the properties from p to dst.
// dst must be a struct pointer.
//
// The values of dst's unmatched struct fields are not modified,
// and matching slice-typed fields are not reset before appending to
// them. In particular, it is recommended to pass a pointer to a zero
// valued struct on each LoadStruct call.
func LoadStruct(ctx context.Context, dst interface{}, p []Property) error {
	x, err := newStructPLS(dst)
	if err != nil {
		return err
	}
	return x.Load(ctx, p)
}

// SaveStruct returns the properties from src as a slice of Properties.
// src must be a struct pointer.
func SaveStruct(ctx context.Context, src interface{}) ([]Property, error) {
	x, err := newStructPLS(src)
	if err != nil {
		return nil, err
	}
	return x.Save(ctx)
}

// plsForLoad tries to convert v to a PropertyLoadSaver.
// If successful, plsForLoad returns a settable v as a PropertyLoadSaver.
//
// plsForLoad is intended to be used with nested struct fields which
// may implement PropertyLoadSaver.
//
// v must be settable.
func plsForLoad(v reflect.Value) (PropertyLoadSaver, error) {
	var nilPtr bool
	if v.Kind() == reflect.Ptr && v.IsNil() {
		nilPtr = true
		v.Set(reflect.New(v.Type().Elem()))
	}

	vpls, err := pls(v)
	if nilPtr && (vpls == nil || err != nil) {
		// unset v
		v.Set(reflect.Zero(v.Type()))
	}

	return vpls, err
}

// plsForSave tries to convert v to a PropertyLoadSaver.
// If successful, plsForSave returns v as a PropertyLoadSaver.
//
// plsForSave is intended to be used with nested struct fields which
// may implement PropertyLoadSaver.
//
// v must be settable.
func plsForSave(v reflect.Value) (PropertyLoadSaver, error) {
	switch v.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Interface, reflect.Chan, reflect.Func:
		// If v is nil, return early. v contains no data to save.
		if v.IsNil() {
			return nil, nil
		}
	}

	return pls(v)
}

func pls(v reflect.Value) (PropertyLoadSaver, error) {
	if v.Kind() != reflect.Ptr {
		if _, ok := v.Interface().(PropertyLoadSaver); ok {
			return nil, fmt.Errorf("datastore: PropertyLoadSaver methods must be implemented on a pointer to %T", v.Interface())
		}

		v = v.Addr()
	}

	vpls, _ := v.Interface().(PropertyLoadSaver)
	return vpls, nil
}

// ptForLoad returns PropertyTranslator and set zero value if needed.
// this function is peculiar to mercari/datastore.
func ptForLoad(v reflect.Value) (PropertyTranslator, error) {
	var nilPtr bool
	if v.Kind() == reflect.Ptr && v.IsNil() {
		nilPtr = true
		v.Set(reflect.New(v.Type().Elem()))
	}

	vpt, err := pt(v)
	if nilPtr && (vpt == nil || err != nil) {
		// unset v
		v.Set(reflect.Zero(v.Type()))
	}

	return vpt, err
}

// pt returns PropertyTranslator from passed reflect.Value.
// this function is peculiar to mercari/datastore.
func pt(v reflect.Value) (PropertyTranslator, error) {
	if v.Kind() != reflect.Ptr {
		vps, ok := v.Interface().(PropertyTranslator)
		if ok {
			return vps, nil
		}

		v = v.Addr()
	}

	vps, _ := v.Interface().(PropertyTranslator)
	return vps, nil
}
