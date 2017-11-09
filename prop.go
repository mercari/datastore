// Copyright 2014 Google Inc. All Rights Reserved.
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

	"go.mercari.io/datastore/internal/c/fields"
)

var SuppressErrFieldMismatch = true

var _ PropertyLoadSaver = (*PropertyList)(nil)

type Property struct {
	Name    string
	Value   interface{}
	NoIndex bool
}

type Entity struct {
	Key        Key
	Properties []Property
}

type PropertyLoadSaver interface {
	Load(ctx context.Context, ps []Property) error
	Save(ctx context.Context) ([]Property, error)
}

type KeyLoader interface {
	PropertyLoadSaver
	LoadKey(ctx context.Context, k Key) error
}

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

// validateChildType is a recursion helper func for ValidateType
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
			// Handle error from ParseTag now instead of later (in cache.Fields call).
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

// IsLeafType determines whether or not a type is a 'leaf type'
// and should not be recursed into, but considered one field.
func isLeafType(t reflect.Type) bool {
	return t == typeOfTime || t == typeOfGeoPoint
}

// structCache collects the structs whose fields have already been calculated.
var structCache = fields.NewCache(parseTag, validateType, isLeafType)

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
			return nil, fmt.Errorf("datastore: PropertyLoadSaver methods must be implemented on a pointer to %T.", v.Interface())
		}

		v = v.Addr()
	}

	vpls, _ := v.Interface().(PropertyLoadSaver)
	return vpls, nil
}

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
