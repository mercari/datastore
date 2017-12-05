// Copyright 4 Google Inc. All Rights Reserved.
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
	"errors"
	"fmt"
	"reflect"
	"time"
)

type saveOpts struct {
	noIndex   bool
	flatten   bool
	omitEmpty bool
}

func saveEntity(ctx context.Context, key Key, src interface{}) (*Entity, error) {
	var err error
	var props []Property
	if e, ok := src.(PropertyLoadSaver); ok {
		props, err = e.Save(ctx)
	} else {
		props, err = SaveStruct(ctx, src)
	}
	if err != nil {
		return nil, err
	}

	entity, err := propertiesToProtoFake(ctx, key, props)
	if err != nil {
		return nil, err
	}
	return entity, nil
}

// TODO(djd): Convert this and below to return ([]Property, error).
func saveStructProperty(ctx context.Context, props *[]Property, name string, opts saveOpts, v reflect.Value) error {
	p := Property{
		Name:    name,
		NoIndex: opts.noIndex,
	}

	if opts.omitEmpty && isEmptyValue(v) {
		return nil
	}

	// First check if field type implements PLS. If so, use PLS to
	// save.
	ok, err := plsFieldSave(ctx, props, p, name, opts, v)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	if v.Type().AssignableTo(typeOfKey) {
		p.Value = v.Interface()

	} else {
		switch x := v.Interface().(type) {
		case time.Time, GeoPoint:
			p.Value = x
		case PropertyTranslator:
			v, err := x.ToPropertyValue(ctx)
			if err != nil {
				return err
			}
			p.Value = v
		default:
			switch v.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				p.Value = v.Int()
			case reflect.Bool:
				p.Value = v.Bool()
			case reflect.String:
				p.Value = v.String()
			case reflect.Float32, reflect.Float64:
				p.Value = v.Float()
			case reflect.Slice:
				if v.Type().Elem().Kind() == reflect.Uint8 {
					p.Value = v.Bytes()
				} else {
					return saveSliceProperty(ctx, props, name, opts, v)
				}
			case reflect.Ptr:
				if v.Type().Elem().Kind() != reflect.Struct {
					return fmt.Errorf("datastore: unsupported struct field type: %s", v.Type())
				}
				if v.IsNil() {
					return nil
				}
				v = v.Elem()
				fallthrough
			case reflect.Struct:
				if !v.CanAddr() {
					return fmt.Errorf("datastore: unsupported struct field: value is unaddressable")
				}
				vi := v.Addr().Interface()

				sub, err := newStructPLS(vi)
				if err != nil {
					return fmt.Errorf("datastore: unsupported struct field: %v", err)
				}

				if opts.flatten {
					return sub.save(ctx, props, opts, name+".")
				}

				var subProps []Property
				err = sub.save(ctx, &subProps, opts, "")
				if err != nil {
					return err
				}
				subKey, err := sub.key(v)
				if err != nil {
					return err
				}

				p.Value = &Entity{
					Key:        subKey,
					Properties: subProps,
				}
			}
		}
		if p.Value == nil {
			return fmt.Errorf("datastore: unsupported struct field type: %v", v.Type())
		}
	}

	*props = append(*props, p)
	return nil
}

// plsFieldSave first tries to converts v's value to a PLS, then v's addressed
// value to a PLS. If neither succeeds, plsFieldSave returns false for first return
// value.
// If v is successfully converted to a PLS, plsFieldSave will then add the
// Value to property p by way of the PLS's Save method, and append it to props.
//
// If the flatten option is present in opts, name must be prepended to each property's
// name before it is appended to props. Eg. if name were "A" and a subproperty's name
// were "B", the resultant name of the property to be appended to props would be "A.B".
func plsFieldSave(ctx context.Context, props *[]Property, p Property, name string, opts saveOpts, v reflect.Value) (ok bool, err error) {
	vpls, err := plsForSave(v)
	if err != nil {
		return false, err
	}

	if vpls == nil {
		return false, nil
	}

	subProps, err := vpls.Save(ctx)
	if err != nil {
		return true, err
	}

	if opts.flatten {
		for _, subp := range subProps {
			subp.Name = name + "." + subp.Name
			*props = append(*props, subp)
		}
		return true, nil
	}

	p.Value = &Entity{Properties: subProps}
	*props = append(*props, p)

	return true, nil
}

// key extracts the Key interface field from struct v based on the structCodec of s.
func (s structPLS) key(v reflect.Value) (Key, error) {
	if v.Kind() != reflect.Struct {
		return nil, errors.New("datastore: cannot save key of non-struct type")
	}

	keyField := s.codec.Match(keyFieldName)

	if keyField == nil {
		return nil, nil
	}

	f := v.FieldByIndex(keyField.Index)
	k, ok := f.Interface().(Key)
	if !ok {
		return nil, fmt.Errorf("datastore: %s field on struct %T is not a datastore.Key", keyFieldName, v.Interface())
	}

	return k, nil
}

func saveSliceProperty(ctx context.Context, props *[]Property, name string, opts saveOpts, v reflect.Value) error {
	// Easy case: if the slice is empty, we're done.
	if v.Len() == 0 {
		return nil
	}
	// Work out the properties generated by the first element in the slice. This will
	// usually be a single property, but will be more if this is a slice of structs.
	var headProps []Property
	if err := saveStructProperty(ctx, &headProps, name, opts, v.Index(0)); err != nil {
		return err
	}

	// Convert the first element's properties into slice properties, and
	// keep track of the values in a map.
	values := make(map[string][]interface{}, len(headProps))
	for _, p := range headProps {
		values[p.Name] = append(make([]interface{}, 0, v.Len()), p.Value)
	}

	// Find the elements for the subsequent elements.
	for i := 1; i < v.Len(); i++ {
		elemProps := make([]Property, 0, len(headProps))
		if err := saveStructProperty(ctx, &elemProps, name, opts, v.Index(i)); err != nil {
			return err
		}
		for _, p := range elemProps {
			v, ok := values[p.Name]
			if !ok {
				return fmt.Errorf("datastore: unexpected property %q in elem %d of slice", p.Name, i)
			}
			values[p.Name] = append(v, p.Value)
		}
	}

	// Convert to the final properties.
	for _, p := range headProps {
		p.Value = values[p.Name]
		*props = append(*props, p)
	}
	return nil
}

func (s structPLS) Save(ctx context.Context) ([]Property, error) {
	var props []Property
	if err := s.save(ctx, &props, saveOpts{}, ""); err != nil {
		return nil, err
	}
	return props, nil
}

func (s structPLS) save(ctx context.Context, props *[]Property, opts saveOpts, prefix string) error {
	for _, f := range s.codec {
		name := prefix + f.Name
		v := getField(s.v, f.Index)
		if !v.IsValid() || !v.CanSet() {
			continue
		}

		var tagOpts saveOpts
		if f.ParsedTag != nil {
			tagOpts = f.ParsedTag.(saveOpts)
		}

		var opts1 saveOpts
		opts1.noIndex = opts.noIndex || tagOpts.noIndex
		opts1.flatten = opts.flatten || tagOpts.flatten
		opts1.omitEmpty = tagOpts.omitEmpty // don't propagate
		if err := saveStructProperty(ctx, props, name, opts1, v); err != nil {
			return err
		}
	}
	return nil
}

// getField returns the field from v at the given index path.
// If it encounters a nil-valued field in the path, getField
// stops and returns a zero-valued reflect.Value, preventing the
// panic that would have been caused by reflect's FieldByIndex.
func getField(v reflect.Value, index []int) reflect.Value {
	var zero reflect.Value
	if v.Type().Kind() != reflect.Struct {
		return zero
	}

	for _, i := range index {
		if v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Struct {
			if v.IsNil() {
				return zero
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

func propertiesToProtoFake(ctx context.Context, key Key, props []Property) (*Entity, error) {
	e := &Entity{
		Key:        key,
		Properties: props,
	}
	for idx, p := range props {
		// Do not send a Key value a a field to datastore.
		if p.Name == keyFieldName {
			continue
		}

		val, err := interfaceToProtoFake(ctx, p.Value, p.NoIndex)
		if err != nil {
			return nil, fmt.Errorf("datastore: %v for a Property with Name %q", err, p.Name)
		}
		props[idx].Value = val
	}
	return e, nil
}

func interfaceToProtoFake(ctx context.Context, iv interface{}, noIndex bool) (interface{}, error) {
	switch v := iv.(type) {
	case time.Time:
		if v.Before(minTime) || v.After(maxTime) {
			return nil, errors.New("time value out of range")
		}
		// This rounding process reproduces the cloud.google.com/go/datastore
		// don't use original time.Time's locale and UTC both. use machine default.
		um := toUnixMicro(v)
		return fromUnixMicro(um), nil
	default:
		return v, nil
	}
}

// isEmptyValue is taken from the encoding/json package in the
// standard library.
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}
