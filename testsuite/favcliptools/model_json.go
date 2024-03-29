// Code generated by jwg -output model_json.go -transcripttag swagger .; DO NOT EDIT

package favcliptools

import (
	"encoding/json"
)

// UserJSON is jsonized struct for User.
type UserJSON struct {
	ID       userID `json:"id,omitempty"`
	Name     string `json:"name,omitempty"`
	MentorID userID `json:"mentorID,omitempty"`
}

// UserJSONList is synonym about []*UserJSON.
type UserJSONList []*UserJSON

// UserPropertyEncoder is property encoder for [1]sJSON.
type UserPropertyEncoder func(src *User, dest *UserJSON) error

// UserPropertyDecoder is property decoder for [1]sJSON.
type UserPropertyDecoder func(src *UserJSON, dest *User) error

// UserPropertyInfo stores property information.
type UserPropertyInfo struct {
	fieldName string
	jsonName  string
	Encoder   UserPropertyEncoder
	Decoder   UserPropertyDecoder
}

// FieldName returns struct field name of property.
func (info *UserPropertyInfo) FieldName() string {
	return info.fieldName
}

// JSONName returns json field name of property.
func (info *UserPropertyInfo) JSONName() string {
	return info.jsonName
}

// UserJSONBuilder convert between User to UserJSON mutually.
type UserJSONBuilder struct {
	_properties        map[string]*UserPropertyInfo
	_jsonPropertyMap   map[string]*UserPropertyInfo
	_structPropertyMap map[string]*UserPropertyInfo
	ID                 *UserPropertyInfo
	Name               *UserPropertyInfo
	MentorID           *UserPropertyInfo
}

// NewUserJSONBuilder make new UserJSONBuilder.
func NewUserJSONBuilder() *UserJSONBuilder {
	jb := &UserJSONBuilder{
		_properties:        map[string]*UserPropertyInfo{},
		_jsonPropertyMap:   map[string]*UserPropertyInfo{},
		_structPropertyMap: map[string]*UserPropertyInfo{},
		ID: &UserPropertyInfo{
			fieldName: "ID",
			jsonName:  "id",
			Encoder: func(src *User, dest *UserJSON) error {
				if src == nil {
					return nil
				}
				dest.ID = src.ID
				return nil
			},
			Decoder: func(src *UserJSON, dest *User) error {
				if src == nil {
					return nil
				}
				dest.ID = src.ID
				return nil
			},
		},
		Name: &UserPropertyInfo{
			fieldName: "Name",
			jsonName:  "name",
			Encoder: func(src *User, dest *UserJSON) error {
				if src == nil {
					return nil
				}
				dest.Name = src.Name
				return nil
			},
			Decoder: func(src *UserJSON, dest *User) error {
				if src == nil {
					return nil
				}
				dest.Name = src.Name
				return nil
			},
		},
		MentorID: &UserPropertyInfo{
			fieldName: "MentorID",
			jsonName:  "mentorID",
			Encoder: func(src *User, dest *UserJSON) error {
				if src == nil {
					return nil
				}
				dest.MentorID = src.MentorID
				return nil
			},
			Decoder: func(src *UserJSON, dest *User) error {
				if src == nil {
					return nil
				}
				dest.MentorID = src.MentorID
				return nil
			},
		},
	}
	jb._structPropertyMap["ID"] = jb.ID
	jb._jsonPropertyMap["id"] = jb.ID
	jb._structPropertyMap["Name"] = jb.Name
	jb._jsonPropertyMap["name"] = jb.Name
	jb._structPropertyMap["MentorID"] = jb.MentorID
	jb._jsonPropertyMap["mentorID"] = jb.MentorID
	return jb
}

// Properties returns all properties on UserJSONBuilder.
func (b *UserJSONBuilder) Properties() []*UserPropertyInfo {
	return []*UserPropertyInfo{
		b.ID,
		b.Name,
		b.MentorID,
	}
}

// AddAll adds all property to UserJSONBuilder.
func (b *UserJSONBuilder) AddAll() *UserJSONBuilder {
	b._properties["ID"] = b.ID
	b._properties["Name"] = b.Name
	b._properties["MentorID"] = b.MentorID
	return b
}

// Add specified property to UserJSONBuilder.
func (b *UserJSONBuilder) Add(infos ...*UserPropertyInfo) *UserJSONBuilder {
	for _, info := range infos {
		b._properties[info.fieldName] = info
	}
	return b
}

// AddByJSONNames add properties to UserJSONBuilder by JSON property name. if name is not in the builder, it will ignore.
func (b *UserJSONBuilder) AddByJSONNames(names ...string) *UserJSONBuilder {
	for _, name := range names {
		info := b._jsonPropertyMap[name]
		if info == nil {
			continue
		}
		b._properties[info.fieldName] = info
	}
	return b
}

// AddByNames add properties to UserJSONBuilder by struct property name. if name is not in the builder, it will ignore.
func (b *UserJSONBuilder) AddByNames(names ...string) *UserJSONBuilder {
	for _, name := range names {
		info := b._structPropertyMap[name]
		if info == nil {
			continue
		}
		b._properties[info.fieldName] = info
	}
	return b
}

// Remove specified property to UserJSONBuilder.
func (b *UserJSONBuilder) Remove(infos ...*UserPropertyInfo) *UserJSONBuilder {
	for _, info := range infos {
		delete(b._properties, info.fieldName)
	}
	return b
}

// RemoveByJSONNames remove properties to UserJSONBuilder by JSON property name. if name is not in the builder, it will ignore.
func (b *UserJSONBuilder) RemoveByJSONNames(names ...string) *UserJSONBuilder {

	for _, name := range names {
		info := b._jsonPropertyMap[name]
		if info == nil {
			continue
		}
		delete(b._properties, info.fieldName)
	}
	return b
}

// RemoveByNames remove properties to UserJSONBuilder by struct property name. if name is not in the builder, it will ignore.
func (b *UserJSONBuilder) RemoveByNames(names ...string) *UserJSONBuilder {
	for _, name := range names {
		info := b._structPropertyMap[name]
		if info == nil {
			continue
		}
		delete(b._properties, info.fieldName)
	}
	return b
}

// Convert specified non-JSON object to JSON object.
func (b *UserJSONBuilder) Convert(orig *User) (*UserJSON, error) {
	if orig == nil {
		return nil, nil
	}
	ret := &UserJSON{}

	for _, info := range b._properties {
		if err := info.Encoder(orig, ret); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

// ConvertList specified non-JSON slice to JSONList.
func (b *UserJSONBuilder) ConvertList(orig []*User) (UserJSONList, error) {
	if orig == nil {
		return nil, nil
	}

	list := make(UserJSONList, len(orig))
	for idx, or := range orig {
		json, err := b.Convert(or)
		if err != nil {
			return nil, err
		}
		list[idx] = json
	}

	return list, nil
}

// Convert specified JSON object to non-JSON object.
func (orig *UserJSON) Convert() (*User, error) {
	ret := &User{}

	b := NewUserJSONBuilder().AddAll()
	for _, info := range b._properties {
		if err := info.Decoder(orig, ret); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

// Convert specified JSONList to non-JSON slice.
func (jsonList UserJSONList) Convert() ([]*User, error) {
	orig := ([]*UserJSON)(jsonList)

	list := make([]*User, len(orig))
	for idx, or := range orig {
		obj, err := or.Convert()
		if err != nil {
			return nil, err
		}
		list[idx] = obj
	}

	return list, nil
}

// Marshal non-JSON object to JSON string.
func (b *UserJSONBuilder) Marshal(orig *User) ([]byte, error) {
	ret, err := b.Convert(orig)
	if err != nil {
		return nil, err
	}
	return json.Marshal(ret)
}
