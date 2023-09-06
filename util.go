package objectql

import (
	"fmt"
	"reflect"
)

func FindObjectFromList(list []*Object, api string) *Object {
	for _, item := range list {
		if item.Api == api {
			return item
		}
	}
	return nil
}

func FindFieldFromObject(object *Object, api string) *Field {
	for _, field := range object.Fields {
		if field.Api == api {
			return field
		}
	}
	return nil
}

func FindFieldFromName(list []*Object, object, field string) (*Field, error) {
	o := FindObjectFromList(list, object)
	if o == nil {
		return nil, fmt.Errorf("can't find object '%s'", object)
	}
	f := FindFieldFromObject(o, field)
	if f == nil {
		return nil, fmt.Errorf("can't find field '%s' from object '%s'", field, object)
	}
	return f, nil
}

func isNull(i interface{}) bool {
	if i == nil {
		return true
	}
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	return false
}
