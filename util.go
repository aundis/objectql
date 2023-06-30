package main

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
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

func formatInputValue(fields []*Field, doc map[string]interface{}) error {
	if doc == nil {
		return errors.New("FormatInputValue doc not be null")
	}
	// 将对象id字符串转为bson.ObjectId
	for _, field := range fields {
		if field.Type == Relate {
			if v, ok := doc[field.Api]; ok {
				if v2, ok := v.(string); ok {
					if objectId, err := primitive.ObjectIDFromHex(v2); err == nil {
						doc[field.Api] = objectId
					} else {
						return fmt.Errorf("FormatInputValue doc[\"%s\"] not valid object id hex", field.Api)
					}
				}
			}
		}
	}
	return nil
}
