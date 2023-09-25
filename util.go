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

func IsObjectIDType(tpe Type) bool {
	_, ok := tpe.(*ObjectIDType)
	return ok
}

func IsIntType(tpe Type) bool {
	_, ok := tpe.(*IntType)
	return ok
}

func IsStringType(tpe Type) bool {
	_, ok := tpe.(*StringType)
	return ok
}

func IsBoolType(tpe Type) bool {
	_, ok := tpe.(*BoolType)
	return ok
}

func IsFloatType(tpe Type) bool {
	_, ok := tpe.(*FloatType)
	return ok
}

func IsDateTimeType(tpe Type) bool {
	_, ok := tpe.(*DateTimeType)
	return ok
}

func IsRelateType(tpe Type) bool {
	_, ok := tpe.(*RelateType)
	return ok
}

func IsFormulaType(tpe Type) bool {
	_, ok := tpe.(*FormulaType)
	return ok
}

func IsArrayType(tpe Type) bool {
	_, ok := tpe.(*ArrayType)
	return ok
}

func IsAggregationType(tpe Type) bool {
	_, ok := tpe.(*AggregationType)
	return ok
}

func IsExpandType(tpe Type) bool {
	_, ok := tpe.(*ExpandType)
	return ok
}
