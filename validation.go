package main

import "fmt"

func (o *Objectql) validateDocument(object *Object, doc map[string]interface{}) error {
	exist := map[string]bool{}
	for k := range doc {
		exist[k] = false
	}
	for _, field := range object.Fields {
		if v, ok := doc[field.Api]; ok {
			exist[field.Api] = true
			if !o.validateAssignable(field, v) {
				return fmt.Errorf("%T not assign to %s.%s", v, object.Api, field.Api)
			}
		}
	}
	return nil
}

// TODO: 可以支持字段自定义校验(扩展校验这一层要先通过)
func (o *Objectql) validateAssignable(field *Field, value interface{}) bool {
	simple := func(tpe FieldType, value interface{}) bool {
		switch tpe {
		case Int:
			return isIntLike(value)
		case Float:
			return isFloatLike(value)
		case String:
			return isStringLick(value)
		case Bool:
			return isBoolLike(value)
		default:
			return false
		}
	}

	switch field.Type {
	case Int, Float, String, Bool:
		return simple(field.Type, value)
	case Relate:
		return value == nil || isStringLick(value)
	case Formula:
		data := field.Data.(*FormulaData)
		return simple(data.Type, value)
	case Aggregation:
		data := field.Data.(*AggregationData)
		return simple(data.Type, value)
	}
	return false
}

func isIntLike(value interface{}) bool {
	switch value.(type) {
	case int, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func isFloatLike(value interface{}) bool {
	switch value.(type) {
	case int, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
	default:
		return false
	}
}

func isStringLick(value interface{}) bool {
	switch value.(type) {
	case string:
		return true
	default:
		return false
	}
}

func isBoolLike(value interface{}) bool {
	switch value.(type) {
	case bool:
		return true
	default:
		return false
	}
}
