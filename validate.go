package objectql

import (
	"fmt"
	"time"
)

func (o *Objectql) validateDocument(object *Object, doc map[string]interface{}) error {
	exist := map[string]bool{}
	for k := range doc {
		exist[k] = false
	}
	for _, field := range object.Fields {
		if v, ok := doc[field.Api]; ok {
			exist[field.Api] = true
			if !o.validateAssignable(field, v) {
				return fmt.Errorf("validateDocument %T not assign to %s.%s", v, object.Api, field.Api)
			}
		}
	}
	return nil
}

// TODO: 可以支持字段自定义校验(扩展校验这一层要先通过)
func (o *Objectql) validateAssignable(field *Field, value interface{}) bool {
	// TODO: 允许所有字段为空
	if value == nil {
		return true
	}

	simple := func(tpe Type, value interface{}) bool {
		switch tpe.(type) {
		case *IntType:
			return isIntLike(value)
		case *FloatType:
			return isFloatLike(value)
		case *StringType:
			return isStringLick(value)
		case *BoolType:
			return isBoolLike(value)
		case *DateTimeType, *DateType, *TimeType:
			return isDateTimeLike(value)
		default:
			return false
		}
	}

	switch n := field.Type.(type) {
	case *IntType, *FloatType, *StringType, *BoolType, *DateTimeType:
		return simple(field.Type, value)
	case *RelateType:
		return value == nil || isStringLick(value)
	case *FormulaType:
		return simple(n.Type, value)
	case *AggregationType:
		return simple(n.Type, value)
	}
	return true
}

func isIntLike(value interface{}) bool {
	switch value.(type) {
	case int, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
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

func isDateTimeLike(value interface{}) bool {
	switch value.(type) {
	case string, *time.Time, time.Time:
		return true
	default:
		return false
	}
}
