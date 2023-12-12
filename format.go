package objectql

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func formatDatabaseValueToCompute(field *Field, value interface{}) (interface{}, error) {
	simpleHandle := func(tpe Type, value interface{}) (interface{}, error) {
		switch tpe.(type) {
		case *IntType:
			return gconv.Int(value), nil
		case *FloatType:
			return gconv.Float32(value), nil
		case *BoolType:
			return gconv.Bool(value), nil
		case *StringType:
			return gconv.String(value), nil
		case *DateTimeType:
			return formatDatabaseDateTimeValueToCompute(value)
		default:
			return nil, fmt.Errorf("formatOutputValue simpleHandle unknown field type %v", tpe)
		}
	}

	switch n := field.Type.(type) {
	case *IntType, *FloatType, *BoolType, *StringType, *DateTimeType:
		return simpleHandle(field.Type, value)
	case *RelateType:
		return simpleHandle(String, value)
	case *FormulaType:
		return simpleHandle(n.Type, value)
	case *AggregationType:
		return simpleHandle(n.Type, value)
	default:
		return nil, fmt.Errorf("formatFormulaReturnValue unknown field type %v", field.Type)
	}
}

func formatDatabaseDateTimeValueToCompute(v interface{}) (interface{}, error) {
	switch n := v.(type) {
	case primitive.DateTime:
		return n.Time(), nil
	default:
		return nil, fmt.Errorf("formatDatabaseDateTimeValueToCompute type error %v", v)
	}
}

func formatDocumentToDatabase(fields []*Field, doc map[string]interface{}) error {
	if doc == nil {
		return errors.New("FormatInputValue doc not be null")
	}
	// 将对象id字符串转为bson.ObjectId
	for _, field := range fields {
		var err error
		cur, ok := doc[field.Api]
		if !ok {
			continue
		}
		res, err := formatValueToDatabase(field.Type, cur)
		if err != nil {
			return fmt.Errorf("formatValueToDatabase doc[\"%s\"] error: %s", field.Api, err.Error())
		}
		doc[field.Api] = res
	}
	return nil
}

func formatValueToDatabase(tpe Type, value interface{}) (interface{}, error) {
	switch n := tpe.(type) {
	case *IntType:
		return formatIntValueToDatebase(value)
	case *FloatType:
		return formatFloatValueToDatebase(value)
	case *StringType:
		return formatStringValueToDatebase(value)
	case *BoolType:
		return formatBooleanValueToDatebase(value)
	case *RelateType:
		return formatRelateValueToDatebase(value)
	case *DateTimeType:
		return formatDateTimeValueToDatebase(value)
	case *ArrayType:
		return formatArrayValueToDatebase(n, value)
	case *FormulaType:
		return formatValueToDatabase(n.Type, value)
	case *AggregationType:
		return formatValueToDatabase(n.Type, value)
	default:
		return nil, fmt.Errorf("formatValueToDatabase not support type(%T)", tpe)
	}
}

func formatIntValueToDatebase(v interface{}) (interface{}, error) {
	if isNull(v) {
		return nil, nil
	}
	return gconv.Int(v), nil
}

func formatFloatValueToDatebase(v interface{}) (interface{}, error) {
	if isNull(v) {
		return nil, nil
	}
	return gconv.Float32(v), nil
}

func formatStringValueToDatebase(v interface{}) (interface{}, error) {
	if isNull(v) {
		return nil, nil
	}
	return gconv.String(v), nil
}

func formatBooleanValueToDatebase(v interface{}) (interface{}, error) {
	if isNull(v) {
		return nil, nil
	}
	return gconv.Bool(v), nil
}

func formatRelateValueToDatebase(v interface{}) (interface{}, error) {
	if v2, ok := v.(string); ok {
		return primitive.ObjectIDFromHex(v2)
	}
	return v, nil
}

func formatDateTimeValueToDatebase(v interface{}) (interface{}, error) {
	switch n := v.(type) {
	case string:
		return gtime.StrToTime(n)
	case time.Time:
		return primitive.NewDateTimeFromTime(n), nil
	default:
		return v, nil
	}
}

func formatArrayValueToDatebase(at *ArrayType, v interface{}) (interface{}, error) {
	sourceValue := reflect.ValueOf(v)
	if sourceValue.Type() != nil && sourceValue.Type().Kind() != reflect.Array && sourceValue.Type().Kind() != reflect.Slice {
		return nil, fmt.Errorf("formatArrayValueToDatebase can't conv type %T to array", v)
	}
	sliceValue := reflect.MakeSlice(reflect.TypeOf([]any{}), 0, 0)
	for i := 0; i < sourceValue.Len(); i++ {
		evalue, err := formatValueToDatabase(at.Type, sourceValue.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		sliceValue = reflect.Append(sliceValue, reflect.ValueOf(evalue))
	}
	return sliceValue.Interface(), nil
}

func formatComputedValue(tpe Type, value interface{}) (interface{}, error) {
	switch n := tpe.(type) {
	case *IntType:
		return gconv.Int(value), nil
	case *FloatType:
		return gconv.Float32(value), nil
	case *BoolType:
		return gconv.Bool(value), nil
	case *StringType:
		return gconv.String(value), nil
	case *RelateType:
		return formatComputedValue(String, value)
	case *FormulaType:
		return formatComputedValue(n.Type, value)
	case *AggregationType:
		return formatComputedValue(n.Type, value)
	case *ArrayType:
		return formatComputedArrayValue(n, value)
	default:
		return nil, fmt.Errorf("formatComputedValue unknown field type %T", tpe)
	}
}

func formatComputedArrayValue(at *ArrayType, value interface{}) (interface{}, error) {
	sourceValue := reflect.ValueOf(value)
	if sourceValue.Type() != nil && sourceValue.Type().Kind() != reflect.Array && sourceValue.Type().Kind() != reflect.Slice {
		return nil, fmt.Errorf("formatComputedArrayValue can't conv type %T to array", value)
	}
	sliceValue := reflect.MakeSlice(reflect.TypeOf([]any{}), 0, 0)
	for i := 0; i < sourceValue.Len(); i++ {
		evalue, err := formatComputedValue(at.Type, sourceValue.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		sliceValue = reflect.Append(sliceValue, reflect.ValueOf(evalue))
	}
	return sliceValue.Interface(), nil
}

func getFieldComputeDefaultValue(field *Field) (interface{}, error) {
	simpleHandle := func(tpe Type) (interface{}, error) {
		switch tpe.(type) {
		case *IntType:
			return 0, nil
		case *FloatType:
			return 0, nil
		case *BoolType:
			return false, nil
		case *StringType:
			return "", nil
		default:
			return nil, fmt.Errorf("getFieldComputeDefaultValue simpleHandle unknown field type %v", tpe)
		}
	}

	switch n := field.Type.(type) {
	case *IntType, *FloatType, *BoolType, *StringType:
		return simpleHandle(field.Type)
	case *RelateType:
		return simpleHandle(String)
	case *FormulaType:
		return simpleHandle(n.Type)
	case *AggregationType:
		return simpleHandle(n.Type)
	default:
		return nil, fmt.Errorf("getFieldComputeDefaultValue unknown field type %v", field.Type)
	}
}

func boolOrNil(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return gconv.Bool(v)
}

func intOrNil(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return gconv.Int(v)
}

func floatOrNil(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return gconv.Float32(v)
}

func stringOrNil(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return gconv.String(v)
}

func dateTimeOrNil(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	return gconv.Time(v)
}
