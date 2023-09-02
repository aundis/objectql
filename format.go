package objectql

import (
	"errors"
	"fmt"
	"time"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func formatDatabaseValueToCompute(field *Field, value interface{}) (interface{}, error) {
	simpleHandle := func(tpe FieldType, value interface{}) (interface{}, error) {
		switch tpe {
		case Int:
			return gconv.Int(value), nil
		case Float:
			return gconv.Float32(value), nil
		case Bool:
			return gconv.Bool(value), nil
		case String:
			return gconv.String(value), nil
		case DateTime:
			return formatDatabaseDateTimeValueToCompute(value)
		default:
			return nil, fmt.Errorf("formatOutputValue simpleHandle unknown field type %v", tpe)
		}
	}

	switch field.Type {
	case Int, Float, Bool, String, DateTime:
		return simpleHandle(field.Type, value)
	case Relate:
		return simpleHandle(String, value)
	case Formula:
		data := field.Data.(*FormulaData)
		return simpleHandle(data.Type, value)
	case Aggregation:
		data := field.Data.(*AggregationData)
		return simpleHandle(data.Type, value)
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

func formatValueToDatabase(fields []*Field, doc map[string]interface{}) error {
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
		switch field.Type {
		case Relate:
			cur, err = formatRelateValueToDatebase(cur)
		case DateTime:
			cur, err = formatDateTimeValueToDatebase(cur)
		}
		if err != nil {
			return fmt.Errorf("formatValueToDatabase doc[\"%s\"] error: %s", field.Api, err.Error())
		}
		doc[field.Api] = cur
	}
	return nil
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

func formatComputedValue(field *Field, value interface{}) (interface{}, error) {
	simpleHandle := func(tpe FieldType, value interface{}) (interface{}, error) {
		switch tpe {
		case Int:
			return formula.ToInt(value)
		case Float:
			return formula.ToFloat32(value)
		case Bool:
			return formula.ToString(value)
		case String:
			return formula.ToString(value)
		default:
			return nil, fmt.Errorf("formatComputedValue simpleHandle unknown field type %v", tpe)
		}
	}

	switch field.Type {
	case Int, Float, Bool, String:
		return simpleHandle(field.Type, value)
	case Relate:
		return simpleHandle(String, value)
	case Formula:
		data := field.Data.(*FormulaData)
		return simpleHandle(data.Type, value)
	case Aggregation:
		data := field.Data.(*AggregationData)
		return simpleHandle(data.Type, value)
	default:
		return nil, fmt.Errorf("formatComputedValue unknown field type %v", field.Type)
	}
}

func getFieldComputeDefaultValue(field *Field) (interface{}, error) {
	simpleHandle := func(tpe FieldType) (interface{}, error) {
		switch tpe {
		case Int:
			return 0, nil
		case Float:
			return 0, nil
		case Bool:
			return false, nil
		case String:
			return "", nil
		default:
			return nil, fmt.Errorf("getFieldComputeDefaultValue simpleHandle unknown field type %v", tpe)
		}
	}

	switch field.Type {
	case Int, Float, Bool, String:
		return simpleHandle(field.Type)
	case Relate:
		return simpleHandle(String)
	case Formula:
		data := field.Data.(*FormulaData)
		return simpleHandle(data.Type)
	case Aggregation:
		data := field.Data.(*AggregationData)
		return simpleHandle(data.Type)
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
