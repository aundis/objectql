package objectql

import (
	"context"
	"fmt"
	"reflect"

	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/container/gmap"
)

type ListenChangeHandler struct {
	Listen     []string
	Query      []string
	UpdateOnly bool
	Handle     func(ctx context.Context, change map[string]bool, entity *Var, before *Var) error
}

func (o *Objectql) ListenChange(table string, handle *ListenChangeHandler) {
	if !o.eventMap.Contains(table) {
		o.eventMap.Set(table, gmap.NewIntAnyMap(true))
	}
	handleMap := o.eventMap.Get(table).(*gmap.IntAnyMap)
	if !handleMap.Contains(int(FieldChange)) {
		handleMap.Set(int(FieldChange), garray.NewArray(true))
	}
	array := handleMap.Get(int(FieldChange)).(*garray.Array)
	array.Append(handle)
}

func (o *Objectql) UnListenChange(table string, handle *ListenChangeHandler) {
	if !o.eventMap.Contains(table) {
		return
	}
	handleMap := o.eventMap.Get(table).(*gmap.IntAnyMap)
	if !handleMap.Contains(int(FieldChange)) {
		return
	}
	array := handleMap.Get(int(FieldChange)).(*garray.Array)
	array.RemoveValue(handle)
}

func (o *Objectql) triggerChange(ctx context.Context, object *Object, before *Var, after *Var, inUpdateMutation bool) error {
	for _, handle := range o.getEventHanders(ctx, object.Api, FieldChange) {
		ins := handle.(*ListenChangeHandler)
		if ins.UpdateOnly && !inUpdateMutation {
			continue
		}
		change := map[string]bool{}
		hasChange := false
		for _, fieldStr := range ins.Listen {
			field := FindFieldFromObject(object, fieldStr)
			if field == nil {
				return fmt.Errorf("not found field %s in object %s", fieldStr, object.Api)
			}
			equal, err := isFieldValueEqual(field.Type, before.Var(field.Api), after.Var(field.Api))
			if err != nil {
				return err
			}
			change[fieldStr] = !equal
			hasChange = hasChange || change[fieldStr]
		}
		// 有改变的字段触发handle
		if hasChange {
			err := ins.Handle(ctx, change, after, before)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func isFieldValueEqual(tpe Type, v1 *Var, v2 *Var) (bool, error) {
	if v1 == v2 || isNull(v1) && isNull(v2) {
		return true, nil
	}
	if isNull(v1) {
		return false, nil
	}
	if isNull(v2) {
		return false, nil
	}
	switch n := tpe.(type) {
	case *IntType:
		return v1.ToInt() == v2.ToInt(), nil
	case *FloatType:
		return v1.ToFloat32() == v2.ToFloat32(), nil
	case *StringType:
		return v1.ToString() == v2.ToString(), nil
	case *BoolType:
		return v1.ToBool() == v2.ToBool(), nil
	case *RelateType:
		return v1.ToString() == v2.ToString(), nil
	case *DateTimeType:
		return v1.ToTime().Equal(v2.ToTime()), nil
	case *ArrayType:
		return isArrayFieldValueEqual(n.Type, v1, v2)
	case *FormulaType:
		return isFieldValueEqual(n.Type, v1, v2)
	case *AggregationType:
		return isFieldValueEqual(n.Type, v1, v2)
	default:
		return false, fmt.Errorf("isFieldValueEqual not support type %T", tpe)
	}
}

func isArrayFieldValueEqual(tpe Type, v1 *Var, v2 *Var) (bool, error) {
	sourceValue1 := reflect.ValueOf(v1.ToAny())
	sourceValue2 := reflect.ValueOf(v2.ToAny())
	if sourceValue1.Len() != sourceValue2.Len() {
		return false, nil
	}
	if !isArrayLikeType(sourceValue1.Type()) {
		return false, nil
	}
	if !isArrayLikeType(sourceValue2.Type()) {
		return false, nil
	}
	if sourceValue1.Type().Kind() != sourceValue2.Kind() {
		return false, nil
	}

	for i := 0; i < sourceValue1.Len(); i++ {
		equal, err := isFieldValueEqual(tpe, NewVar(sourceValue1.Index(i).Interface()), NewVar(sourceValue2.Index(i).Interface()))
		if err != nil {
			return false, err
		}
		if !equal {
			return false, nil
		}
	}
	return true, nil
}

func isArrayLikeType(tpe reflect.Type) bool {
	switch tpe.Kind() {
	case reflect.Array, reflect.Slice:
		return true
	}
	return false
}
