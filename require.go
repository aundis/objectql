package objectql

import (
	"context"
	"fmt"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/util/gconv"
)

func (o *Objectql) checkInsertFieldBoolRequires(object *Object, doc M) error {
	for _, f := range object.Fields {
		if f.Require == true {
			if v, ok := doc[f.Api]; !ok || isNull(v) {
				return fmt.Errorf("object %s field %s is require", object.Api, f.Api)
			}
		}
	}
	return nil
}

func (o *Objectql) checkUpdateFieldBoolRequires(object *Object, doc M) error {
	for _, f := range object.Fields {
		if f.Require == true {
			if v, ok := doc[f.Api]; ok && isNull(v) {
				return fmt.Errorf("object %s field %s is require", object.Api, f.Api)
			}
		}
	}
	return nil
}

func isBoolRequire(field *Field) bool {
	_, ok := field.Require.(bool)
	return ok
}

func (o *Objectql) checkFieldFormulaOrHandledRequires(ctx context.Context, object *Object, cur *Var) error {
	for _, field := range object.Fields {
		if field.Require != nil && !isBoolRequire(field) {
			// 字段非空不需要再进行校验
			if !cur.isNull(field.Api) {
				continue
			}
			switch n := field.Require.(type) {
			case string:
				return o.checkFieldFormulaRequires(ctx, field, cur)
			case *FieldReqireCheckHandle:
				return o.checkFieldHandleeRequires(ctx, n, cur)
			}
		}
	}
	return nil
}

func (o *Objectql) checkFieldFormulaRequires(ctx context.Context, field *Field, cur *Var) error {
	runner := formula.NewRunner()
	runner.SetThis(cur.ToStrAnyMap())
	result, err := runner.Resolve(ctx, field.requireSourceCode.Expression)
	if err != nil {
		return fmt.Errorf("checkFieldFormulaRequires error: %s", field.RequireMsg)
	}
	if gconv.Bool(result) {
		return fmt.Errorf("字段<%s>是必填项", field.Name)
		// return fmt.Errorf("field require error: %s", field.RequireMsg)
	}
	return nil
}

func (o *Objectql) checkFieldHandleeRequires(ctx context.Context, handle *FieldReqireCheckHandle, cur *Var) error {
	return handle.Handle(ctx, cur)
}

func (o *Objectql) getObjectRequireQueryFields(object *Object) []string {
	var result []string
	for _, field := range object.Fields {
		switch n := field.Require.(type) {
		case bool:
			result = append(result, field.Api)
		case string:
			result = append(result, field.requireSourceCodeFields...)
		case *FieldReqireCheckHandle:
			result = append(result, n.Fields...)
		}
	}
	return result
}
