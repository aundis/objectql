package objectql

import (
	"context"
	"fmt"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/util/gconv"
)

func (o *Objectql) checkFieldFormulaOrHandledUpdateables(ctx context.Context, object *Object, cur *Var, before *Var) error {
	for _, field := range object.Fields {
		if field.Updateable != nil {
			switch n := field.Updateable.(type) {
			case string:
				return o.checkFieldFormulaUpdateable(ctx, field, cur, before)
			case *FieldUpdateableHandle:
				return o.checkFieldHandleUpdateable(ctx, field, n, cur, before)
			}
		}
	}
	return nil
}

func (o *Objectql) checkFieldFormulaUpdateable(ctx context.Context, field *Field, cur *Var, before *Var) error {
	err := o.checkFieldFormulaUpdateableHandle(ctx, field, cur)
	if err == nil {
		return nil
	}
	err = o.checkFieldFormulaUpdateableHandle(ctx, field, before)
	if err == nil {
		return nil
	}
	return err
}

func (o *Objectql) checkFieldFormulaUpdateableHandle(ctx context.Context, field *Field, cur *Var) error {
	runner := formula.NewRunner()
	runner.SetThis(cur.ToStrAnyMap())
	result, err := runner.Resolve(ctx, field.updateableSourceCode.Expression)
	if err != nil {
		return fmt.Errorf("checkFieldFormulaRequires error: %s", field.RequireMsg)
	}
	if !gconv.Bool(result) {
		return fmt.Errorf("字段<%s>禁止修改: %s", field.Name, field.UpdateableMsg)
	}
	return nil
}

func (o *Objectql) checkFieldHandleUpdateable(ctx context.Context, field *Field, handle *FieldUpdateableHandle, cur *Var, before *Var) error {
	var errs []error
	err := handle.Handle(ctx, cur)
	if err == nil {
		return nil
	} else {
		errs = append(errs, err)
	}
	err = handle.Handle(ctx, before)
	if err == nil {
		return nil
	} else {
		errs = append(errs, err)
	}
	return fmt.Errorf("字段<%s>禁止修改: %s", field.Name, errs[0].Error())
}

func (o *Objectql) getObjectUpdateableQueryFields(object *Object) []string {
	var result []string
	for _, field := range object.Fields {
		switch n := field.Updateable.(type) {
		case string:
			result = append(result, field.updateableSourceCodeFields...)
		case *FieldUpdateableHandle:
			result = append(result, n.Fields...)
		}
	}
	return result
}
