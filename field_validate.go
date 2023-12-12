package objectql

import (
	"context"
	"fmt"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/util/gconv"
)

func (o *Objectql) checkFieldFormulaOrHandledValidates(ctx context.Context, object *Object, cur *Var) error {
	for _, field := range object.Fields {
		if field.Validate == nil || cur.isNull(field.Api) {
			continue
		}

		switch n := field.Validate.(type) {
		case string:
			return o.checkFieldFormulaValidate(ctx, field, cur)
		case *FieldValidateHandle:
			return o.checkFieldHandleValidate(ctx, n, cur)
		}
	}
	return nil
}

func (o *Objectql) checkFieldFormulaValidate(ctx context.Context, field *Field, cur *Var) error {
	runner := formula.NewRunner()
	runner.SetThis(cur.ToStrAnyMap())
	result, err := runner.Resolve(ctx, field.validateSourceCode.Expression)
	if err != nil {
		return fmt.Errorf("checkFieldFormulaValidate error: %s", field.ValidateMsg)
	}
	if !gconv.Bool(result) {
		return fmt.Errorf("field validate error: %s", field.ValidateMsg)
	}
	return nil
}

func (o *Objectql) checkFieldHandleValidate(ctx context.Context, handle *FieldValidateHandle, cur *Var) error {
	return handle.Handle(ctx, cur)
}

func (o *Objectql) getObjectValidateQueryFields(object *Object) []string {
	var result []string
	for _, field := range object.Fields {
		switch n := field.Validate.(type) {
		case string:
			result = append(result, field.validateSourceCodeFields...)
		case *FieldValidateHandle:
			result = append(result, n.Fields...)
		}
	}
	return result
}
