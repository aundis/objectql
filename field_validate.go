package objectql

import (
	"context"
	"fmt"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/samber/lo"
)

func (o *Objectql) checkFieldFormulaOrHandledValidates(ctx context.Context, object *Object, doc M, cur *Var) error {
	fields := o.getEffectValidateFields(object, doc)
	for _, field := range fields {
		// nil值不进行校验
		if cur.isNull(field.Api) {
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

func (o *Objectql) getEffectValidateFieldsQuerys(object *Object, doc M) []string {
	fields := o.getEffectValidateFields(object, doc)
	var result []string
	for _, field := range fields {
		switch n := field.Validate.(type) {
		case string:
			result = append(result, field.validateSourceCodeFields...)
		case *FieldValidateHandle:
			result = append(result, n.Fields...)
		}
	}
	return result
}

func (o *Objectql) getEffectValidateFields(object *Object, doc M) []*Field {
	vfields := o.getObjectValidateFields(object)
	vmap := o.getValidateFieldsRelations(vfields)
	var result []*Field
	for k := range doc {
		for fapi, fieldNams := range vmap {
			if lo.Contains(fieldNams, k) {
				result = append(result, object.getField(fapi))
			}
		}
	}
	return result
}

func (o *Objectql) getValidateFieldsRelations(fields []*Field) map[string][]string {
	result := map[string][]string{}
	for _, field := range fields {
		switch n := field.Validate.(type) {
		case string:
			result[field.Api] = field.validateSourceCodeFields
		case *FieldValidateHandle:
			result[field.Api] = n.Fields
		}
	}
	return result
}

func (o *Objectql) getObjectValidateFields(object *Object) []*Field {
	var result []*Field
	for _, field := range object.Fields {
		if field.Validate != nil {
			result = append(result, field)
		}
	}
	return result
}
