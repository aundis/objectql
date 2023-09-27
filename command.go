package objectql

import (
	"context"
	"fmt"
	"strings"

	"github.com/aundis/formula"
)

func (o *Objectql) DoCommand(ctx context.Context, commands []Command) (map[string]any, error) {
	result, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		var resultMap = map[string]any{}
		for _, command := range commands {
			var result any
			var err error
			var mapKey string
			switch n := command.(type) {
			case *FindOneByIdCommand:
				mapKey = n.Result
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.FindOneById(ctx, n.Object, n.ID, n.Fields)
			case *FindOneCommand:
				mapKey = n.Result
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.FindOne(ctx, n.Object, FindOneOptions{
					Condition: n.Condition,
					Sort:      n.Sort,
					Fields:    n.Fields,
				})
			case *FindListCommand:
				mapKey = n.Result
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.FindList(ctx, n.Object, FindListOptions{
					Condition: n.Condition,
					Top:       n.Top,
					Skip:      n.Skip,
					Sort:      n.Sort,
					Fields:    n.Fields,
				})
			case *CountCommand:
				mapKey = n.Result
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.Count(ctx, n.Object, n.Condition)
			case *InsertCommand:
				mapKey = n.Result
				err = o.computeDocument(ctx, n.Object, resultMap, n.Doc)
				if err != nil {
					return nil, err
				}
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.Insert(ctx, n.Object, InsertOptions{
					Doc:    n.Doc,
					Fields: n.Fields,
				})
			case *UpdateByIdCommand:
				mapKey = n.Result
				err = o.computeDocument(ctx, n.Object, resultMap, n.Doc)
				if err != nil {
					return nil, err
				}
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.UpdateById(ctx, n.Object, n.ID, UpdateByIdOptions{
					Doc:    n.Doc,
					Fields: n.Fields,
				})
			case *UpdateCommand:
				mapKey = n.Result
				err = o.computeDocument(ctx, n.Object, resultMap, n.Doc)
				if err != nil {
					return nil, err
				}
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.Update(ctx, n.Object, UpdateOptions{
					Condition: n.Condition,
					Doc:       n.Doc,
					Fields:    n.Fields,
				})
			case *DeleteByIdCommand:
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				err = o.DeleteById(ctx, n.Object, n.ID)
			case *DeleteCommand:
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				err = o.Delete(ctx, n.Object, n.Condition)
			}
			if err != nil {
				return nil, err
			}
			if len(mapKey) > 0 {
				switch n := result.(type) {
				case Entity:
					resultMap[mapKey] = n.Raw()
				case []Entity:
					resultMap[mapKey] = EntityArrayToRawArray(n)
				default:
					resultMap[mapKey] = result
				}
			}
		}
		return resultMap, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(map[string]any), nil
}

func (o *Objectql) computeDocument(ctx context.Context, objectApi string, this map[string]any, doc map[string]any) error {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return fmt.Errorf("computeDocument error: not found object %s", objectApi)
	}
	for k, v := range doc {
		if str, ok := v.(string); ok && strings.HasPrefix(str, "$$ ") {
			field := FindFieldFromObject(object, k)
			if field == nil {
				return fmt.Errorf("computeDocument error: not found object %s field %s", objectApi, k)
			}
			sourceCode, err := formula.ParseSourceCode([]byte(strings.Replace(str, "$$ ", "", 1)))
			if err != nil {
				return err
			}
			runner := formula.NewRunner()
			runner.IdentifierResolver = o.resolverDocumentIdentifier
			runner.SelectorExpressionResolver = o.resolveDocumentSelectorExpression
			runner.Set("this", this)
			value, err := runner.Resolve(ctx, sourceCode.Expression)
			if err != nil {
				return err
			}
			input, err := formatComputedValue(field.Type, value)
			if err != nil {
				return err
			}
			doc[k] = input
		}
	}
	return nil
}

func (o *Objectql) resolverDocumentIdentifier(ctx context.Context, name string) (interface{}, error) {
	runner := formula.RunnerFromCtx(ctx)
	this := runner.Get("this").(map[string]any)
	return this[name], nil
}

func (o *Objectql) resolveDocumentSelectorExpression(ctx context.Context, cha string) (interface{}, error) {
	runner := formula.RunnerFromCtx(ctx)
	this := runner.Get("this").(map[string]any)
	names := strings.Split(cha, ".")
	var cur = interface{}(this)
	for _, name := range names {
		if m, ok := cur.(map[string]any); ok {
			cur = m[name]
		} else {
			return nil, fmt.Errorf("resolveDocumentSelectorExpression error: can't conv %s to  map[string]any", name)
		}
	}
	return cur, nil
}
