package objectql

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/aundis/formula"
)

func (o *Objectql) DoCommand(ctx context.Context, commands []Command, filter ...map[string]any) (map[string]any, error) {
	result, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		var this = map[string]any{}
		for _, command := range commands {
			command, err := o.computeCommand(ctx, this, command)
			if err != nil {
				return nil, err
			}

			var result any
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
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.Insert(ctx, n.Object, InsertOptions{
					Doc:    n.Doc,
					Fields: n.Fields,
				})
			case *UpdateByIdCommand:
				mapKey = n.Result
				ctx = context.WithValue(ctx, blockEventsKey, n.Direct)
				result, err = o.UpdateById(ctx, n.Object, n.ID, UpdateByIdOptions{
					Doc:    n.Doc,
					Fields: n.Fields,
				})
			case *UpdateCommand:
				mapKey = n.Result
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
			case *HandleCommand:
				if o.isQueryHandle(n.Object, n.Command) {
					result, err = o.Query(ctx, n.Object, n.Command, n.Args, n.Fields)
				} else if o.isMutationHandle(n.Object, n.Command) {
					result, err = o.Mutation(ctx, n.Object, n.Command, n.Args, n.Fields)
				} else {
					err = fmt.Errorf("not found command '%s' from '%s", n.Command, n.Object)
				}
			}
			if err != nil {
				return nil, err
			}
			if len(mapKey) > 0 {
				switch n := result.(type) {
				case Entity:
					this[mapKey] = n.Raw()
				case []Entity:
					this[mapKey] = EntityArrayToRawArray(n)
				default:
					this[mapKey] = result
				}
			}
		}
		return this, nil
	})
	if err != nil {
		return nil, err
	}
	this := result.(map[string]any)
	if len(filter) == 0 {
		return this, nil
	}
	res, err := computeValue(ctx, this, filter[0])
	if err != nil {
		return nil, err
	}
	return res.(map[string]any), nil
}

func (o *Objectql) computeCommand(ctx context.Context, this map[string]any, command Command) (Command, error) {
	switch n := command.(type) {
	case *FindOneByIdCommand:
		r, err := computeString(ctx, this, n.ID)
		if err != nil {
			return nil, err
		}
		n.ID = r.(string)
		return n, nil
	case *FindOneCommand:
		r, err := computeValue(ctx, this, n.Condition)
		if err != nil {
			return nil, err
		}
		n.Condition = r.(map[string]any)
		return n, nil
	case *FindListCommand:
		r, err := computeValue(ctx, this, n.Condition)
		if err != nil {
			return nil, err
		}
		n.Condition = r.(map[string]any)
		return n, nil
	case *CountCommand:
		r, err := computeValue(ctx, this, n.Condition)
		if err != nil {
			return nil, err
		}
		n.Condition = r.(map[string]any)
		return n, nil
	case *InsertCommand:
		r, err := o.computeDocument(ctx, n.Object, this, n.Doc)
		if err != nil {
			return nil, err
		}
		n.Doc = r
		return n, nil
	case *UpdateByIdCommand:
		r, err := computeString(ctx, this, n.ID)
		if err != nil {
			return nil, err
		}
		n.ID = r.(string)
		doc, err := o.computeDocument(ctx, n.Object, this, n.Doc)
		if err != nil {
			return nil, err
		}
		n.Doc = doc
		return n, nil
	case *UpdateCommand:
		r, err := computeValue(ctx, this, n.Condition)
		if err != nil {
			return nil, err
		}
		n.Condition = r.(map[string]any)
		doc, err := o.computeDocument(ctx, n.Object, this, n.Doc)
		if err != nil {
			return nil, err
		}
		n.Doc = doc
		return n, nil
	case *DeleteByIdCommand:
		r, err := computeString(ctx, this, n.ID)
		if err != nil {
			return nil, err
		}
		n.ID = r.(string)
		return n, nil
	case *DeleteCommand:
		r, err := computeValue(ctx, this, n.Condition)
		if err != nil {
			return nil, err
		}
		n.Condition = r.(map[string]any)
		return n, nil
	default:
		return nil, fmt.Errorf("computeCommand error: unknown command type %T", command)
	}
}

func (o *Objectql) computeDocument(ctx context.Context, objectApi string, this map[string]any, doc map[string]any) (map[string]any, error) {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("computeDocument error: not found object %s", objectApi)
	}
	for k, v := range doc {
		if str, ok := v.(string); ok && strings.HasPrefix(str, "$$ ") {
			field := FindFieldFromObject(object, k)
			if field == nil {
				return nil, fmt.Errorf("computeDocument error: not found object %s field %s", objectApi, k)
			}
			value, err := computeString(ctx, this, v)
			if err != nil {
				return nil, err
			}
			input, err := formatComputedValue(field.Type, value)
			if err != nil {
				return nil, err
			}
			doc[k] = input
		}
	}
	return doc, nil
}

func computeValue(ctx context.Context, this map[string]any, value any) (any, error) {
	tpe := reflect.TypeOf(value)
	switch tpe.Kind() {
	case reflect.String:
		return computeStringAndNormalizeNumber(ctx, this, value)
	case reflect.Array, reflect.Slice:
		return computeArray(ctx, this, value)
	case reflect.Map:
		return computeMap(ctx, this, value)
	}
	return value, nil
}

func computeMap(ctx context.Context, this map[string]any, value any) (any, error) {
	sv := reflect.ValueOf(value)
	result := reflect.MakeMap(reflect.TypeOf(value))
	iter := sv.MapRange()
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		evalue, err := computeValue(ctx, this, v.Interface())
		if err != nil {
			return nil, err
		}
		result.SetMapIndex(k, reflect.ValueOf(evalue))
	}
	return result.Interface(), nil
}

func computeArray(ctx context.Context, this map[string]any, value any) (interface{}, error) {
	array := reflect.ValueOf(value)
	result := reflect.MakeSlice(reflect.TypeOf(value), 0, 0)
	for i := 0; i < array.Len(); i++ {
		evalue, err := computeValue(ctx, this, array.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		result = reflect.Append(result, reflect.ValueOf(evalue))
	}
	return result.Interface(), nil
}

func computeStringAndNormalizeNumber(ctx context.Context, this map[string]any, value any) (any, error) {
	res, err := computeString(ctx, this, value)
	if err != nil {
		return nil, err
	}
	if formula.IsNumber(res) {
		return formula.ToFloat64(res)
	}
	return res, nil
}

func computeString(ctx context.Context, this map[string]any, value any) (any, error) {
	if str, ok := value.(string); ok && strings.HasPrefix(str, "$$ ") {
		sourceCode, err := formula.ParseSourceCode([]byte(strings.Replace(str, "$$ ", "", 1)))
		if err != nil {
			return nil, err
		}
		runner := formula.NewRunner()
		runner.IdentifierResolver = resolverDocumentIdentifier
		runner.SelectorExpressionResolver = resolveDocumentSelectorExpression
		runner.Set("this", this)
		return runner.Resolve(ctx, sourceCode.Expression)
	}
	return value, nil
}

func resolverDocumentIdentifier(ctx context.Context, name string) (interface{}, error) {
	runner := formula.RunnerFromCtx(ctx)
	this := runner.Get("this").(map[string]any)
	return this[name], nil
}

func resolveDocumentSelectorExpression(ctx context.Context, cha string) (interface{}, error) {
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
