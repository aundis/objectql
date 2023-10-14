package objectql

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gconv"
)

func (o *Objectql) DoCommand(ctx context.Context, command Command) (*Var, error) {
	command.Result = "data"
	res, err := o.DoCommands(ctx, []Command{command})
	if err != nil {
		return nil, err
	}
	return res.Var("data"), nil
}

func (o *Objectql) DoCommands(ctx context.Context, commands []Command, filter ...map[string]any) (*Var, error) {
	result, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		var this = map[string]any{}
		for _, command := range commands {
			arr := strings.Split(command.Call, ".")
			if len(arr) != 2 {
				return nil, fmt.Errorf("command call '%s' format error", command.Call)
			}
			objectApi := arr[0]
			funcNamme := arr[1]
			args, err := o.parseCommandArgs(&command)
			if err != nil {
				return nil, err
			}
			args, err = o.computeCommandArgs(ctx, this, objectApi, args)
			if err != nil {
				return nil, err
			}

			var result any
			var mapKey string
			switch n := args.(type) {
			case *FindOneByIdArgs:
				mapKey = command.Result
				result, err = o.FindOneById(ctx, objectApi, FindOneByIdOptions{
					ID:     n.ID,
					Fields: command.Fields,
					Direct: n.Direct,
				})
			case *FindOneArgs:
				mapKey = command.Result
				result, err = o.FindOne(ctx, objectApi, FindOneOptions{
					Filter: n.Filter,
					Sort:   n.Sort,
					Fields: command.Fields,
					Direct: n.Direct,
				})
			case *FindListArgs:
				mapKey = command.Result
				result, err = o.FindList(ctx, objectApi, FindListOptions{
					Filter: n.Filter,
					Top:    n.Top,
					Skip:   n.Skip,
					Sort:   n.Sort,
					Fields: command.Fields,
					Direct: n.Direct,
				})
			case *CountArgs:
				mapKey = command.Result
				result, err = o.Count(ctx, objectApi, CountOptions{
					Filter: n.Filter,
					Fields: command.Fields,
					Direct: n.Direct,
				})
			case *InsertArgs:
				mapKey = command.Result
				result, err = o.Insert(ctx, objectApi, InsertOptions{
					Doc:    n.Doc,
					Fields: command.Fields,
					Direct: n.Direct,
				})
			case *UpdateByIdArgs:
				mapKey = command.Result
				result, err = o.UpdateById(ctx, objectApi, UpdateByIdOptions{
					ID:     n.ID,
					Doc:    n.Doc,
					Fields: command.Fields,
					Direct: n.Direct,
				})
			case *UpdateArgs:
				mapKey = command.Result
				result, err = o.Update(ctx, objectApi, UpdateOptions{
					Filter: n.Filter,
					Doc:    n.Doc,
					Fields: command.Fields,
					Direct: n.Direct,
				})
			case *DeleteByIdArgs:
				err = o.DeleteById(ctx, objectApi, DeleteByIdOptions{
					ID:     n.ID,
					Direct: n.Direct,
				})
			case *DeleteArgs:
				err = o.Delete(ctx, objectApi, DeleteOptions{
					Filter: n.Filter,
					Direct: n.Direct,
				})
			case map[string]any:
				mapKey = command.Result
				result, err = o.Call(ctx, objectApi, funcNamme, n, command.Fields)
			case nil:
				mapKey = command.Result
				result, err = o.Call(ctx, objectApi, funcNamme, nil, command.Fields)
			}
			if err != nil {
				return nil, err
			}
			if len(mapKey) > 0 {
				switch n := result.(type) {
				case *Var:
					this[mapKey] = n.ToAny()
				case []*Var:
					this[mapKey] = VarsToAnys(n)
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
	if len(filter) == 0 {
		return NewVar(result), nil
	}
	res, err := computeValue(ctx, result.(map[string]any), filter[0])
	if err != nil {
		return nil, err
	}
	return NewVar(res), nil
}

func (o *Objectql) parseCommandArgs(command *Command) (any, error) {
	if gstr.HasSuffix(command.Call, ".insert") {
		var args *InsertArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".delete") {
		var args *DeleteArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".deleteById") {
		var args *DeleteByIdArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".updateById") {
		var args *UpdateByIdArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".update") {
		var args *UpdateArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".findList") {
		var args *FindListArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".findOne") {
		var args *FindOneArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".findOneById") {
		var args *FindOneByIdArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	return command.Args, nil
}

func (o *Objectql) computeCommandArgs(ctx context.Context, this map[string]any, object string, args any) (any, error) {
	switch n := args.(type) {
	case *FindOneByIdArgs:
		r, err := computeString(ctx, this, n.ID)
		if err != nil {
			return nil, err
		}
		n.ID = r.(string)
		return n, nil
	case *FindOneArgs:
		r, err := computeValue(ctx, this, n.Filter)
		if err != nil {
			return nil, err
		}
		n.Filter = r.(map[string]any)
		return n, nil
	case *FindListArgs:
		r, err := computeValue(ctx, this, n.Filter)
		if err != nil {
			return nil, err
		}
		n.Filter = r.(map[string]any)
		return n, nil
	case *CountArgs:
		r, err := computeValue(ctx, this, n.Filter)
		if err != nil {
			return nil, err
		}
		n.Filter = r.(map[string]any)
		return n, nil
	case *InsertArgs:
		r, err := o.computeDocument(ctx, object, this, n.Doc)
		if err != nil {
			return nil, err
		}
		n.Doc = r
		return n, nil
	case *UpdateByIdArgs:
		r, err := computeString(ctx, this, n.ID)
		if err != nil {
			return nil, err
		}
		n.ID = r.(string)
		doc, err := o.computeDocument(ctx, object, this, n.Doc)
		if err != nil {
			return nil, err
		}
		n.Doc = doc
		return n, nil
	case *UpdateArgs:
		r, err := computeValue(ctx, this, n.Filter)
		if err != nil {
			return nil, err
		}
		n.Filter = r.(map[string]any)
		doc, err := o.computeDocument(ctx, object, this, n.Doc)
		if err != nil {
			return nil, err
		}
		n.Doc = doc
		return n, nil
	case *DeleteByIdArgs:
		r, err := computeString(ctx, this, n.ID)
		if err != nil {
			return nil, err
		}
		n.ID = r.(string)
		return n, nil
	case *DeleteArgs:
		r, err := computeValue(ctx, this, n.Filter)
		if err != nil {
			return nil, err
		}
		n.Filter = r.(map[string]any)
		return n, nil
	case map[string]any:
		r, err := computeValue(ctx, this, n)
		if err != nil {
			return nil, err
		}
		return r, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("computeCommandArgs error: unknown commandArgs type %T", args)
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
