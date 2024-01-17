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
	// 将args结构转为map
	convCommandArgStructToMap(commands)
	result, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		var this = map[string]any{}
		for _, command := range commands {
			arr := strings.Split(command.Call, ".")
			if len(arr) != 2 {
				return nil, fmt.Errorf("command call '%s' format error", command.Call)
			}
			objectApi := arr[0]
			funcNamme := arr[1]
			err := o.computeCommandArgs(ctx, this, &command)
			if err != nil {
				return nil, err
			}
			args, err := o.convCommandArgs(&command)
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
					Doc:      n.Doc,
					Index:    n.Index,
					Dir:      n.Dir,
					Absolute: n.Absolute,
					Fields:   command.Fields,
					Direct:   n.Direct,
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
			case *AggregateArgs:
				mapKey = command.Result
				result, err = o.Aggregate(ctx, objectApi, AggregateOptions{
					Pipeline: n.Pipeline,
					Direct:   n.Direct,
				})
			case *MoveArgs:
				err = o.Move(ctx, objectApi, MoveOptions{
					ID:       n.ID,
					Index:    n.Index,
					Dir:      n.Dir,
					Absolute: n.Absolute,
					Direct:   n.Direct,
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
					if n == nil {
						this[mapKey] = nil
					} else {
						this[mapKey] = n.ToAny()
					}
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

func convCommandArgStructToMap(commands []Command) {
	for _, cmd := range commands {
		var args M
		switch n := cmd.Args.(type) {
		case FindOneByIdArgs:
			args = structToMap(n)
		case FindOneArgs:
			args = structToMap(n)
		case FindListArgs:
			args = structToMap(n)
		case AggregateArgs:
			args = structToMap(n)
		case CountArgs:
			args = structToMap(n)
		case InsertArgs:
			args = structToMap(n)
		case UpdateByIdArgs:
			args = structToMap(n)
		case UpdateArgs:
			args = structToMap(n)
		case DeleteByIdArgs:
			args = structToMap(n)
		case DeleteArgs:
			args = structToMap(n)
		case MoveArgs:
			args = structToMap(n)
		}
		if args != nil {
			cmd.Args = args
		}
	}
}

func structToMap(inputStruct interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	structValue := reflect.ValueOf(inputStruct)
	structType := structValue.Type()
	for i := 0; i < structValue.NumField(); i++ {
		field := structValue.Field(i)
		tag := structType.Field(i).Tag
		fieldName := tag.Get("json")
		if len(fieldName) == 0 {
			fieldName = structType.Field(i).Name
		}
		result[fieldName] = field.Interface()
	}
	return result
}

func (o *Objectql) computeCommandArgs(ctx context.Context, this map[string]any, command *Command) error {
	// 给定一个默认值，不然后面会出现nil错误
	if isNull(command.Args) {
		command.Args = map[string]any{}
	}
	r, err := computeValue(ctx, this, command.Args)
	if err != nil {
		return err
	}
	// if _, ok := r.(map[string]any); !ok {
	// 	return fmt.Errorf("command args computed after result value not map[string]any")
	// }
	command.Args = r
	return nil
}

func (o *Objectql) convCommandArgs(command *Command) (any, error) {
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
	if gstr.HasSuffix(command.Call, ".count") {
		var args *CountArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".aggregate") {
		var args *AggregateArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	if gstr.HasSuffix(command.Call, ".move") {
		var args *MoveArgs
		err := gconv.Struct(command.Args, &args)
		if err != nil {
			return nil, err
		}
		return args, nil
	}
	return command.Args, nil
}

func computeValue(ctx context.Context, this map[string]any, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	tpe := reflect.TypeOf(value)
	switch tpe.Kind() {
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
		if k.String() == "$formula" {
			if sv.Len() > 1 {
				return nil, fmt.Errorf("$formula object contain multiple key")
			}
			// 如果是公式，上面值已经计算过一次了，支持嵌套公式
			return computeString(ctx, this, evalue)
		}
		var elem reflect.Value
		if evalue == nil {
			elem = reflect.New(reflect.TypeOf((*interface{})(nil)).Elem()).Elem()
		} else {
			elem = reflect.ValueOf(evalue)
		}
		result.SetMapIndex(k, elem)
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

func computeString(ctx context.Context, this map[string]any, value any) (any, error) {
	if str, ok := value.(string); ok {
		sourceCode, err := formula.ParseSourceCode([]byte(str))
		if err != nil {
			return nil, err
		}
		runner := formula.NewRunner()
		runner.SetThis(this)
		return runner.Resolve(ctx, sourceCode.Expression)
	}
	return nil, fmt.Errorf("formula type must is string, but got %T", value)
}
