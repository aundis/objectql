package objectql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/aundis/graphql"
	"github.com/aundis/graphql/language/ast"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (o *Objectql) getGraphqlObject(name string) *graphql.Object {
	if o.gobjects.Contains(name) {
		return o.gobjects.Get(name).(*graphql.Object)
	}
	return nil
}

func (o *Objectql) initObjectGraphqlQuery(ctx context.Context, querys graphql.Fields, object *Object) error {
	querys[object.Api+"__findList"] = &graphql.Field{
		Type: graphql.NewList(o.getGraphqlObject(object.Api)),
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "过滤条件",
			},
			"top": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: "返回数量限制",
			},
			"skip": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: "跳过指定数量的返回结果，用于分页",
			},
			"sort": &graphql.ArgumentConfig{
				Type:        graphql.NewList(graphql.String),
				Description: "排序",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlQueryListResolver(p.Context, p, object)
		},
	}

	querys[object.Api+"__aggregate"] = &graphql.Field{
		Type: graphql.NewList(graphqlAny),
		Args: graphql.FieldConfigArgument{
			"pipeline": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlQueryAggregateResolver(p.Context, p, object)
		},
	}

	querys[object.Api+"__findOne"] = &graphql.Field{
		Type: o.getGraphqlObject(object.Api),
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "过滤条件",
			},
			"sort": &graphql.ArgumentConfig{
				Type:        graphql.NewList(graphql.String),
				Description: "排序",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlQueryOneResolver(p.Context, p, object)
		},
	}

	querys[object.Api+"__findOneById"] = &graphql.Field{
		Type: o.getGraphqlObject(object.Api),
		Args: graphql.FieldConfigArgument{
			"id": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "对象id",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlQueryOneByIdResolver(p.Context, p, object)
		},
	}

	querys[object.Api+"__count"] = &graphql.Field{
		Type: graphql.Int,
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "过滤条件",
			},
			"top": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: "返回数量限制",
			},
			"skip": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: "跳过指定数量的返回结果，用于分页",
			},
			"sort": &graphql.ArgumentConfig{
				Type:        graphql.NewList(graphql.String),
				Description: "排序",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlQueryCountResolver(p.Context, p, object)
		},
	}

	// 自定义mutation
	for _, handle := range object.Querys {
		err := o.validateHandle(handle)
		if err != nil {
			return err
		}
		args, err := o.getGraphqlArgsFromHandle(ctx, handle)
		if err != nil {
			return err
		}
		rtn, err := o.getGraphqlReturnFromHandle(ctx, handle)
		if err != nil {
			return err
		}
		curHandle := handle
		querys[object.Api+"__"+handle.Api] = &graphql.Field{
			Type: rtn,
			Args: args,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return o.handleGraphqlResovler(p.Context, p, object, curHandle)
			},
			Description: handle.Name + ": " + handle.Comment,
		}
	}
	return nil
}

func (o *Objectql) graphqlQueryAggregateResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	pipeline, err := o.parseMongoAggregatePipeline(ctx, p)
	if err != nil {
		return nil, err
	}
	cursor, err := o.getCollection(object.Api).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	var list []bson.M
	err = cursor.All(ctx, &list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (o *Objectql) parseMongoAggregatePipeline(ctx context.Context, p graphql.ResolveParams) ([]bson.M, error) {
	pipeline := p.Args["pipeline"]
	var result []bson.M
	if v, ok := pipeline.(string); ok && len(v) > 0 {
		// TODO: 详细了解一下UnmarshalExtJSON的用法
		err := json.Unmarshal([]byte(v), &result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (o *Objectql) graphqlQueryListResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	options, err := o.parseMongoFindOptions(ctx, p)
	if err != nil {
		return nil, err
	}
	result, err := o.mongoFindAllEx(ctx, object.Api, *options)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func (o *Objectql) graphqlQueryOneResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	options, err := o.parseMongoFindOneOptinos(ctx, p)
	if err != nil {
		return nil, err
	}
	result, err := o.mongoFindOneEx(ctx, object.Api, *options)
	if err != nil {
		return nil, err
	}
	if isNull(result) {
		return nil, nil
	}
	return result, nil
}

func (o *Objectql) graphqlQueryOneByIdResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	objectId := gconv.String(p.Args["id"])
	if len(objectId) == 0 {
		return nil, errors.New("arguemnt id can't empty")
	}
	hexId, err := primitive.ObjectIDFromHex(objectId)
	if err != nil {
		return nil, err
	}
	fields := o.parseMongoQueryFields(p)
	result, err := o.mongoFindOneEx(ctx, object.Api, findOneExOptions{
		Fields: fields,
		Filter: M{
			"_id": hexId,
		},
	})
	if err != nil {
		return nil, err
	}
	if isNull(result) {
		return nil, nil
	}
	return result, nil
}

func (o *Objectql) graphqlQueryCountResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	filter, err := o.parseMongoFindFilters(ctx, gconv.String(p.Args["filter"]))
	if err != nil {
		return nil, err
	}
	count, err := o.mongoCountEx(ctx, object.Api, countExOptions{
		Filter: filter,
	})
	if err != nil {
		return nil, err
	}
	return count, nil
}

func (o *Objectql) parseMongoFindOneOptinos(ctx context.Context, p graphql.ResolveParams) (*findOneExOptions, error) {
	findOptions := &findOneExOptions{}
	findOptions.Fields = o.parseMongoQueryFields(p)
	filter, err := o.parseMongoFindFilters(ctx, gconv.String(p.Args["filter"]))
	if err != nil {
		return nil, err
	}
	if filter != nil {
		findOptions.Filter = filter
	}
	sort := p.Args["sort"]
	if sort != nil {
		findOptions.Sort = gconv.Strings(sort)
	}
	return findOptions, nil
}

func (o *Objectql) parseMongoFindOptions(ctx context.Context, p graphql.ResolveParams) (*findAllExOptions, error) {
	findOptions := &findAllExOptions{}
	findOptions.Fields = o.parseMongoQueryFields(p)
	skip := p.Args["skip"]
	if skip != nil {
		findOptions.Skip = gconv.Int(skip)
	}
	top := p.Args["top"]
	if top != nil {
		findOptions.Top = gconv.Int(top)
	}
	sort := p.Args["sort"]
	if sort != nil {
		findOptions.Sort = gconv.Strings(sort)
	}
	filter, err := o.parseMongoFindFilters(ctx, gconv.String(p.Args["filter"]))
	if err != nil {
		return nil, err
	}
	if filter != nil {
		findOptions.Filter = filter
	}
	return findOptions, nil
}

func (o *Objectql) parseMongoQueryFields(p graphql.ResolveParams) []string {
	project := convertFieldASTsToMongoProject(p)
	var fields []string
	convProjectToQueryFields("", project, &fields)
	return fields
}

func convProjectToQueryFields(preStr string, project map[string]interface{}, result *[]string) {
	if len(preStr) != 0 {
		preStr += "."
	}
	for k, v := range project {
		switch n := v.(type) {
		case int:
			*result = append(*result, preStr+k)
		case map[string]interface{}:
			convProjectToQueryFields(preStr+k, n, result)
		}
	}
}

// 递归函数，将GraphQL类型映射为Mongo查询条件
func mapGraphqlFieldToMongoProject(fieldAST *ast.Field, project map[string]interface{}) {
	fieldName := fieldAST.Name.Value
	if fieldAST.SelectionSet != nil {
		nestedProejct := make(map[string]interface{})
		for _, selection := range fieldAST.SelectionSet.Selections {
			nestedFieldAST, ok := selection.(*ast.Field)
			if !ok {
				continue
			}

			mapGraphqlFieldToMongoProject(nestedFieldAST, nestedProejct)

		}
		project[fieldName] = nestedProejct
	} else {
		project[fieldName] = 1
	}
}

// 获取Graphql请求要查询的字段
func convertFieldASTsToMongoProject(p graphql.ResolveParams) map[string]interface{} {
	project := make(map[string]interface{})
	rootFieldName := p.Info.FieldName
	for _, rootFieldAst := range p.Info.FieldASTs {
		currentFieldName := rootFieldAst.Name.Value
		if currentFieldName != rootFieldName {
			continue
		}

		for _, selection := range rootFieldAst.SelectionSet.Selections {
			fieldAst, ok := selection.(*ast.Field)
			if !ok {
				continue
			}
			mapGraphqlFieldToMongoProject(fieldAst, project)
		}
	}
	return project
}

func stringsToSortMap(arr []string) bson.M {
	var result = bson.M{}
	for _, v := range gconv.Strings(arr) {
		if len(v) == 0 {
			continue
		}
		if v[0] == '-' {
			result[v[1:]] = -1
		} else {
			result[strings.Trim(v[1:], "+")] = 1
		}
	}
	return result
}

func (o *Objectql) parseMongoFiledSelects(ctx context.Context, p graphql.ResolveParams) bson.M {
	selects := getGraphqlSelectFieldNames(p)
	if len(selects) > 0 {
		return stringArrayToMongodbSelects(selects)
	}
	return nil
}

func (o *Objectql) exceptParseMongoFindFilters(ctx context.Context, filterJsonStr string) (M, error) {
	filter, err := o.parseMongoFindFilters(ctx, filterJsonStr)
	if err != nil {
		return nil, err
	}
	if len(filter) == 0 {
		return nil, gerror.New("parsed filter can't be empty")
	}
	return filter, nil
}

func (o *Objectql) parseMongoFindFilters(ctx context.Context, filterJsonStr string) (M, error) {
	// 空的删选条件
	if len(filterJsonStr) == 0 {
		return nil, nil
	}

	var fjson M
	err := json.Unmarshal([]byte(filterJsonStr), &fjson)
	if err != nil {
		return nil, err
	}
	filter, err := preprocessMongoMap(fjson)
	if err != nil {
		return nil, err
	}
	return filter.(M), nil
}

func preprocessMongoMap(data interface{}) (interface{}, error) {
	switch n := data.(type) {
	case M:
		res := M{}
		for k, v := range n {
			switch k {
			case "$toId":
				if len(n) != 1 {
					return nil, fmt.Errorf("$toId object contain multiple keys")
				}
				return preprocessMongoMapToId(v)
			case "$toDate":
				if len(n) != 1 {
					return nil, fmt.Errorf("$toDate object contain multiple keys")
				}
				return preprocessMongoMapToDate(v)
			default:
				r, err := preprocessMongoMap(v)
				if err != nil {
					return nil, err
				}
				res[k] = r
			}
		}
		return res, nil
	case A:
		var list A
		for _, v := range n {
			r, err := preprocessMongoMap(v)
			if err != nil {
				return nil, err
			}
			list = append(list, r)
		}
		return list, nil
	default:
		return data, nil
	}
}

func preprocessMongoMapToId(value any) (primitive.ObjectID, error) {
	if v, ok := value.(string); ok {
		return primitive.ObjectIDFromHex(v)
	}
	return primitive.NilObjectID, fmt.Errorf("$toId value must is string")
}

func preprocessMongoMapToDate(value any) (time.Time, error) {
	if v, ok := value.(string); ok {
		r, err := gtime.StrToTime(v)
		if err != nil {
			return time.Time{}, err
		}
		return r.Time, nil
	}
	return time.Time{}, fmt.Errorf("$toDate value must is string")
}

func (o *Objectql) initObjectGraphqlMutation(ctx context.Context, mutations graphql.Fields, object *Object) error {
	form := o.getGrpahqlObjectMutationForm(object)
	// 新增
	mutations[object.Api+"__insert"] = &graphql.Field{
		Type: o.getGraphqlObject(object.Api),
		Args: graphql.FieldConfigArgument{
			"doc": &graphql.ArgumentConfig{
				Type:        form,
				Description: "对象文档",
			},
			"index": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: "插入位置",
			},
			"dir": &graphql.ArgumentConfig{
				Type:        graphql.Int,
				Description: "插入方向",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationInsertResolver(p.Context, p, object)
		},
	}
	// 批量修改
	mutations[object.Api+"__update"] = &graphql.Field{
		Type: graphql.NewList(o.getGraphqlObject(object.Api)),
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "条件",
			},
			"doc": &graphql.ArgumentConfig{
				Type:        form,
				Description: "对象文档",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationUpdateResolver(p.Context, p, object)
		},
	}
	// 单个修改
	mutations[object.Api+"__updateById"] = &graphql.Field{
		Type: o.getGraphqlObject(object.Api),
		Args: graphql.FieldConfigArgument{
			"_id": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "对象id",
			},
			"doc": &graphql.ArgumentConfig{
				Type:        form,
				Description: "对象文档",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationUpdateByIdResolver(p.Context, p, object)
		},
	}
	// 移动
	if object.Index {
		mutations[object.Api+"__move"] = &graphql.Field{
			Type: graphql.Boolean,
			Args: graphql.FieldConfigArgument{
				"_id": &graphql.ArgumentConfig{
					Type:        graphql.String,
					Description: "对象id",
				},
				"index": &graphql.ArgumentConfig{
					Type:        graphql.Int,
					Description: "排序位置",
				},
				"dir": &graphql.ArgumentConfig{
					Type:        graphql.Int,
					Description: "插入方向",
				},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return o.graphqlMutationMoveResolver(p.Context, p, object)
			},
		}
	}
	// 批量删除
	mutations[object.Api+"__delete"] = &graphql.Field{
		Type: graphql.Boolean,
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "条件",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationDeleteResolver(p.Context, p, object)
		},
	}
	// 单个删除
	mutations[object.Api+"__deleteById"] = &graphql.Field{
		Type: graphql.Boolean,
		Args: graphql.FieldConfigArgument{
			"_id": &graphql.ArgumentConfig{
				Type:        graphql.String,
				Description: "对象id",
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationDeleteByIdResolver(p.Context, p, object)
		},
	}
	// 自定义mutation
	for _, handle := range object.Mutations {
		err := o.validateHandle(handle)
		if err != nil {
			return err
		}
		args, err := o.getGraphqlArgsFromHandle(ctx, handle)
		if err != nil {
			return err
		}
		rtn, err := o.getGraphqlReturnFromHandle(ctx, handle)
		if err != nil {
			return err
		}
		curHandle := handle
		mutations[object.Api+"__"+handle.Api] = &graphql.Field{
			Type: rtn,
			Args: args,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return o.handleGraphqlResovler(p.Context, p, object, curHandle)
			},
			Description: handle.Name + ": " + handle.Comment,
		}
	}
	return nil
}

func (o *Objectql) validateHandle(handle *Handle) error {
	// 获取函数的类型信息
	fnType := reflect.TypeOf(handle.Resolve)
	if fnType.Kind() != reflect.Func {
		return errors.New("getGraphqlArgsFromMutation mutation handle must is function")
	}
	if fnType.NumIn() < 2 {
		return errors.New("getGraphqlArgsFromMutation mutation handle must has 2 param")
	}
	// 第一个参数必须是 contxt.Context
	if unPointerType(fnType.In(0)).Name() != "Context" {
		return errors.New("getGraphqlArgsFromMutation mutation handle first param type must is context.Context")
	}
	// 如果有第二个参数必须为结构体
	if fnType.NumIn() == 2 && unPointerType(fnType.In(1)).Kind() != reflect.Struct {
		return errors.New("getGraphqlArgsFromMutation mutation handle two param must is struct")
	}
	// 检查返回值
	if fnType.NumOut() == 2 {
		if fnType.NumOut() < 2 {
			return errors.New("getGraphqlArgsFromMutation mutation handle must has 2 return")
		}
		if fnType.Out(1).Name() != "error" {
			return errors.New("getGraphqlArgsFromMutation mutation handle two return type must is error.Error")
		}
		handle.res = fnType.Out(0)
	} else if fnType.NumOut() == 1 {
		if fnType.Out(0).Name() != "error" {
			return errors.New("getGraphqlArgsFromMutation mutation handle one return type must is error.Error")
		}
	} else {
		return errors.New("getGraphqlArgsFromMutation mutation handle can only have 1 or 2 return type")
	}
	handle.req = fnType.In(1)
	return nil
}

func unPointerType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		return unPointerType(t.Elem())
	}
	return t
}

func unPointerValue(t reflect.Value) reflect.Value {
	if t.Kind() == reflect.Pointer {
		return unPointerValue(t.Elem())
	}
	return t
}

func (o *Objectql) handleGraphqlResovler(ctx context.Context, p graphql.ResolveParams, object *Object, handle *Handle) (interface{}, error) {
	if err := o.checkObjectHandlePermission(ctx, object.Api, handle.Api); err != nil {
		return nil, err
	}

	v := reflect.New(unPointerType(handle.req))
	err := gconv.Struct(formatNullValue(p.Args), v.Interface())
	if err != nil {
		return nil, err
	}
	// gvalid.CheckStruct
	err = g.Validator().Data(v.Interface()).Run(ctx)
	if err != nil {
		return nil, err
	}
	// 反射调用
	rt := reflect.TypeOf(handle.Resolve)
	fn := reflect.ValueOf(handle.Resolve)
	var args []reflect.Value
	if handle.req.Kind() == reflect.Pointer {
		args = []reflect.Value{reflect.ValueOf(o.WithRootPermission(ctx)), reflect.ValueOf(v.Interface())}
	} else {
		args = []reflect.Value{reflect.ValueOf(o.WithRootPermission(ctx)), v.Elem()}
	}
	result := fn.Call(args)
	if rt.NumOut() == 1 {
		// 只返回error, 那就返回一个bool类型值
		if !result[0].IsNil() {
			return false, result[0].Interface().(error)
		}
		return true, nil
	} else {
		// 带有自定义类型的返回值
		if !result[1].IsNil() {
			return nil, result[1].Interface().(error)
		}
		if handle.res.Kind() == reflect.Pointer && result[0].IsNil() {
			return nil, nil
		}
		return formatHandleReturnValue(result[0].Interface()), nil
	}
}

func formatHandleReturnValue(v interface{}) interface{} {
	if isNull(v) {
		return v
	}

	t := reflect.TypeOf(v)
	switch t.Kind() {
	case reflect.Array, reflect.Slice:
		return formatHandleArrayReturnValue(v)
	case reflect.Struct:
		return formatHandleStructReturnValue(v)
	case reflect.Pointer:
		vo := reflect.ValueOf(v)
		return formatHandleReturnValue(vo.Elem().Interface())
	default:
		return v
	}
}

func formatHandleArrayReturnValue(source interface{}) interface{} {
	result := []interface{}{}
	sourceValue := reflect.ValueOf(source)
	for i := 0; i < sourceValue.Len(); i++ {
		evalue := formatHandleReturnValue(sourceValue.Index(i).Interface())
		result = append(result, evalue)
	}
	return result
}

func formatHandleStructReturnValue(source interface{}) interface{} {
	result := map[string]interface{}{}
	rv := reflect.ValueOf(source)
	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i)
		ft := rv.Type().Field(i)
		tag := ft.Tag.Get("json")
		if len(tag) > 0 {
			result[tag] = formatHandleReturnValue(field.Interface())
		} else {
			result[firstLower(ft.Name)] = formatHandleReturnValue(field.Interface())
		}
	}
	return result
}

var tarnsKind = struct{}{}

func (o *Objectql) getGraphqlArgsFromHandle(ctx context.Context, handle *Handle) (graphql.FieldConfigArgument, error) {
	ctx = context.WithValue(ctx, tarnsKind, "input")
	fnType := reflect.TypeOf(handle.Resolve)
	gt, err := o.goTypeToGraphqlInputOrOutputType(ctx, fnType.In(1))
	if err != nil {
		return nil, err
	}
	result := graphql.FieldConfigArgument{}
	for name, field := range gt.(*graphql.InputObject).Fields() {
		result[name] = &graphql.ArgumentConfig{
			Type: field.Type,
		}
	}
	return result, nil
}

func (o *Objectql) getGraphqlReturnFromHandle(ctx context.Context, mutation *Handle) (graphql.Output, error) {
	ctx = context.WithValue(ctx, tarnsKind, "output")
	fnType := reflect.TypeOf(mutation.Resolve)
	if fnType.NumOut() == 1 {
		return graphql.Boolean, nil
	}
	gt, err := o.goTypeToGraphqlInputOrOutputType(ctx, fnType.Out(0))
	if err != nil {
		return nil, err
	}
	return gt, nil
}

func (o *Objectql) getGrpahqlObjectMutationForm(object *Object) graphql.Input {
	fields := graphql.InputObjectConfigFieldMap{}
	for _, cur := range object.Fields {
		if cur.Api == "_id" || cur.Api == "__aggregate" {
			continue
		}
		switch cur.Type.(type) {
		// case *ExpandType, *ExpandsType, *FormulaType, *AggregationType:
		case *ExpandType, *ExpandsType:
			continue
		}
		fields[cur.Api] = &graphql.InputObjectFieldConfig{
			Type: o.fieldTypeToInputGraphqlType(cur.Type),
		}
	}
	return graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   object.Api + "__form",
		Fields: fields,
	})
}

func (o *Objectql) graphqlMutationInsertResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	args := formatNullValue(p.Args)
	err := o.graphqlMutationInsertArgumentValidate(object, args)
	if err != nil {
		return nil, err
	}
	var pos *IndexPosition
	if !isNull(p.Args["index"]) {
		pos = &IndexPosition{
			Index: gconv.Int(args["index"]),
			Dir:   gconv.Int(args["dir"]),
		}
	}
	doc := args["doc"].(map[string]interface{})
	objectId, err := o.insertHandle(ctx, object.Api, doc, pos)
	if err != nil {
		return nil, err
	}
	return o.graphqlMutationQueryOne(ctx, p, object, objectId)
}

func (o *Objectql) graphqlMutationInsertArgumentValidate(object *Object, args map[string]interface{}) error {
	if isNull(args["doc"]) {
		return fmt.Errorf(`mutation %s__insert method arg "doc" can't be null`, object.Api)
	}
	if !isNull(args["index"]) {
		dir := gconv.Int(args["dir"])
		if !(dir == 1 || dir == -1) {
			return fmt.Errorf(`mutation %s__insert method arg "dir" can't be %v`, object.Api, args["dir"])
		}
	}
	return nil
}

func (o *Objectql) graphqlMutationUpdateResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	args := formatNullValue(p.Args)
	err := o.graphqlMutationUpdateArgumentValidate(object, args)
	if err != nil {
		return nil, err
	}
	filterJsonStr := gconv.String(p.Args["filter"])
	filter, err := o.exceptParseMongoFindFilters(ctx, filterJsonStr)
	if err != nil {
		return nil, err
	}
	doc := formatNullValue(p.Args["doc"].(map[string]interface{}))
	// 找出需要被修改的id数组
	list, err := o.mongoFindAllEx(ctx, object.Api, findAllExOptions{
		Fields: []string{"_id"},
		Filter: filter,
	})
	if err != nil {
		return nil, err
	}
	// 使用事务来进行操作
	_, err = o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		for _, item := range list {
			err := o.updateHandleRaw(ctx, object.Api, gconv.String(item["_id"]), doc, false)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	//
	fieds := o.parseMongoQueryFields(p)
	result, err := o.mongoFindAllEx(ctx, object.Api, findAllExOptions{
		Fields: fieds,
		Filter: filter,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Objectql) graphqlMutationUpdateArgumentValidate(object *Object, args map[string]interface{}) error {
	if isNull(args["doc"]) {
		return fmt.Errorf(`mutation %s__update method arg "doc" can't be null`, object.Api)
	}
	if isNull(args["filter"]) {
		return fmt.Errorf(`mutation %s__update method arg "filter" can't be null`, object.Api)
	}
	return nil
}

func (o *Objectql) graphqlMutationUpdateByIdResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	objectId, ok := p.Args["_id"].(string)
	if ok {
		m, ok2 := p.Args["doc"].(map[string]interface{})
		if ok2 {
			m = formatNullValue(m)
			err := o.updateHandle(ctx, object.Api, objectId, m, false)
			if err != nil {
				return nil, err
			}
			return o.graphqlMutationQueryOne(ctx, p, object, objectId)
		}
	}
	return nil, nil
}

func (o *Objectql) graphqlMutationMoveResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	err := o.graphqlMutationMoveArgumentValidate(object, p.Args)
	if err != nil {
		return nil, err
	}
	objectId := gconv.String(p.Args["_id"])
	index := gconv.Int(p.Args["index"])
	dir := gconv.Int(p.Args["dir"])
	err = o.moveHandle(ctx, object.Api, objectId, IndexPosition{
		Index: index,
		Dir:   dir,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (o *Objectql) graphqlMutationMoveArgumentValidate(object *Object, args map[string]interface{}) error {
	objectId := gconv.String(args["_id"])
	if len(objectId) == 0 {
		return fmt.Errorf(`mutation %s__move method arg "_id" can't be empty`, object.Api)
	}
	if isNull(args["index"]) {
		return fmt.Errorf("mutation %s__move index can't be embty", object.Api)
	}
	dir := gconv.Int(args["dir"])
	if !(dir == 1 || dir == -1) {
		return fmt.Errorf(`mutation %s__move method arg "dir" can't be %d`, object.Api, dir)
	}
	return nil
}

func (o *Objectql) graphqlMutationDeleteResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	filterJsonStr := gconv.String(p.Args["filter"])
	filter, err := o.exceptParseMongoFindFilters(ctx, filterJsonStr)
	if err != nil {
		return false, err
	}
	// 查询出要被删除的数据
	list, err := o.mongoFindAllEx(ctx, object.Api, findAllExOptions{
		Fields: []string{"_id"},
		Filter: filter,
	})
	if err != nil {
		return false, err
	}
	// 使用事务来进行操作
	_, err = o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		for _, item := range list {
			err := o.deleteHandleRaw(ctx, object.Api, gconv.String(item["_id"]))
			if err != nil {
				return false, err
			}
		}
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (o *Objectql) graphqlMutationDeleteByIdResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	args := formatNullValue(p.Args)
	err := o.graphqlMutationDeleteArgumentValidate(object, args)
	if err != nil {
		return false, err
	}
	objectId := gconv.String(args["_id"])
	err = o.deleteHandle(ctx, object.Api, objectId)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (o *Objectql) graphqlMutationDeleteArgumentValidate(object *Object, args map[string]interface{}) error {
	objectId := gconv.String(args["_id"])
	if len(objectId) == 0 {
		return fmt.Errorf(`mutation %s__delete method arg "_id" can't be empty`, object.Api)
	}
	return nil
}

func (o *Objectql) getObjectBeforeValues(ctx context.Context, object *Object, id string) (beforeValues map[string]interface{}, err error) {
	apis := getObjectRelationObjectApis(object)
	if len(apis) > 0 {
		selects := stringArrayToMongodbSelects(apis)
		if len(selects) > 0 {
			arr := getSelectMapKeys(selects)
			beforeValues, err = o.mongoFindOne(ctx, object.Api, bson.M{"_id": ObjectIdFromHex(id)}, strings.Join(arr, ","))
			if err != nil {
				return
			}
		}
	}
	return
}

func getSelectMapKeys(v bson.M) []string {
	var result []string
	for k := range v {
		result = append(result, k)
	}
	return result
}

func getObjectRelationObjectApis(object *Object) []string {
	var result []string
	for _, field := range object.Fields {
		if IsRelateType(field.Type) {
			result = append(result, field.Api)
		}
	}
	return result
}

func (o *Objectql) goTypeToGraphqlInputOrOutputType(ctx context.Context, tpe reflect.Type) (graphql.Output, error) {
	switch tpe.Kind() {
	case reflect.Bool:
		return graphql.Boolean, nil
	case reflect.Int:
		return graphql.Int, nil
	case reflect.Int8:
		return graphql.Int, nil
	case reflect.Int16:
		return graphql.Int, nil
	case reflect.Int32:
		return graphql.Int, nil
	case reflect.Int64:
		return graphql.Int, nil
	case reflect.Uint:
		return graphql.Int, nil
	case reflect.Uint8:
		return graphql.Int, nil
	case reflect.Uint16:
		return graphql.Int, nil
	case reflect.Uint32:
		return graphql.Int, nil
	case reflect.Uint64:
		return graphql.Int, nil
	case reflect.Uintptr:
		return graphql.Int, nil
	case reflect.Float32:
		return graphql.Float, nil
	case reflect.Float64:
		return graphql.Float, nil
	case reflect.String:
		return graphql.String, nil
	case reflect.Pointer:
		return o.goTypeToGraphqlInputOrOutputType(ctx, tpe.Elem())
	case reflect.Array, reflect.Slice:
		et, err := o.goTypeToGraphqlInputOrOutputType(ctx, tpe.Elem())
		if err != nil {
			return nil, err
		}
		return graphql.NewList(et), nil
	case reflect.Struct:
		return o.goStructTypeToGraphqlInputOrOutputType(ctx, tpe)
	default:
		return graphqlAny, nil
		// return nil, fmt.Errorf("paramTypeToGraphqlInputType not support kind %v", tpe.Kind())
	}
}

var pattern = regexp.MustCompile(`[\./-]`)

func (o *Objectql) goStructTypeToGraphqlInputOrOutputType(ctx context.Context, tpe reflect.Type) (out graphql.Output, err error) {
	switch tpe {
	case reflect.TypeOf(time.Time{}):
		return graphql.DateTime, nil
	}

	kind := ctx.Value(tarnsKind).(string)
	objectName := kind + "_" + pattern.ReplaceAllString(tpe.PkgPath(), "_") + "_" + tpe.Name()
	if o.gstructTypes.Contains(objectName) {
		return o.gstructTypes.Get(objectName).(graphql.Output), nil
	}
	defer func() {
		// 添加类型到缓存当中
		if err == nil {
			o.gstructTypes.Set(objectName, out)
		}
	}()

	raw := map[string]graphql.Output{}
	desc := map[string]string{}
	for i := 0; i < tpe.NumField(); i++ {
		field := tpe.Field(i)
		if !field.IsExported() {
			continue
		}
		if field.Name == "Meta" {
			continue
		}
		tag := field.Tag.Get("json")
		var gname string
		if len(tag) > 0 {
			gname = tag
		} else {
			gname = firstLower(field.Name)
		}
		gtype, err := o.goTypeToGraphqlInputOrOutputType(ctx, field.Type)
		if err != nil {
			return nil, err
		}
		raw[gname] = gtype
		desc[gname] = field.Tag.Get("comment")
	}

	if kind == "input" {
		fields := graphql.InputObjectConfigFieldMap{}
		for name, tpe := range raw {
			fields[name] = &graphql.InputObjectFieldConfig{
				Type:        tpe,
				Description: desc[name],
			}
		}
		out = graphql.NewInputObject(graphql.InputObjectConfig{
			Name:   objectName,
			Fields: fields,
		})
		return
	} else {
		fields := graphql.Fields{}
		for name, tpe := range raw {
			fields[name] = &graphql.Field{
				Name:        name,
				Type:        tpe,
				Description: desc[name],
			}
		}
		out = graphql.NewObject(graphql.ObjectConfig{
			Name:   objectName,
			Fields: fields,
		})
		return
	}
}

// graphql mutation表单字段的类型
func (o *Objectql) fieldTypeToInputGraphqlType(tpe Type) graphql.Output {
	switch n := tpe.(type) {
	case *BoolType:
		return graphql.Boolean
	case *IntType:
		return graphql.Int
	case *FloatType:
		return graphql.Float
	case *StringType:
		return graphql.String
	case *DateTimeType, *DateType, *TimeType:
		return graphql.DateTime
	case *RelateType:
		return graphql.String
	case *ArrayType:
		return graphql.NewList(o.fieldTypeToInputGraphqlType(n.Type))
	case *FormulaType:
		return o.fieldTypeToInputGraphqlType(n.Type)
	case *AggregationType:
		return o.fieldTypeToInputGraphqlType(n.Type)
	}
	return nil
}

// graphql object对象定义的类型
func (o *Objectql) getGraphqlFieldType(tpe Type) graphql.Output {
	switch n := tpe.(type) {
	case *BoolType:
		return graphql.Boolean
	case *IntType:
		return graphql.Int
	case *FloatType:
		return graphql.Float
	case *StringType:
		return graphql.String
	case *DateTimeType, *DateType, *TimeType:
		return graphql.DateTime
	case *RelateType:
		return graphql.String
	case *ExpandType:
		return o.getGraphqlObject(n.ObjectApi)
	case *ExpandsType:
		return graphql.NewList(o.getGraphqlObject(n.ObjectApi))
	case *ObjectIDType:
		return graphql.String
	case *FormulaType:
		return o.getGraphqlFieldType(n.Type)
	case *AggregationType:
		return o.getGraphqlFieldType(n.Type)
	case *ArrayType:
		return graphql.NewList(o.getGraphqlFieldType(n.Type))
	}
	return nil
}

func formatNullValue(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		switch n := v.(type) {
		case map[string]interface{}:
			formatNullValue(n)
		case graphql.NullValue:
			m[k] = nil
		}
	}
	return m
}
