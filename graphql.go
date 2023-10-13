package objectql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/aundis/graphql"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (o *Objectql) getGraphqlObject(name string) *graphql.Object {
	if o.gobjects.Contains(name) {
		return o.gobjects.Get(name).(*graphql.Object)
	}
	return nil
}

func (o *Objectql) initObjectGraphqlQuery(ctx context.Context, querys graphql.Fields, object *Object) error {
	querys[object.Api] = &graphql.Field{
		Type: graphql.NewList(o.getGraphqlObject(object.Api)),
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
			"top": &graphql.ArgumentConfig{
				Type: graphql.Int,
			},
			"skip": &graphql.ArgumentConfig{
				Type: graphql.Int,
			},
			"sort": &graphql.ArgumentConfig{
				Type: graphql.NewList(graphql.String),
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlQueryListResolver(p.Context, p, object)
		},
	}

	querys[object.Api+"__one"] = &graphql.Field{
		Type: o.getGraphqlObject(object.Api),
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
			"top": &graphql.ArgumentConfig{
				Type: graphql.Int,
			},
			"skip": &graphql.ArgumentConfig{
				Type: graphql.Int,
			},
			"sort": &graphql.ArgumentConfig{
				Type: graphql.NewList(graphql.String),
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlQueryOneResolver(p.Context, p, object)
		},
	}

	querys[object.Api+"__count"] = &graphql.Field{
		Type: graphql.Int,
		Args: graphql.FieldConfigArgument{
			"filter": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
			"top": &graphql.ArgumentConfig{
				Type: graphql.Int,
			},
			"skip": &graphql.ArgumentConfig{
				Type: graphql.Int,
			},
			"sort": &graphql.ArgumentConfig{
				Type: graphql.NewList(graphql.String),
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
		querys[object.Api+"__"+handle.Api] = &graphql.Field{
			Type: rtn,
			Args: args,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return o.handleGraphqlResovler(p.Context, p, handle)
			},
		}
	}
	return nil
}

func (o *Objectql) graphqlQueryListResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	filter, err := o.parseMongoFindFilters(ctx, p)
	if err != nil {
		return nil, err
	}
	options, err := o.parseMongoFindOptions(ctx, p)
	if err != nil {
		return nil, err
	}
	cursor, err := o.getCollection(object.Api).Find(ctx, filter, options)
	if err != nil {
		return nil, err
	}
	var result []bson.M
	err = cursor.All(ctx, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Objectql) graphqlQueryOneResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	filter, err := o.parseMongoFindFilters(ctx, p)
	if err != nil {
		return nil, err
	}
	options, err := o.parseMongoFindOneOptinos(ctx, p)
	if err != nil {
		return nil, err
	}
	var result bson.M
	err = o.getCollection(object.Api).FindOne(ctx, filter, options).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Objectql) graphqlQueryCountResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// 对象权限检验
	err := o.checkObjectPermission(ctx, object.Api, ObjectQuery)
	if err != nil {
		return nil, err
	}
	filter, err := o.parseMongoFindFilters(ctx, p)
	if err != nil {
		return nil, err
	}
	count, err := o.getCollection(object.Api).CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}
	return count, nil
}

func (o *Objectql) parseMongoFindOneOptinos(ctx context.Context, p graphql.ResolveParams) (*options.FindOneOptions, error) {
	findOneOptins := options.FindOne()
	selects := o.parseMongoFiledSelects(ctx, p)
	if selects != nil {
		findOneOptins.SetProjection(selects)
	}
	skip := p.Args["skip"]
	if skip != nil {
		findOneOptins.SetSkip(gconv.Int64(skip))
	}
	sort := p.Args["sort"]
	if sort != nil {
		findOneOptins.SetSort(stringsToSortMap(gconv.Strings(sort)))
	}
	return findOneOptins, nil
}

func (o *Objectql) parseMongoFindOptions(ctx context.Context, p graphql.ResolveParams) (*options.FindOptions, error) {
	findOptions := options.Find()
	selects := o.parseMongoFiledSelects(ctx, p)
	if selects != nil {
		findOptions.SetProjection(selects)
	}
	skip := p.Args["skip"]
	if skip != nil {
		findOptions.SetSkip(gconv.Int64(skip))
	}
	top := p.Args["top"]
	if top != nil {
		findOptions.SetLimit(gconv.Int64(top))
	}
	sort := p.Args["sort"]
	if sort != nil {
		findOptions.SetSort(stringsToSortMap(gconv.Strings(sort)))
	}
	return findOptions, nil
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

func (o *Objectql) parseMongoFindFilters(ctx context.Context, p graphql.ResolveParams) (bson.M, error) {
	filter := p.Args["filter"]
	filterMgn := bson.M{}
	if filter != nil && len(filter.(string)) > 0 {
		// TODO: 详细了解一下UnmarshalExtJSON的用法
		err := bson.UnmarshalExtJSON([]byte(filter.(string)), true, &filterMgn)
		if err != nil {
			return nil, err
		}
	}
	return formatMongoFilter(filterMgn).(bson.M), nil
}

func formatMongoFilter(data interface{}) interface{} {
	switch n := data.(type) {
	case string:
		id, err := primitive.ObjectIDFromHex(n)
		if err == nil {
			return id
		} else {
			return n
		}
	case primitive.M:
		for k, v := range n {
			n[k] = formatMongoFilter(v)
		}
		return n
	case primitive.A:
		var list primitive.A
		for _, v := range n {
			list = append(list, formatMongoFilter(v))
		}
		return list
	default:
		return data
	}
}

func (o *Objectql) initObjectGraphqlMutation(ctx context.Context, mutations graphql.Fields, object *Object) error {
	form := o.getGrpahqlObjectMutationForm(object)
	// 新增
	mutations[object.Api+"__insert"] = &graphql.Field{
		Type: o.getGraphqlObject(object.Api),
		Args: graphql.FieldConfigArgument{
			"doc": &graphql.ArgumentConfig{
				Type: form,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationInsertResolver(p.Context, p, object)
		},
	}
	// 修改
	mutations[object.Api+"__update"] = &graphql.Field{
		Type: o.getGraphqlObject(object.Api),
		Args: graphql.FieldConfigArgument{
			"_id": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
			"doc": &graphql.ArgumentConfig{
				Type: form,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationUpdateResolver(p.Context, p, object)
		},
	}
	// 删除
	mutations[object.Api+"__delete"] = &graphql.Field{
		Type: graphql.Boolean,
		Args: graphql.FieldConfigArgument{
			"_id": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.graphqlMutationDeleteResolver(p.Context, p, object)
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
		mutations[object.Api+"__"+handle.Api] = &graphql.Field{
			Type: rtn,
			Args: args,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return o.handleGraphqlResovler(p.Context, p, handle)
			},
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

func (o *Objectql) handleGraphqlResovler(ctx context.Context, p graphql.ResolveParams, handle *Handle) (interface{}, error) {
	v := reflect.New(unPointerType(handle.req))
	err := gconv.Struct(p.Args, v.Interface())
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
		args = []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(v.Interface())}
	} else {
		args = []reflect.Value{reflect.ValueOf(ctx), v.Elem()}
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
		if cur.Api == "_id" {
			continue
		}
		switch cur.Type.(type) {
		case *ExpandType, *ExpandsType, *FormulaType, *AggregationType:
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
	if m, ok := p.Args["doc"].(map[string]interface{}); ok {
		m = formatNullValue(m)
		objectId, err := o.insertHandle(ctx, object.Api, m)
		if err != nil {
			return nil, err
		}
		return o.graphqlMutationQueryOne(ctx, p, object, objectId)
	}
	return nil, nil
}

func (o *Objectql) graphqlMutationUpdateResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
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

func (o *Objectql) graphqlMutationDeleteResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
	// source, _ := p.Source.(bson.M)
	id, ok := p.Args["_id"].(string)
	if ok {
		err := o.deleteHandle(ctx, object.Api, id)
		if err != nil {
			return false, err
		}
	}
	return true, nil
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
		return nil, fmt.Errorf("paramTypeToGraphqlInputType not support kind %v", tpe.Kind())
	}
}

var pattern = regexp.MustCompile(`[\./]`)

func (o *Objectql) goStructTypeToGraphqlInputOrOutputType(ctx context.Context, tpe reflect.Type) (out graphql.Output, err error) {
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
	}

	if kind == "input" {
		fields := graphql.InputObjectConfigFieldMap{}
		for name, tpe := range raw {
			fields[name] = &graphql.InputObjectFieldConfig{Type: tpe}
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
				Name: name,
				Type: tpe,
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
	case *DateTimeType:
		return graphql.DateTime
	case *RelateType:
		return graphql.String
	case *ArrayType:
		return graphql.NewList(o.fieldTypeToInputGraphqlType(n.Type))
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
	case *DateTimeType:
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
		if _, ok := v.(graphql.NullValue); ok {
			m[k] = nil
		}
	}
	return m
}
