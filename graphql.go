package main

import (
	"context"
	"strings"

	"github.com/aundis/graphql"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (o *Objectql) GraphqlExec(request string) {
	// graphql.Do(graphql.Params{
	// 	Schema:        schema,
	// 	RequestString: params.Query,
	// })
}

func (o *Objectql) initObjectGraphqlQuery(querys graphql.Fields, object *Object) {
	querys[object.Api] = &graphql.Field{
		Type: graphql.NewList(o.gobjects[object.Api]),
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
		Type: o.gobjects[object.Api],
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
		findOneOptins.SetSort(gconv.Strings(sort))
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
		findOptions.SetSort(gconv.Strings(sort))
	}
	return findOptions, nil
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
	case []interface{}:
		var list []interface{}
		for _, v := range n {
			list = append(list, formatMongoFilter(v))
		}
		return list
	default:
		return data
	}
}

func (o *Objectql) initObjectGraphqlMutation(mutations graphql.Fields, object *Object) {
	fields := graphql.InputObjectConfigFieldMap{}
	for _, cur := range object.Fields {
		if cur.Type == Formula || cur.Type == Aggregation {
			continue
		}
		fields[cur.Api] = &graphql.InputObjectFieldConfig{
			Type: o.toInputGraphqlType(cur),
		}
	}
	form := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   object.Api + "__form",
		Fields: fields,
	})
	// 新增
	mutations[object.Api+"__insert"] = &graphql.Field{
		Type: o.gobjects[object.Api],
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
		Type: o.gobjects[object.Api],
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
		if field.Type == Relate {
			result = append(result, field.Api)
		}
	}
	return result
}

func (o *Objectql) toInputGraphqlType(field *Field) graphql.Output {
	switch field.Type {
	case Bool, Int, Float, String:
		return o.basicToGrpuahType(field.Type)
	case Relate:
		return graphql.String
	}
	return nil
}

func (o *Objectql) toGraphqlType(field *Field, gapi string) graphql.Output {
	switch field.Type {
	case Bool, Int, Float, String:
		return o.basicToGrpuahType(field.Type)
	case Relate:
		if strings.Contains(gapi, "__expand") {
			data := field.Data.(*RelateData)
			return o.gobjects[data.ObjectApi]
		} else {
			return graphql.String
		}
	case Formula:
		data := field.Data.(*FormulaData)
		return o.basicToGrpuahType(data.Type)
	case Aggregation:
		data := field.Data.(*AggregationData)
		return o.basicToGrpuahType(data.Type)
	}
	return nil
}

func (o *Objectql) basicToGrpuahType(tpe FieldType) graphql.Output {
	switch tpe {
	case Bool:
		return graphql.Boolean
	case Int:
		return graphql.Int
	case Float:
		return graphql.Float
	case String:
		return graphql.String
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
