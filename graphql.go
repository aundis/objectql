package main

import (
	"context"
	"strings"

	"github.com/aundis/graphql"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

func (o *Objectql) InitObjectGraphqlQuery(object *Object) {
	o.query[object.Api] = &graphql.Field{
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

	o.query[object.Api+"__one"] = &graphql.Field{
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

	o.query[object.Api+"__count"] = &graphql.Field{
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
	filter, err := o.parseMongoFindFilters(ctx, p)
	if err != nil {
		return nil, err
	}
	options, err := o.parseMongoFindOneOptinos(ctx, p)
	if err != nil {
		return nil, err
	}
	var one bson.M
	result := o.getCollection(object.Api).FindOne(ctx, filter, options)
	err = result.Decode(&one)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Objectql) graphqlQueryCountResolver(ctx context.Context, p graphql.ResolveParams, object *Object) (interface{}, error) {
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
		err := bson.UnmarshalJSON([]byte(filter.(string)), &filterMgn)
		if err != nil {
			return nil, err
		}
	}
	return filterMgn, nil
}

// func (o *Objectql) parseMgoQuery(ctx context.Context, p graphql.ResolveParams, object *Object) (*mgo.Query, error) {
// 	filter := p.Args["filter"]
// 	skip := p.Args["skip"]
// 	top := p.Args["top"]
// 	sort := p.Args["sort"]
// 	filterMgn := bson.M{}
// 	if filter != nil && len(filter.(string)) > 0 {
// 		err := bson.UnmarshalJSON([]byte(filter.(string)), &filterMgn)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	// 字段过滤
// 	selects := getGraphqlSelectFieldNames(p)
// 	mgoSelects := stringArrayToMongodbSelects(selects)
// 	// 开始组合查询语句
// 	query, err := o.getCollection(object.Api).Find(ctx, filterMgn)
// 	query.All()

// 	if skip != nil {
// 		query = query.Skip(skip.(int))
// 	}
// 	if top != nil {
// 		query = query.Limit(top.(int))
// 	}
// 	if sort != nil {
// 		sort = query.Sort(gconv.Strings(sort)...)
// 	}
// 	return query, nil
// }

func (o *Objectql) InitObjectGraphqlMutation(object *Object) {
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
	o.mutation[object.Api+"__insert"] = &graphql.Field{
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
	o.mutation[object.Api+"__update"] = &graphql.Field{
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
	o.mutation[object.Api+"__delete"] = &graphql.Field{
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
			err := o.updateHandle(ctx, object.Api, objectId, m)
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

func (o *Objectql) graphqlMutationQueryOne(ctx context.Context, p graphql.ResolveParams, object *Object, id string) (interface{}, error) {
	var one bson.M
	// 字段过滤
	selects := getGraphqlSelectFieldNames(p)
	mgoSelects := stringArrayToMongodbSelects(selects)
	// 从数据库获取数据
	err := o.getCollection(ctx, object.Api).Find(bson.M{"_id": bson.ObjectIdHex(id)}).Select(mgoSelects).One(&one)
	if err != nil {
		return nil, err
	}
	return one, nil
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
