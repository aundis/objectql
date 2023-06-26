package main

import (
	"github.com/aundis/graphql"
)

func (o *Objectql) InitObjectQuery(object *Object) {
	o.query[object.Api] = &graphql.Field{
		Type: graphql.NewList(o.gobjects[object.Api]),
		Args: graphql.FieldConfigArgument{
			"_id": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
		},
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.GetObjectList(object.Api)

			// id, ok := p.Args["_id"].(string)
			// if ok {
			// 	return o.GetObjectByID(object.Api, id)
			// }
			// return nil, nil
		},
	}

	o.query[object.Api+"__count"] = &graphql.Field{
		Type: graphql.Int,
		Args: nil,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return o.GetObjectCount(object.Api)

			// id, ok := p.Args["_id"].(string)
			// if ok {
			// 	return o.GetObjectByID(object.Api, id)
			// }
			// return nil, nil
		},
	}
}

func (o *Objectql) InitObjectMutation(object *Object) {
	fields := graphql.InputObjectConfigFieldMap{}
	for _, cur := range object.Fields {
		if cur.Type == Formula || cur.Type == Aggregation {
			continue
		}
		fields[cur.Api] = &graphql.InputObjectFieldConfig{
			Type: toGraphqlType(cur),
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
			if m, ok := p.Args["doc"].(map[string]interface{}); ok {
				m = formatNullValue(m)
				objectId, err := o.Insert(object.Api, m)
				if err != nil {
					return nil, err
				}
				return o.GetObjectByID(object.Api, objectId)
			}
			return nil, nil
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
			id, ok := p.Args["_id"].(string)
			if ok {
				m, ok2 := p.Args["doc"].(map[string]interface{})
				if ok2 {
					m = formatNullValue(m)
					err := o.Update(object.Api, id, m)
					if err != nil {
						return nil, err
					}
					return o.GetObjectByID(object.Api, id)
				}
			}
			return nil, nil
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
			// source, _ := p.Source.(bson.M)
			id, ok := p.Args["_id"].(string)
			if ok {
				o.Delete(object.Api, id)
			}
			return true, nil
		},
	}
}

func toGraphqlType(field *Field) graphql.Output {
	switch field.Type {
	case Bool, Int, Float, String:
		return basicToGrpuahType(field.Type)
	case Relate:
		return graphql.String
	case Formula:
		data := field.Data.(*FormulaData)
		return basicToGrpuahType(data.Type)
	case Aggregation:
		data := field.Data.(*AggregationData)
		return basicToGrpuahType(data.Type)
	}
	return nil
}

func basicToGrpuahType(tpe FieldType) graphql.Output {
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
