package main

import (
	"github.com/aundis/graphql"
	"gopkg.in/mgo.v2/bson"
)

func getObjectSchema(object Object) *graphql.ObjectConfig {
	config := &graphql.ObjectConfig{
		Name:   object.Api,
		Fields: nil,
	}
	fields := graphql.Fields{
		"_id": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				source, _ := p.Source.(bson.M)
				if source != nil && source["_id"] != nil {
					return source["_id"].(bson.ObjectId).Hex(), nil
				}
				return nil, nil
			},
		},
	}
	for _, field := range object.Fields {
		fields[field.Api] = &graphql.Field{
			Name: field.Api,
			Type: toGraphqlType(field.Type),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				source, _ := p.Source.(bson.M)
				if source != nil && source[field.Api] != nil {
					return source[field.Api], nil
				}
				return nil, nil
			},
			Description: field.Comment,
		}
	}
	config.Fields = fields
	return config
}

func toGraphqlType(source Type) graphql.Output {
	switch source {
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
