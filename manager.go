package main

import (
	"fmt"
	"strings"

	"github.com/aundis/formula"
	"github.com/aundis/graphql"
	"gopkg.in/mgo.v2/bson"
)

var global *Objectql

func Manager() *Objectql {
	if global == nil {
		global = &Objectql{
			query:    map[string]*graphql.Field{},
			mutation: map[string]*graphql.Field{},
			objects:  []Object{},
			gobjects: map[string]*graphql.Object{},
		}
	}
	return global
}

type Objectql struct {
	query    graphql.Fields
	mutation graphql.Fields
	objects  []Object
	gobjects map[string]*graphql.Object
	list     []*Object
}

func (o *Objectql) InitObjects(list []*Object) {
	o.list = list
	// 为所有的field设定parent
	for _, v := range list {
		for _, f := range v.Fields {
			f.Parent = v
		}
	}
	// 解析公式及其引用关系
	for _, object := range list {
		for _, cur := range object.Fields {
			// 累计总和
			if cur.Type == Aggregation {
				adata := cur.Data.(*AggregationData)
				// 关联字段(相关表)
				relateField, err := FindFieldFromName(list, adata.Object, adata.Relate)
				if err != nil {
					panic(err)
				}
				relateField.relations = append(relateField.relations, &RelationFiledInfo{
					ThroughField: relateField,
					TargetField:  cur,
				})
				// 被统计的字段
				beCountedField, err := FindFieldFromName(list, adata.Object, adata.Field)
				if err != nil {
					panic(err)
				}
				beCountedField.relations = append(beCountedField.relations, &RelationFiledInfo{
					ThroughField: relateField,
					TargetField:  cur,
				})
			}
			// 公式字段
			if cur.Type == Formula {
				fdata := cur.Data.(*FormulaData)
				var err error
				fdata.SourceCode, err = formula.ParseSourceCode([]byte(fdata.Formula))
				if err != nil {
					panic(err)
				}
				fields, err := formula.ResolveReferenceFields(fdata.SourceCode)
				if err != nil {
					panic(err)
				}
				// 字段挂载
				for _, fstr := range fields {
					arr := strings.Split(fstr, ".")
					if len(arr) != 1 && len(arr) != 2 {
						panic("filed must is e.g. field or object.field")
					}
					// 找到引用的字段(在本对象找到引用类型的字段)
					relatedField, err := FindFieldFromName(list, object.Api, arr[0])
					if err != nil {
						panic(err)
					}

					if len(arr) == 1 {
						relatedField.relations = append(relatedField.relations, &RelationFiledInfo{
							ThroughField: nil,
							TargetField:  cur,
						})
					} else {
						if relatedField.Type != Relate {
							panic(fmt.Sprintf("object %s field %s not a relate field", object.Api, arr[0]))
						}
						relatedField.relations = append(relatedField.relations, &RelationFiledInfo{
							ThroughField: relatedField,
							TargetField:  cur,
						})
						relateData := relatedField.Data.(*RelateData)
						beCountedField, err := FindFieldFromName(list, relateData.ObjectApi, arr[1])
						if err != nil {
							panic(err)
						}
						beCountedField.relations = append(beCountedField.relations, &RelationFiledInfo{
							ThroughField: relatedField,
							TargetField:  cur,
						})
					}
				}
			}
		}
	}
	// 局部初始化全部的对象,因为后面可能相关表需要相互引用
	for _, v := range list {
		// 先填充后面再替换
		fields := graphql.Fields{
			"_id": &graphql.Field{
				Name: "_id",
				Type: graphql.String,
			},
		}
		for _, f := range v.Fields {
			fields[f.Api] = &graphql.Field{
				Name: f.Api,
				Type: graphql.String,
			}
		}
		o.gobjects[v.Api] = graphql.NewObject(graphql.ObjectConfig{
			Name:   v.Api,
			Fields: fields,
		})
	}
	for _, v := range list {
		o.FullGraphqlObject(o.gobjects[v.Api], v)
		o.InitObjectQuery(v)
		o.InitObjectMutation(v)
	}
}

func (o *Objectql) GetObjectCount(api string) (int, error) {
	c := session.DB("test").C(api)
	return c.Count()
}

func (o *Objectql) GetObjectByID(api, id string) (bson.M, error) {
	c := session.DB("test").C(api)
	var result bson.M
	err := c.Find(bson.M{"_id": bson.ObjectIdHex(id)}).One(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Objectql) GetObjectList(api string) ([]bson.M, error) {
	c := session.DB("test").C(api)
	var result []bson.M
	err := c.Find(bson.M{}).All(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func stringArrayToMongodbSelects(arr []string) bson.M {
	result := bson.M{}
	for _, item := range arr {
		result[item] = 1
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

func (o *Objectql) InsertObject(api string, m bson.M) (bson.M, error) {
	newId := bson.NewObjectId()
	c := session.DB("test").C(api)
	m["_id"] = newId
	err := c.Insert(m)
	if err != nil {
		return nil, err
	}
	return o.GetObjectByID(api, newId.Hex())
}

func (o *Objectql) UpdateObject(api string, id string, m bson.M) (bson.M, error) {
	c := session.DB("test").C(api)
	err := c.Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{
		"$set": m,
	})
	if err != nil {
		return nil, err
	}
	return o.GetObjectByID(api, id)
}

func (o *Objectql) DeleteObject(api, id string) error {
	return session.DB("test").C(api).RemoveId(bson.ObjectIdHex(id))
}

func (o *Objectql) FullGraphqlObject(gobj *graphql.Object, object *Object) {
	gobj.AddFieldConfig("_id", &graphql.Field{
		Type: graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			source, _ := p.Source.(bson.M)
			if source != nil && source["_id"] != nil {
				return source["_id"].(bson.ObjectId).Hex(), nil
			}
			return nil, nil
		},
	})
	for _, field := range object.Fields {
		api := field.Api
		gobj.AddFieldConfig(api, &graphql.Field{
			Name: api,
			Type: toGraphqlType(field),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				source, _ := p.Source.(bson.M)
				if source != nil && source[api] != nil {
					return source[api], nil
				}
				return nil, nil
			},
			Description: field.Comment,
		})
	}
}

func (o *Objectql) GetSchema() (graphql.Schema, error) {
	// TODO: 加个版本号, 可以动态增删对象
	return graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name:   "RootQuery",
			Fields: o.query,
		},
		),
		Mutation: graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: o.mutation,
		}),
	})
}
