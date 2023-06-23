package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aundis/formula"
	"github.com/aundis/graphql"
	"github.com/gogf/gf/v2/util/gconv"
	"gopkg.in/mgo.v2"
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
			if cur.Kind == Aggregation {
				adata := cur.Data.(AggregationData)
				// 关联字段(相关表)
				relateField, err := findField(list, adata.Object, adata.Relate)
				if err != nil {
					panic(err)
				}
				relateField.Relations = append(relateField.Relations, &RelationFiledInfo{
					ThroughField: relateField,
					TargetField:  cur,
				})
				// 被统计的字段
				beCountedField, err := findField(list, adata.Object, adata.Field)
				if err != nil {
					panic(err)
				}
				beCountedField.Relations = append(beCountedField.Relations, &RelationFiledInfo{
					ThroughField: relateField,
					TargetField:  cur,
				})
			}
			// 公式字段
			if cur.Kind == Formula {
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
					relatedField, err := findField(list, object.Api, arr[0])
					if err != nil {
						panic(err)
					}

					if len(arr) == 1 {
						relatedField.Relations = append(relatedField.Relations, &RelationFiledInfo{
							ThroughField: nil,
							TargetField:  cur,
						})
					} else {
						if relatedField.Kind != Relate {
							panic(fmt.Sprintf("object %s field %s not a relate field", object.Api, arr[0]))
						}
						relatedField.Relations = append(relatedField.Relations, &RelationFiledInfo{
							ThroughField: relatedField,
							TargetField:  cur,
						})
						relateData := relatedField.Data.(RelateData)
						beCountedField, err := findField(list, relateData.ObjectApi, arr[1])
						if err != nil {
							panic(err)
						}
						beCountedField.Relations = append(beCountedField.Relations, &RelationFiledInfo{
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

func findField(list []*Object, objectApi string, fieldApi string) (*Field, error) {
	object := findObject(list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("can't find object %s", objectApi)
	}
	for _, v := range object.Fields {
		if v.Api == fieldApi {
			return v, nil
		}
	}
	return nil, fmt.Errorf("%s object can't find field %s", objectApi, fieldApi)
}

func findObject(list []*Object, api string) *Object {
	for _, object := range list {
		if object.Api == api {
			return object
		}
	}
	return nil
}

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

func (o *Objectql) InitObjectMutation(object *Object) {
	findField := func(api string) *Field {
		for _, cur := range object.Fields {
			if cur.Api == api {
				return cur
			}
		}
		return nil
	}

	fields := graphql.InputObjectConfigFieldMap{}
	for _, cur := range object.Fields {
		if cur.Kind == Formula {
			continue
		}
		fields[cur.Api] = &graphql.InputObjectFieldConfig{
			Type: toGraphqlType(cur.Type),
		}
	}
	form := graphql.NewInputObject(graphql.InputObjectConfig{
		Name:   object.Api + "__form",
		Fields: fields,
	})

	// args := graphql.FieldConfigArgument{}
	// for _, f := range object.Fields {
	// 	args[f.Api] = &graphql.ArgumentConfig{
	// 		Type: toGraphqlType(f.Type),
	// 	}
	// }
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
				// 对于部分字段需要进行二次修改
				for k, v := range m {
					field := findField(k)
					if field != nil {
						switch field.Kind {
						case Relate:
							m[k] = bson.ObjectIdHex(v.(string))
						}
					}
				}
				result, err := o.InsertObject(object.Api, m)
				if err != nil {
					return nil, err
				}
				// 响应字段修改
				objectId := result["_id"].(bson.ObjectId).Hex()
				for k := range m {
					field := findField(k)
					if field != nil {
						o.OnFieldChange(field.Parent, objectId, field, bson.M{})
					}
				}
				return result, nil
			}
			return nil, nil
		},
	}
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
					// 保存相关表的字段
					beforeValues := map[string]interface{}{}
					apis := getObjectRelationObjectApis(object)
					if len(apis) > 0 {
						c := session.DB("test").C(object.Api)
						err := c.Find(bson.M{"_id": bson.ObjectIdHex(id)}).Select(stringArrayToMongodbSelects(apis)).One(&beforeValues)
						if err != nil {
							return nil, err
						}
					}
					// 对于部分字段需要进行二次修改
					for k, v := range m {
						field := findField(k)
						if field != nil {
							switch field.Kind {
							case Relate:
								if id, ok := v.(string); ok {
									m[k] = bson.ObjectIdHex(id)
								}
							}
						}
					}
					_, err := o.UpdateObject(object.Api, id, m)
					if err != nil {
						return nil, err
					}
					for k := range m {
						field := findField(k)
						if field != nil {
							o.OnFieldChange(field.Parent, id, field, beforeValues)
						}
					}
					return o.GetObjectByID(object.Api, id)
				}
			}
			return nil, nil
		},
	}
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
				// 保存相关表的字段
				beforeValues := map[string]interface{}{}
				apis := getObjectRelationObjectApis(object)
				if len(apis) > 0 {
					c := session.DB("test").C(object.Api)
					err := c.Find(bson.M{"_id": bson.ObjectIdHex(id)}).Select(stringArrayToMongodbSelects(apis)).One(&beforeValues)
					if err != nil {
						return nil, err
					}
				}
				// 删除对象
				err := o.DeleteObject(object.Api, id)
				if err != nil {
					return false, err
				}
				// 响应字段修改
				for _, field := range object.Fields {
					o.OnFieldChange(object, id, field, beforeValues)
				}
			}
			return true, nil
		},
	}
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
		if field.Kind == Relate {
			result = append(result, field.Api)
		}
	}
	return result
}

func (o *Objectql) OnFieldChange(object *Object, id string, field *Field, beforeValues bson.M) {
	if len(field.Relations) > 0 {
		for _, r := range field.Relations {
			o.ComputedFiled(object, id, field, r, beforeValues)
		}
	}
}

func (o *Objectql) ComputedFiled(object *Object, id string, mod *Field, info *RelationFiledInfo, beforeValues bson.M) {
	fmt.Printf("comput field %v\n", info)
	// 如果是聚合字段
	if info.TargetField.Kind == Aggregation {
		// 聚合2次, 修改前和修改后
		// 修改前
		objectId := beforeValues[info.ThroughField.Api]
		if objectId != nil {
			o.AggregateField(info.TargetField.Parent, objectId.(bson.ObjectId).Hex(), info.TargetField)
		}
		// 修改后
		data, err := o.GetObjectByID(object.Api, id)
		if err != nil {
			panic(err)
		}
		// 找不到对象就返回
		if data == nil {
			return
		}
		objectId = data[info.ThroughField.Api]
		if objectId != nil {
			o.AggregateField(info.TargetField.Parent, objectId.(bson.ObjectId).Hex(), info.TargetField)
		}
		// if info.TargetField.Parent == object {
		// 	// 聚合自身对象
		// 	data, err := o.GetObjectByID(object.Api, id)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	objectId := data["_id"].(bson.ObjectId).Hex()
		// 	o.AggregateField(object, objectId, info.TargetField)
		// } else {
		// 	// 聚合其他对象
		// 	data, err := o.GetObjectByID(object.Api, id)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	objectId := data[info.ThroughField.Api]
		// 	if objectId != nil {
		// 		o.AggregateField(info.TargetField.Parent, objectId.(bson.ObjectId).Hex(), info.TargetField)
		// 	}
		// }
		return
	}
	var result []bson.M
	if info.TargetField.Parent == object {
		// 计算字段在自身
		data, err := o.GetObjectByID(object.Api, id)
		if err != nil {
			panic(err)
		}
		result = append(result, data)
	} else {
		// 存在通过字段肯定是相关表
		c := session.DB("test").C(info.ThroughField.Parent.Api)
		err := c.Find(bson.M{info.ThroughField.Api: bson.ObjectIdHex(id)}).All(&result)
		if err != nil {
			panic(err)
		}
		fmt.Println("find object count", len(result))
	}

	resolverIdentifier := func(ctx context.Context, name string) (interface{}, error) {
		runner := formula.RunnerFromCtx(ctx)
		object := runner.Get("object").(bson.M)
		return formula.FormatValue(object[name])
	}

	resolveSelectorExpression := func(ctx context.Context, name string) (interface{}, error) {
		runner := formula.RunnerFromCtx(ctx)
		object := runner.Get("object").(bson.M)
		// 找到引用对象的id值
		arr := strings.Split(name, ".")
		id, ok := object[arr[0]].(bson.ObjectId)
		if !ok {
			return nil, nil
		}
		// 拿到这个字段
		field, err := findField(o.list, info.TargetField.Parent.Api, arr[0])
		if err != nil {
			return nil, err
		}
		data := field.Data.(RelateData)
		// 取出id值对应的对象
		target, err := o.GetObjectByID(data.ObjectApi, id.Hex())
		if err != nil {
			return nil, err
		}
		return formula.FormatValue(target[arr[1]])
	}

	formatValue := func(field *Field, value interface{}) (interface{}, error) {
		switch field.Type {
		case Int:
			return formula.ToInt(value)
		case Float:
			return formula.ToFloat32(value)
		case Bool:
			return formula.ToBool(value)
		case String:
			return formula.ToString(value)
		default:
			return nil, fmt.Errorf("unknown field type %v", field.Type)
		}
	}

	runner := formula.NewRunner()
	runner.IdentifierResolver = resolverIdentifier
	runner.SelectorExpressionResolver = resolveSelectorExpression
	formulaData := info.TargetField.Data.(*FormulaData)
	for _, item := range result {
		runner.Set("object", item)
		value, err := runner.Resolve(context.Background(), formulaData.SourceCode.Expression)
		if err != nil {
			panic(err)
		}
		formated, err := formatValue(info.TargetField, value)
		if err != nil {
			panic(err)
		}
		_, err = o.UpdateObject(info.TargetField.Parent.Api, item["_id"].(bson.ObjectId).Hex(), bson.M{
			info.TargetField.Api: formated,
		})
		if err != nil {
			panic(err)
		}
	}
}

// 支持聚合自身
func (o *Objectql) AggregateField(object *Object, id string, field *Field) {
	adata := field.Data.(AggregationData)
	if adata.Resolved == nil {
		field, err := findField(o.list, adata.Object, adata.Relate)
		if err != nil {
			panic(err)
		}
		adata.Resolved = field
	}
	// 聚合方法
	funcStr := ""
	switch adata.Kind {
	case Avg:
		funcStr = "$avg"
	case Min:
		funcStr = "$min"
	case Max:
		funcStr = "$max"
	default:
		panic("not support aggregate kind")
	}
	// 聚合查询
	// db.getCollection('student').aggregate([
	// 	{ $match : { teacher: ObjectId("6493bde9229bcc1c50ed5ded") } },
	//   { $group : { _id : "$item", avgSpeed: { $avg : "$age" } } },
	// ])
	var result bson.M
	c := session.DB("test").C(adata.Object)
	err := c.Pipe([]bson.M{
		{
			"$match": bson.M{
				adata.Relate: bson.ObjectIdHex(id),
			},
		},
		{
			"$group": bson.M{
				"_id":    "$item",
				"result": bson.M{funcStr: "$" + adata.Field},
			},
		},
	}).One(&result)
	if err != nil && err != mgo.ErrNotFound {
		panic(err)
	}
	// 应用修改
	var value float64 = 0
	if result != nil {
		value = gconv.Float64(result["result"])
	}
	fmt.Println("aggregate object:", result, value)
	_, err = o.UpdateObject(object.Api, id, bson.M{
		field.Api: value,
	})
	if err != nil {
		panic(err)
	}
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
			Type: toGraphqlType(field.Type),
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
