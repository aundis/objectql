package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aundis/formula"
	"github.com/aundis/graphql"
	"github.com/aundis/graphql/language/ast"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var global *Objectql

func New() *Objectql {
	if global == nil {
		global = &Objectql{
			query:    map[string]*graphql.Field{},
			mutation: map[string]*graphql.Field{},
			objects:  []Object{},
			gobjects: map[string]*graphql.Object{},
			eventMap: gmap.NewAnyAnyMap(true),
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
	// database
	mongoClientOpts        *options.ClientOptions
	mongoClient            *mongo.Client
	mongoCollectionOptions *options.CollectionOptions
	// event
	eventMap *gmap.AnyAnyMap
	// permission
	objectPermissionCheckHandler      ObjectPermissionCheckHandler
	objectFieldPermissionCheckHandler ObjectFieldPermissionCheckHandler
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
			// 展开查询
			if f.Type == Relate {
				fields[f.Api+"__expand"] = &graphql.Field{
					Name: f.Api,
					Type: graphql.String,
				}
			}
		}
		o.gobjects[v.Api] = graphql.NewObject(graphql.ObjectConfig{
			Name:   v.Api,
			Fields: fields,
		})
	}
	for _, v := range list {
		o.FullGraphqlObject(o.gobjects[v.Api], v)
		o.InitObjectGraphqlQuery(v)
		o.InitObjectGraphqlMutation(v)
	}
}

func stringArrayToMongodbSelects(arr []string) bson.M {
	result := bson.M{}
	for _, item := range arr {
		if strings.Contains(item, "__expand") {
			continue
		}
		result[item] = 1
	}
	return result
}

func getSelectMapKeys(v bson.M) []string {
	var result []string
	for k := range v {
		result = append(result, k)
	}
	return result
}

func selectMapToQueryString(v bson.M) string {
	var result []string
	for k := range v {
		result = append(result, k)
	}
	return strings.Join(result, ",")
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

func (o *Objectql) FullGraphqlObject(gobj *graphql.Object, object *Object) {
	gobj.AddFieldConfig("_id", &graphql.Field{
		Type: graphql.String,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			source, _ := p.Source.(bson.M)
			if source != nil && source["_id"] != nil {
				return source["_id"].(primitive.ObjectID).Hex(), nil
			}
			return nil, nil
		},
	})
	for _, field := range object.Fields {
		cur := field
		gobj.AddFieldConfig(cur.Api, &graphql.Field{
			Name: cur.Api,
			Type: o.toGraphqlType(cur, cur.Api),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return o.graphqlFieldResolver(p.Context, p, cur, cur.Api)
			},
			Description: cur.Comment,
		})
		if cur.Type == Relate {
			expandApi := cur.Api + "__expand"
			gobj.AddFieldConfig(expandApi, &graphql.Field{
				Name: expandApi,
				Type: o.toGraphqlType(cur, expandApi),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					return o.graphqlFieldResolver(p.Context, p, cur, expandApi)
				},
				Description: cur.Comment,
			})
		}
	}
}

func (o *Objectql) graphqlFieldResolver(ctx context.Context, p graphql.ResolveParams, field *Field, gapi string) (interface{}, error) {
	source, ok := p.Source.(bson.M)
	if !ok {
		return nil, errors.New("graphqlFieldResolver source not bson.M")
	}
	// 字段权限校验(无权限返回null)
	err := o.checkObjectFieldPermission(ctx, field.Parent.Api, field.Api, FieldQuery)
	if err != nil {
		return nil, nil
	}
	// 格式化输出值 (自己调用需要先写个声明)
	var formatGraohqlOutValue func(fieldType FieldType, value interface{}) (interface{}, error)
	formatGraohqlOutValue = func(fieldType FieldType, value interface{}) (interface{}, error) {
		switch fieldType {
		case Bool:
			return gconv.Bool(value), nil
		case Int:
			return gconv.Int(value), nil
		case Float:
			return gconv.Float32(value), nil
		case String:
			return gconv.String(value), nil
		case Relate:
			if value == nil {
				return nil, nil
			}
			if strings.Contains(gapi, "__expand") {
				objectId := value.(primitive.ObjectID).Hex()
				data := field.Data.(*RelateData)
				return o.relateResolver(ctx, p, data.ObjectApi, objectId)
			} else {
				return value.(primitive.ObjectID).Hex(), nil
			}
		case Formula:
			data := field.Data.(*FormulaData)
			return formatGraohqlOutValue(data.Type, value)
		case Aggregation:
			data := field.Data.(*AggregationData)
			return formatGraohqlOutValue(data.Type, value)
		default:
			return nil, fmt.Errorf("formatGraohqlOutValue simple not support type(%v)", fieldType)
		}
	}

	return formatGraohqlOutValue(field.Type, source[field.Api])
}

func (o *Objectql) relateResolver(ctx context.Context, p graphql.ResolveParams, objectApi string, objectId string) (interface{}, error) {
	selects := getGraphqlSelectFieldNames(p)
	mgoSelects := stringArrayToMongodbSelects(selects)
	results, err := o.mongoFindOne(ctx, objectApi, bson.M{"_id": ObjectIdFromHex(objectId)}, selectMapToQueryString(mgoSelects))
	if err != nil {
		return nil, err
	}
	return results, nil
}

func getGraphqlSelectFieldNames(p graphql.ResolveParams) []string {
	if p.Info.FieldASTs[0].SelectionSet == nil {
		return nil
	}
	var result []string
	for _, selection := range p.Info.FieldASTs[0].SelectionSet.Selections {
		if field, ok := selection.(*ast.Field); ok {
			result = append(result, field.Name.Value)
		}
	}
	return result
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
