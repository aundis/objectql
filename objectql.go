package main

import (
	"bytes"
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

func New() *Objectql {
	return &Objectql{
		gobjects: map[string]*graphql.Object{},
		eventMap: gmap.NewAnyAnyMap(true),
	}
}

type Objectql struct {
	list     []*Object
	gobjects map[string]*graphql.Object
	gschema  graphql.Schema
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

func (o *Objectql) AddObject(object *Object) {
	o.list = append(o.list, object)
}

func (o *Objectql) InitObjects() error {
	// 初始化字段的parent
	o.initFieldParent()
	// 解析字段的引用关系
	err := o.parseFields()
	if err != nil {
		return err
	}
	// 预初始化所有对象
	o.preInitObjects()
	//
	querys := graphql.Fields{}
	mutations := graphql.Fields{}
	for _, v := range o.list {
		// 初始化Graphql对象的字段
		o.fullGraphqlObject(o.gobjects[v.Api], v)
		// 初始化Graphql对象的query
		o.initObjectGraphqlQuery(querys, v)
		// 初始化Graphql对象的mutation
		o.initObjectGraphqlMutation(mutations, v)
	}
	// 初始化Graphql Schema
	o.gschema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name:   "RootQuery",
			Fields: querys,
		}),
		Mutation: graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: mutations,
		}),
	})
	return err
}

func (o *Objectql) initFieldParent() {
	// 为所有的field设定parent
	for _, v := range o.list {
		for _, f := range v.Fields {
			f.Parent = v
		}
	}
}

// 解析公式及其引用关系
func (o *Objectql) parseFields() (err error) {
	for _, object := range o.list {
		// 解析统计和公式字段
		for _, field := range object.Fields {
			if field.Type == Aggregation {
				err = o.parseAggregationField(object, field)
			}
			if field.Type == Formula {
				err = o.parseFormulaField(object, field)
			}
			if err != nil {
				return fmt.Errorf("parse field %s.%s error: %s", object.Api, field.Api, err.Error())
			}
		}
	}
	return nil
}

func (o *Objectql) parseAggregationField(object *Object, field *Field) error {
	adata := field.Data.(*AggregationData)
	// 解析引用的相关表字段
	resolved, err := FindFieldFromName(o.list, adata.Object, adata.Relate)
	if err != nil {
		return err
	}
	adata.resolved = resolved
	// 关联字段(相关表)
	relateField, err := FindFieldFromName(o.list, adata.Object, adata.Relate)
	if err != nil {
		return err
	}
	relateField.relations = append(relateField.relations, &RelationFiledInfo{
		ThroughField: relateField,
		TargetField:  field,
	})
	// 被统计的字段
	beCountedField, err := FindFieldFromName(o.list, adata.Object, adata.Field)
	if err != nil {
		return err
	}
	beCountedField.relations = append(beCountedField.relations, &RelationFiledInfo{
		ThroughField: relateField,
		TargetField:  field,
	})
	return nil
}

func (o *Objectql) parseFormulaField(object *Object, field *Field) error {
	var err error
	fdata := field.Data.(*FormulaData)
	fdata.SourceCode, err = formula.ParseSourceCode([]byte(fdata.Formula))
	if err != nil {
		return err
	}
	names, err := formula.ResolveReferenceFields(fdata.SourceCode)
	if err != nil {
		return err
	}
	// 字段挂载
	for _, name := range names {
		arr := strings.Split(name, ".")
		if len(arr) != 1 && len(arr) != 2 {
			return fmt.Errorf("formual reference name dot len > 2")
		}
		// 找到引用的字段(在本对象找到引用类型的字段)
		relatedField, err := FindFieldFromName(o.list, object.Api, arr[0])
		if err != nil {
			return err
		}

		if len(arr) == 1 {
			relatedField.relations = append(relatedField.relations, &RelationFiledInfo{
				ThroughField: nil,
				TargetField:  field,
			})
		} else {
			if relatedField.Type != Relate {
				return fmt.Errorf("object %s field %s not a relate field", object.Api, arr[0])
			}
			relatedField.relations = append(relatedField.relations, &RelationFiledInfo{
				ThroughField: relatedField,
				TargetField:  field,
			})
			relateData := relatedField.Data.(*RelateData)
			beCountedField, err := FindFieldFromName(o.list, relateData.ObjectApi, arr[1])
			if err != nil {
				return err
			}
			beCountedField.relations = append(beCountedField.relations, &RelationFiledInfo{
				ThroughField: relatedField,
				TargetField:  field,
			})
		}
	}
	return nil
}

// 局部初始化全部的对象,因为后面可能相关表需要相互引用
func (o *Objectql) preInitObjects() {
	for _, object := range o.list {
		// 先填充后面再替换
		fields := graphql.Fields{
			"_id": &graphql.Field{
				Name: "_id",
				Type: graphql.String,
			},
		}
		for _, field := range object.Fields {
			fields[field.Api] = &graphql.Field{
				Name: field.Api,
				Type: graphql.String,
			}
			// 展开查询
			if field.Type == Relate {
				fields[field.Api+"__expand"] = &graphql.Field{
					Name: field.Api,
					Type: graphql.String,
				}
			}
		}
		o.gobjects[object.Api] = graphql.NewObject(graphql.ObjectConfig{
			Name:   object.Api,
			Fields: fields,
		})
	}
}

func selectMapToQueryString(v bson.M) string {
	var result []string
	for k := range v {
		result = append(result, k)
	}
	return strings.Join(result, ",")
}

func (o *Objectql) fullGraphqlObject(gobj *graphql.Object, object *Object) {
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

// 增删改查接口
func (o *Objectql) Insert(ctx context.Context, objectApi string, doc bson.M, opts ...interface{}) (map[string]interface{}, error) {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString("data: " + objectApi + "__insert(")
	buffer.WriteString(" doc:")
	buffer.WriteString(docToGrpahqlArgument(doc))
	buffer.WriteString(")")
	buffer.WriteString(getObjectFieldsQueryString(object))
	buffer.WriteString("}")
	result := graphql.Do(graphql.Params{
		Schema:        o.gschema,
		RequestString: buffer.String(),
		Context:       ctx,
	})
	if len(result.Errors) > 0 {
		return nil, result.Errors[0]
	}
	return result.Data.(map[string]interface{})["data"].(map[string]interface{}), nil
}

func (o *Objectql) Update(ctx context.Context, objectApi string, id string, doc bson.M) (bson.M, error) {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString("data: " + objectApi + "__update(")
	buffer.WriteString(" _id:")
	buffer.WriteString(`"` + id + `"`)
	buffer.WriteString(" doc:")
	buffer.WriteString(docToGrpahqlArgument(doc))
	buffer.WriteString(")")
	buffer.WriteString(getObjectFieldsQueryString(object))
	buffer.WriteString("}")
	result := graphql.Do(graphql.Params{
		Schema:        o.gschema,
		RequestString: buffer.String(),
		Context:       ctx,
	})
	if len(result.Errors) > 0 {
		return nil, result.Errors[0]
	}
	return result.Data.(map[string]interface{})["data"].(map[string]interface{}), nil
}

func (o *Objectql) Delete(ctx context.Context, objectApi string, id string) error {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return fmt.Errorf("not found object '%s'", objectApi)
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString(objectApi + "__delete(")
	buffer.WriteString(" _id:")
	buffer.WriteString(`"` + id + `"`)
	buffer.WriteString(")")
	buffer.WriteString("}")
	result := graphql.Do(graphql.Params{
		Schema:        o.gschema,
		RequestString: buffer.String(),
		Context:       ctx,
	})
	if len(result.Errors) > 0 {
		return result.Errors[0]
	}
	return nil
}

func (o *Objectql) FindAll(ctx context.Context, condition bson.M, opts ...interface{}) ([]bson.M, error) {



	// "filter": &graphql.ArgumentConfig{
	// 	Type: graphql.String,
	// },
	// "top": &graphql.ArgumentConfig{
	// 	Type: graphql.Int,
	// },
	// "skip": &graphql.ArgumentConfig{
	// 	Type: graphql.Int,
	// },
	// "sort": &graphql.ArgumentConfig{
	// 	Type: graphql.NewList(graphql.String),
	// }

	return nil, nil
}

func (o *Objectql) FindOne(ctx context.Context, condition bson.M, id string, fields ...string) (bson.M, error) {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString("data: " + objectApi + "__insert(")
	buffer.WriteString(" doc:")
	buffer.WriteString(docToGrpahqlArgument(doc))
	buffer.WriteString(")")
	buffer.WriteString(getObjectFieldsQueryString(object))
	buffer.WriteString("}")
	result := graphql.Do(graphql.Params{
		Schema:        o.gschema,
		RequestString: buffer.String(),
		Context:       ctx,
	})
	if len(result.Errors) > 0 {
		return nil, result.Errors[0]
	}
	return result.Data.(map[string]interface{})["data"].(map[string]interface{}), nil
}

func (o *Objectql) Aggregate() {}

// Direct 通过context控制
func (o *Objectql) DirectInsert()    {}
func (o *Objectql) DirectUpdate()    {}
func (o *Objectql) DirectDelete()    {}
func (o *Objectql) DirectFind()      {}
func (o *Objectql) DirectFindOne()   {}
func (o *Objectql) DirectAggregate() {}

func docToGrpahqlArgument(doc bson.M) string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for k, v := range doc {
		buffer.WriteString(k)
		buffer.WriteString(":")

		switch n := v.(type) {
		case string:
			buffer.WriteString(`"`)
			buffer.WriteString(escapeString(n))
			buffer.WriteString(`"`)
		default:
			buffer.WriteString(escapeString(gconv.String(n)))
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}

func getObjectFieldsQueryString(object *Object) string {
	var result []string
	for _, field := range object.Fields {
		if strings.Contains(field.Api, "__") {
			continue
		}
		result = append(result, field.Api)
	}
	return fieldNamesToQueryString(result)
}

func fieldNamesToQueryString(arr []string) string {
	return "{" + strings.Join(arr, ",") + "}"
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, `"`, `\"`)
}
