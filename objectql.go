package objectql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aundis/formula"
	"github.com/aundis/graphql"
	"github.com/aundis/graphql/language/ast"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/text/gstr"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var blockEventsKey = "objectql_blockEventsKey"

type ObjectqlOptiosn struct {
	OperatorObject string
	GetOperator    func(ctx context.Context) (any, error)
}

func New(optinos ...ObjectqlOptiosn) *Objectql {
	option := ObjectqlOptiosn{}
	if len(optinos) > 0 {
		option = optinos[0]
	}
	return &Objectql{
		gobjects:     gmap.NewStrAnyMap(true),
		eventMap:     gmap.NewAnyAnyMap(true),
		gstructTypes: gmap.NewStrAnyMap(true),
		// owner
		operatorObject: option.OperatorObject,
		getOperator:    option.GetOperator,
	}
}

type Objectql struct {
	list       []*Object
	objectMap  *gmap.StrAnyMap
	gobjects   *gmap.StrAnyMap
	gschema    graphql.Schema
	gquerys    graphql.Fields
	gmutations graphql.Fields
	// database
	mongoClientOpts        *options.ClientOptions
	mongoClient            *mongo.Client
	mongoCollectionOptions *options.CollectionOptions
	// event
	eventMap *gmap.AnyAnyMap
	// permission
	objectPermissionCheckHandler       ObjectPermissionCheckHandler
	objectFieldPermissionCheckHandler  ObjectFieldPermissionCheckHandler
	objectHandlePermissionCheckHandler ObjectHandlePermissionCheckHandler
	// struct types
	gstructTypes *gmap.StrAnyMap
	// owner
	operatorObject string
	getOperator    func(ctx context.Context) (any, error)
}

func (o *Objectql) AddObject(object *Object) {
	// 添加一些固有的字段
	// 对象ID
	object.Fields = append([]*Field{{
		Type:    ObjectID,
		Name:    "对象ID",
		Api:     "_id",
		Comment: "对象唯一标识",
	}}, object.Fields...)
	// 拥有者
	if len(o.operatorObject) > 0 {
		object.Fields = append(object.Fields, &Field{
			Type: NewRelate(o.operatorObject),
			Name: "拥有者",
			Api:  "owner",
		})
	}
	// 添加一些关联对象 __expand __expands
	var expands []*Field
	for _, field := range object.Fields {
		switch n := field.Type.(type) {
		case *RelateType:
			expands = append(expands, &Field{
				Api:      field.Api + "__expand",
				valueApi: field.Api,
				Type: &ExpandType{
					ObjectApi: n.ObjectApi,
					FieldApi:  field.Api,
				},
			})
		case *ArrayType:
			if IsRelateType(n.Type) {
				tpe := n.Type.(*RelateType)
				expands = append(expands, &Field{
					Api:      field.Api + "__expands",
					valueApi: field.Api,
					Type: &ExpandsType{
						ObjectApi: tpe.ObjectApi,
						FieldApi:  field.Api,
					},
				})
			}
		}
	}
	object.Fields = append(object.Fields, expands...)
	// 创建时间
	object.Fields = append(object.Fields, &Field{
		Type: DateTime,
		Name: "创建时间",
		Api:  "createTime",
	})
	// 修改时间
	object.Fields = append(object.Fields, &Field{
		Type: DateTime,
		Name: "修改时间",
		Api:  "updateTime",
	})
	o.list = append(o.list, object)
}

func (o *Objectql) GetObject(api string) *Object {
	if o.objectMap == nil {
		o.objectMap = gmap.NewStrAnyMap(true)
		for _, item := range o.list {
			o.objectMap.Set(item.Api, item)
		}
	}
	if v := o.objectMap.Get(api); v != nil {
		return v.(*Object)
	}
	return nil
}

func (o *Objectql) MustGetObject(api string) (*Object, error) {
	r := o.GetObject(api)
	if r == nil {
		return nil, fmt.Errorf("not found object '%s'", api)
	}
	return r, nil
}

func (o *Objectql) InitObjects(ctx context.Context) error {
	// 初始化字段的parent
	o.initFieldParent()
	// 解析字段的引用关系
	err := o.parseFields()
	if err != nil {
		return err
	}
	// 预初始化所有对象
	o.preInitObjects()
	o.gquerys = graphql.Fields{}
	o.gmutations = graphql.Fields{}
	for _, v := range o.list {
		// 初始化绑定对对象
		err = o.bindObjectMethod(v, v.Bind)
		if err != nil {
			return err
		}
		// 初始化Graphql对象的字段
		err = o.fullGraphqlObject(o.getGraphqlObject(v.Api), v)
		if err != nil {
			return err
		}
		// 初始化Graphql对象的query
		err = o.initObjectGraphqlQuery(ctx, o.gquerys, v)
		if err != nil {
			return err
		}
		// 初始化Graphql对象的mutation
		err = o.initObjectGraphqlMutation(ctx, o.gmutations, v)
		if err != nil {
			return err
		}
	}
	// 初始化Graphql Schema
	o.gschema, err = graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name:   "RootQuery",
			Fields: o.gquerys,
		}),
		Mutation: graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: o.gmutations,
		}),
	})
	return err
}

func (o *Objectql) isMutationHandle(object string, name string) bool {
	return o.gmutations[object+"__"+name] != nil
}

func (o *Objectql) isQueryHandle(object string, name string) bool {
	return o.gquerys[object+"__"+name] != nil
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
		for _, field := range object.Fields {
			// 解析字段必填
			err = o.parseFieldRequire(object, field)
			if err != nil {
				return err
			}
			// 解析字段数据校验
			err = o.parseFieldValidate(object, field)
			if err != nil {
				return err
			}
			// 解析统计和公式字段
			switch field.Type.(type) {
			case *AggregationType:
				err = o.parseAggregationField(object, field)
			case *FormulaType:
				err = o.parseFormulaField(object, field)
			}
			if err != nil {
				return fmt.Errorf("parse field %s.%s error: %s", object.Api, field.Api, err.Error())
			}
		}
	}
	return nil
}

func (o *Objectql) parseFieldRequire(object *Object, field *Field) error {
	if field.Require != nil {
		switch n := field.Require.(type) {
		case bool:
			// ...
		case string:
			souceCode, err := formula.ParseSourceCode([]byte(n + " ? true : false"))
			if err != nil {
				return err
			}
			field.requireSourceCode = souceCode
			fields, err := formula.ResolveReferenceFields(souceCode)
			if err != nil {
				return err
			}
			// 同时要把自己加进去
			field.requireSourceCodeFields = append(fields, field.Api)
		case *FieldReqireCheckHandle:
			n.Fields = append(n.Fields, field.Api)
		default:
			return fmt.Errorf("%s.%s require value error: field require juse support string or *FieldReqireCheckHandle", object.Api, field.Api)
		}
	}
	return nil
}

func (o *Objectql) parseFieldValidate(object *Object, field *Field) error {
	if field.Validate != nil {
		switch n := field.Validate.(type) {
		case string:
			souceCode, err := formula.ParseSourceCode([]byte(n + " ? true : false"))
			if err != nil {
				return err
			}
			field.validateSourceCode = souceCode
			fields, err := formula.ResolveReferenceFields(souceCode)
			if err != nil {
				return err
			}
			// 同时要把自己加进去
			field.validateSourceCodeFields = append(fields, field.Api)
		case *FieldValidateHandle:
			n.Fields = append(n.Fields, field.Api)
		default:
			return fmt.Errorf("%s.%s validate value error: field validate value juse support string or *FieldValidateHandle", object.Api, field.Api)
		}
	}
	return nil
}

func (o *Objectql) parseAggregationField(object *Object, field *Field) error {
	adata := field.Type.(*AggregationType)
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
	relateField.relations = append(relateField.relations, &relationFiledInfo{
		ThroughField: relateField,
		TargetField:  field,
	})
	// 被统计的字段
	beCountedField, err := FindFieldFromName(o.list, adata.Object, adata.Field)
	if err != nil {
		return err
	}
	beCountedField.relations = append(beCountedField.relations, &relationFiledInfo{
		ThroughField: relateField,
		TargetField:  field,
	})
	return nil
}

func (o *Objectql) parseFormulaField(object *Object, field *Field) error {
	var err error
	fdata := field.Type.(*FormulaType)
	fdata.sourceCode, err = formula.ParseSourceCode([]byte(fdata.Formula))
	if err != nil {
		return err
	}
	names, err := formula.ResolveReferenceFields(fdata.sourceCode)
	if err != nil {
		return err
	}
	fdata.referenceFields = names
	// 字段挂载
	for _, name := range names {
		arr := strings.Split(name, ".")
		if len(arr) != 1 && len(arr) != 2 {
			return fmt.Errorf("formual reference name dot len > 2")
		}
		// 找到引用的字段(在本对象找到引用类型的字段)
		relatedField, err := FindFieldFromName(o.list, object.Api, removeFieldSuffix(arr[0]))
		if err != nil {
			return err
		}

		if len(arr) == 1 {
			relatedField.relations = append(relatedField.relations, &relationFiledInfo{
				ThroughField: nil,
				TargetField:  field,
			})
		} else {
			relate, ok := relatedField.Type.(*RelateType)
			if !ok {
				return fmt.Errorf("object %s field %s not a relate field", object.Api, arr[0])
			}
			relatedField.relations = append(relatedField.relations, &relationFiledInfo{
				ThroughField: relatedField,
				TargetField:  field,
			})
			beCountedField, err := FindFieldFromName(o.list, relate.ObjectApi, arr[1])
			if err != nil {
				return err
			}
			beCountedField.relations = append(beCountedField.relations, &relationFiledInfo{
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
			if IsRelateType(field.Type) {
				fields[field.Api+"__expand"] = &graphql.Field{
					Name: field.Api,
					Type: graphql.String,
				}
			}
		}
		o.gobjects.Set(object.Api, graphql.NewObject(graphql.ObjectConfig{
			Name:   object.Api,
			Fields: fields,
		}))
	}
}

func selectMapToQueryString(v bson.M) string {
	var result []string
	for k := range v {
		result = append(result, k)
	}
	return strings.Join(result, ",")
}

func (o *Objectql) fullGraphqlObject(gobj *graphql.Object, object *Object) error {
	for _, field := range object.Fields {
		cur := field
		tpe := o.getGraphqlFieldType(cur.Type)
		if isNull(tpe) {
			return fmt.Errorf("can't resolve field '%s.%s' type", object.Api, cur.Api)
		}
		gobj.AddFieldConfig(cur.Api, &graphql.Field{
			Name: cur.Api,
			Type: tpe,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return o.graphqlFieldResolver(p.Context, p, cur)
			},
			Description: cur.Name + ":" + cur.Comment,
		})
	}
	return nil
}

var graphqlResolveParamsKey = &struct{}{}

func (o *Objectql) graphqlFieldResolver(ctx context.Context, p graphql.ResolveParams, field *Field) (interface{}, error) {
	source, ok := p.Source.(M)
	if !ok {
		return nil, fmt.Errorf("graphqlFieldResolver source not map[string]interface{} got %T", p.Source)
	}
	// 字段权限校验(无权限返回null)
	if field.Api != "_id" {
		has, err := o.hasObjectFieldPermission(ctx, field.Parent.Api, field.Api, FieldQuery)
		if err != nil {
			return nil, err
		}
		if !has {
			return nil, nil
		}
	}
	return source[field.Api], nil

	// // 格式化输出值
	// ctx = context.WithValue(ctx, graphqlResolveParamsKey, p)
	// valueApi := field.valueApi
	// if len(valueApi) == 0 {
	// 	valueApi = field.Api
	// }
	// return o.fieldResolver(ctx, field.Type, source[valueApi])
}

func (o *Objectql) fieldResolver(ctx context.Context, fieldType Type, value interface{}) (interface{}, error) {
	if isNull(value) {
		return nil, nil
	}
	switch n := fieldType.(type) {
	case *ObjectIDType:
		return value.(primitive.ObjectID).Hex(), nil
	case *BoolType:
		return boolOrNil(value), nil
	case *IntType:
		return intOrNil(value), nil
	case *FloatType:
		return floatOrNil(value), nil
	case *StringType:
		return stringOrNil(value), nil
	case *DateTimeType, *DateType, *TimeType:
		return dateTimeOrNil(value), nil
	case *RelateType:
		return value.(primitive.ObjectID).Hex(), nil
	case *ExpandType:
		if objectId, ok := value.(primitive.ObjectID); ok {
			return o.expandFieldResolver(ctx, n.ObjectApi, objectId.Hex())
		}
		return nil, nil
	case *ExpandsType:
		objectIds := gconv.Interfaces(value)
		if len(objectIds) > 0 {
			return o.expandsFieldResolver(ctx, n.ObjectApi, convInterfaces2ObjectIds(objectIds))
		}
		return nil, nil
	case *FormulaType:
		return o.fieldResolver(ctx, n.Type, value)
	case *AggregationType:
		return o.fieldResolver(ctx, n.Type, value)
	case *ArrayType:
		return o.arrayFieldResolver(ctx, n, value)
	default:
		return nil, fmt.Errorf("fieldResolver not support type(%v)", fieldType)
	}
}

func convInterfaces2ObjectIds(arr []interface{}) []primitive.ObjectID {
	var result []primitive.ObjectID
	for _, item := range arr {
		if v, ok := item.(primitive.ObjectID); ok {
			result = append(result, v)
		}
	}
	return result
}

func (o *Objectql) arrayFieldResolver(ctx context.Context, tpe *ArrayType, value interface{}) (interface{}, error) {
	sourceValue := reflect.ValueOf(value)
	if sourceValue.Type() != nil && sourceValue.Type().Kind() != reflect.Array && sourceValue.Type().Kind() != reflect.Slice {
		return nil, fmt.Errorf("arrayFieldResolver can't conv type %T to array", value)
	}
	sliceValue := reflect.MakeSlice(reflect.TypeOf([]any{}), 0, 0)
	for i := 0; i < sourceValue.Len(); i++ {
		evalue, err := o.fieldResolver(ctx, tpe.Type, sourceValue.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		sliceValue = reflect.Append(sliceValue, reflect.ValueOf(evalue))
	}
	return sliceValue.Interface(), nil
}

func (o *Objectql) expandFieldResolver(ctx context.Context, objectApi string, objectId string) (interface{}, error) {
	p := ctx.Value(graphqlResolveParamsKey).(graphql.ResolveParams)
	selects := getGraphqlSelectFieldNames(p)
	mgoSelects := stringArrayToMongodbSelects(selects)
	results, err := o.mongoFindOne(ctx, objectApi, bson.M{"_id": ObjectIdFromHex(objectId)}, selectMapToQueryString(mgoSelects))
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (o *Objectql) expandsFieldResolver(ctx context.Context, objectApi string, objectIds []primitive.ObjectID) (interface{}, error) {
	p := ctx.Value(graphqlResolveParamsKey).(graphql.ResolveParams)
	selects := getGraphqlSelectFieldNames(p)
	// 确保_id加入到查询当中, 排序需要用到
	if lo.IndexOf(selects, "_id") == -1 {
		selects = append(selects, "_id")
	}
	mgoSelects := stringArrayToMongodbSelects(selects)
	results, err := o.mongoFindAll(ctx, objectApi, bson.M{"_id": bson.M{"$in": objectIds}}, selectMapToQueryString(mgoSelects))
	if err != nil {
		return nil, err
	}
	// 这里要进行排序后再进行返回
	return sortByObjectIDs(results, objectIds), nil
}

func sortByObjectIDs(data []bson.M, order []primitive.ObjectID) []bson.M {
	// 使用sort.Slice函数根据自定义比较函数对data进行排序
	sort.Slice(data, func(i, j int) bool {
		// 获取data中元素的_id字段值
		idI := data[i]["_id"].(primitive.ObjectID)
		idJ := data[j]["_id"].(primitive.ObjectID)

		// 获取_id字段值在order数组中的索引
		indexI := lo.IndexOf(order, idI)
		indexJ := lo.IndexOf(order, idJ)

		// 比较索引，返回排序结果
		return indexI < indexJ
	})

	return data
}

// 如果是 user__expand 会将 user 添加进去
func stringArrayToMongodbSelects(arr []string) bson.M {
	result := bson.M{}
	for _, item := range arr {
		if strings.Contains(item, "__expand") {
			result[strings.ReplaceAll(item, "__expand", "")] = 1
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
			fieldName := field.Name.Value
			if gstr.HasSuffix(fieldName, "__expand") {
				result = append(result, gstr.Replace(fieldName, "__expand", ""))
			}
			if gstr.HasSuffix(fieldName, "__expands") {
				result = append(result, gstr.Replace(fieldName, "__expands", ""))
			}
			result = append(result, field.Name.Value)
		}
	}
	return result
}

func (o *Objectql) Do(ctx context.Context, request string) *graphql.Result {
	return graphql.Do(graphql.Params{
		Schema:        o.gschema,
		RequestString: request,
		Context:       ctx,
	})
}

// 调用用户定义的query和mutation
func (o *Objectql) Call(ctx context.Context, objectApi string, method string, param map[string]any, fields ...[]string) (*Var, error) {
	if o.isMutationHandle(objectApi, method) {
		return o.Mutation(ctx, objectApi, method, param, fields...)
	}
	if o.isQueryHandle(objectApi, method) {
		return o.Query(ctx, objectApi, method, param, fields...)
	}
	return nil, fmt.Errorf("object '%s' not found query or mutation '%s'", objectApi, method)
}

func (o *Objectql) Query(ctx context.Context, objectApi string, method string, param map[string]any, fields ...[]string) (*Var, error) {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	fullName := objectApi + "__" + method
	gquery := o.gquerys[fullName]
	if gquery == nil {
		return nil, fmt.Errorf("not found object '%s' query '%s'", objectApi, method)
	}
	var buffer bytes.Buffer
	buffer.WriteString("{")
	buffer.WriteString("data: " + fullName)
	if len(param) > 0 {
		buffer.WriteString("(")
		text, err := o.mapToGrpahqlFormat(param)
		if err != nil {
			return nil, err
		}
		text = strings.Trim(text, "{")
		text = strings.Trim(text, "}")
		buffer.WriteString(text)
		buffer.WriteString(")")
	}
	if len(fields) > 0 && len(fields[0]) > 0 {
		buffer.WriteString("{")
		buffer.WriteString(strings.Join(fields[0], ","))
		buffer.WriteString("}")
	} else {
		writeGraphqlOutputFieldQueryString(&buffer, gquery.Type)
	}
	//
	buffer.WriteString("}")
	result := o.Do(ctx, buffer.String())
	if len(result.Errors) > 0 {
		return nil, result.Errors[0]
	}
	return NewVar(result.Data.(map[string]interface{})["data"]), nil
}

func (o *Objectql) Mutation(ctx context.Context, objectApi string, method string, param map[string]any, fields ...[]string) (*Var, error) {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	fullName := objectApi + "__" + method
	gmutation := o.gmutations[fullName]
	if gmutation == nil {
		return nil, fmt.Errorf("not found object '%s' mutation '%s'", objectApi, method)
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString("data: " + fullName)
	if len(param) > 0 {
		buffer.WriteString("(")
		text, err := o.mapToGrpahqlFormat(param)
		if err != nil {
			return nil, err
		}
		text = strings.Trim(text, "{")
		text = strings.Trim(text, "}")
		buffer.WriteString(text)
		buffer.WriteString(")")
	}
	if len(fields) > 0 && len(fields[0]) > 0 {
		buffer.WriteString("{")
		buffer.WriteString(strings.Join(fields[0], ","))
		buffer.WriteString("}")
	} else {
		writeGraphqlOutputFieldQueryString(&buffer, gmutation.Type)
	}
	//
	buffer.WriteString("}")
	return getVarFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func writeGraphqlOutputFieldQueryString(buffer *bytes.Buffer, gtype graphql.Output) {
	switch n := gtype.(type) {
	case *graphql.List:
		writeGraphqlOutputFieldQueryString(buffer, n.OfType)
	case *graphql.Object:
		buffer.WriteString("{")
		for name, fd := range n.Fields() {
			buffer.WriteString(name)
			buffer.WriteString(" ")
			writeGraphqlOutputFieldQueryString(buffer, fd.Type)
		}
		buffer.WriteString("}")
	}
}

// 增删改查接口
func (o *Objectql) Insert(ctx context.Context, objectApi string, options InsertOptions) (*Var, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString("data: " + objectApi + "__insert(")
	buffer.WriteString(" doc:")
	text, err := o.docToGrpahqlArgumentText(objectApi, options.Doc)
	if err != nil {
		return nil, err
	}
	buffer.WriteString(text)
	buffer.WriteString(")")
	buffer.WriteString("{")
	writeObjectQueyrFields(&buffer, object, options.Fields)
	buffer.WriteString("}")
	buffer.WriteString("}")
	return getVarFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) Update(ctx context.Context, objectApi string, options UpdateOptions) ([]*Var, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	if len(options.Filter) == 0 {
		return nil, errors.New("filter can't empty")
	}
	filterStr, err := valueToJsonString(options.Filter)
	if err != nil {
		return nil, err
	}
	docStr, err := o.docToGrpahqlArgumentText(objectApi, options.Doc)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString("data: " + objectApi + "__update(")
	buffer.WriteString(" filter:")
	buffer.WriteString(`"`)
	buffer.WriteString(escapeString(filterStr))
	buffer.WriteString(`"`)
	buffer.WriteString(" doc:")
	buffer.WriteString(docStr)
	buffer.WriteString(")")
	buffer.WriteString("{")
	writeObjectQueyrFields(&buffer, object, options.Fields)
	buffer.WriteString("}")
	buffer.WriteString("}")
	return getVarsFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) UpdateById(ctx context.Context, objectApi string, options UpdateByIdOptions) (*Var, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil, fmt.Errorf("not found object '%s'", objectApi)
	}
	docStr, err := o.docToGrpahqlArgumentText(objectApi, options.Doc)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString("data: " + objectApi + "__updateById(")
	buffer.WriteString(" _id:")
	buffer.WriteString(`"` + options.ID + `"`)
	buffer.WriteString(" doc:")
	buffer.WriteString(docStr)
	buffer.WriteString(")")
	buffer.WriteString("{")
	writeObjectQueyrFields(&buffer, object, options.Fields)
	buffer.WriteString("}")
	buffer.WriteString("}")
	return getVarFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) Delete(ctx context.Context, objectApi string, options DeleteOptions) error {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return fmt.Errorf("not found object '%s'", objectApi)
	}
	if len(options.Filter) == 0 {
		return errors.New("filter can't empty")
	}
	filterStr, err := valueToJsonString(options.Filter)
	if err != nil {
		return err
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString(objectApi + "__delete")
	buffer.WriteString("(filter:")
	buffer.WriteString(`"`)
	buffer.WriteString(escapeString(filterStr))
	buffer.WriteString(`"`)
	buffer.WriteString(")")
	buffer.WriteString("}")
	return getErrorFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) DeleteById(ctx context.Context, objectApi string, options DeleteByIdOptions) error {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object, err := o.MustGetObject(objectApi)
	if err != nil {
		return err
	}
	if len(options.ID) == 0 {
		return errors.New("id can't empty")
	}
	var buffer bytes.Buffer
	buffer.WriteString("mutation {")
	buffer.WriteString(object.Api + "__deleteById(")
	buffer.WriteString(" _id:")
	buffer.WriteString(`"` + options.ID + `"`)
	buffer.WriteString(")")
	buffer.WriteString("}")
	return getErrorFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) FindList(ctx context.Context, objectApi string, options FindListOptions) ([]*Var, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object, err := o.MustGetObject(objectApi)
	if err != nil {
		return nil, err
	}
	var filterStr string
	if len(options.Filter) > 0 {
		filterStr, err = valueToJsonString(options.Filter)
		if err != nil {
			return nil, err
		}
	}
	// filters
	var buffer bytes.Buffer
	buffer.WriteString("query {")
	buffer.WriteString("data: " + objectApi + "__findList")
	if len(filterStr) > 0 || options.Skip != 0 || options.Top != 0 || len(options.Sort) > 0 {
		buffer.WriteString("(")
		if len(filterStr) > 0 {
			buffer.WriteString(" filter:")
			buffer.WriteString(`"`)
			buffer.WriteString(escapeString(filterStr))
			buffer.WriteString(`"`)
		}
		if options.Skip != 0 {
			buffer.WriteString(" skip:")
			buffer.WriteString(gconv.String(options.Skip))
		}
		if options.Top != 0 {
			buffer.WriteString(" top:")
			buffer.WriteString(gconv.String(options.Top))
		}
		if len(options.Sort) > 0 {
			buffer.WriteString(" sort:")
			buffer.WriteString(stringsToGraphqlQuery(options.Sort))
		}
		buffer.WriteString(")")
	}
	buffer.WriteString("{")
	writeObjectQueyrFields(&buffer, object, options.Fields)
	buffer.WriteString("}")
	buffer.WriteString("}")
	return getVarsFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) FindOneById(ctx context.Context, objectApi string, options FindOneByIdOptions) (*Var, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object, err := o.MustGetObject(objectApi)
	if err != nil {
		return nil, err
	}
	if len(options.ID) == 0 {
		return nil, errors.New("id can't empty")
	}
	var buffer bytes.Buffer
	buffer.WriteString("query {")
	buffer.WriteString("data: " + objectApi + "__findOneById(")
	buffer.WriteString(" id:")
	buffer.WriteString(`"`)
	buffer.WriteString(escapeString(options.ID))
	buffer.WriteString(`"`)
	buffer.WriteString(")")
	buffer.WriteString("{")
	writeObjectQueyrFields(&buffer, object, options.Fields)
	buffer.WriteString("}")
	buffer.WriteString("}")
	return getVarFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) FindOne(ctx context.Context, objectApi string, options FindOneOptions) (*Var, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object, err := o.MustGetObject(objectApi)
	if err != nil {
		return nil, err
	}
	if len(options.Filter) == 0 {
		return nil, errors.New("filter can't empty")
	}
	filterStr, err := valueToJsonString(options.Filter)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	buffer.WriteString("query {")
	buffer.WriteString("data: " + objectApi + "__findOne(")
	buffer.WriteString(" filter:")
	buffer.WriteString(`"`)
	buffer.WriteString(escapeString(filterStr))
	buffer.WriteString(`"`)
	if len(options.Sort) > 0 {
		buffer.WriteString(" sort:")
		buffer.WriteString(stringsToGraphqlQuery(options.Sort))
	}
	buffer.WriteString(")")
	buffer.WriteString("{")
	writeObjectQueyrFields(&buffer, object, options.Fields)
	buffer.WriteString("}")
	buffer.WriteString("}")
	return getVarFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func writeObjectQueyrFields(buffer *bytes.Buffer, object *Object, fields []string) {
	if len(fields) > 0 {
		buffer.WriteString(strings.Join(gconv.Strings(fields), ","))
	} else {
		buffer.WriteString(getObjectFieldsQueryString(object))
	}
}

func (o *Objectql) Count(ctx context.Context, objectApi string, options CountOptions) (int64, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object, err := o.MustGetObject(objectApi)
	if err != nil {
		return 0, err
	}
	var filterStr string
	if len(options.Filter) > 0 {
		filterStr, err = valueToJsonString(options.Filter)
		if err != nil {
			return 0, err
		}
	}
	// filters
	var buffer bytes.Buffer
	buffer.WriteString("query {")
	buffer.WriteString("data: " + object.Api + "__count")
	if len(filterStr) > 0 {
		buffer.WriteString("(filter:")
		buffer.WriteString(`"`)
		buffer.WriteString(escapeString(filterStr))
		buffer.WriteString(`")`)
	}
	buffer.WriteString("}")
	return getInt64FromGraphqlResult(o.Do(ctx, buffer.String()))
}

func (o *Objectql) Aggregate(ctx context.Context, objectApi string, options AggregateOptions) ([]*Var, error) {
	ctx = context.WithValue(ctx, blockEventsKey, options.Direct)
	object, err := o.MustGetObject(objectApi)
	if err != nil {
		return nil, err
	}
	if len(options.Pipeline) == 0 {
		return nil, errors.New("aggregate pipeline can't empty")
	}
	piplineStr, err := valueToJsonString(options.Pipeline)
	if err != nil {
		return nil, err
	}
	// filters
	var buffer bytes.Buffer
	buffer.WriteString("query {")
	buffer.WriteString("data: " + object.Api + "__aggregate(pipeline:")
	buffer.WriteString(`"`)
	buffer.WriteString(escapeString(piplineStr))
	buffer.WriteString(`"`)
	buffer.WriteString(")")
	buffer.WriteString("}")
	return getVarsFromGraphqlResult(o.Do(ctx, buffer.String()))
}

func getVarsFromGraphqlResult(gr *graphql.Result) ([]*Var, error) {
	err := getErrorFromGraphqlResult(gr)
	if err != nil {
		return nil, err
	}

	data := gr.Data.(map[string]interface{})["data"]
	var list []*Var
	if v1, ok := data.([]interface{}); ok {
		for _, v := range v1 {
			if v2, ok := v.(map[string]interface{}); ok {
				list = append(list, NewVar(v2))
			}
		}
	}
	return list, nil
}

func getVarFromGraphqlResult(gr *graphql.Result) (*Var, error) {
	err := getErrorFromGraphqlResult(gr)
	if err != nil {
		return nil, err
	}

	data := gr.Data.(map[string]interface{})["data"]
	if isNull(data) {
		return nil, nil
	}
	return NewVar(data), nil
}

func getInt64FromGraphqlResult(gr *graphql.Result) (int64, error) {
	err := getErrorFromGraphqlResult(gr)
	if err != nil {
		return 0, err
	}

	data := gr.Data.(map[string]interface{})["data"]
	if data == nil {
		return 0, nil
	}
	return gconv.Int64(data), nil
}

func getErrorFromGraphqlResult(gr *graphql.Result) error {
	if len(gr.Errors) > 0 {
		return gr.Errors[0]
	}
	return nil
}

func valueToJsonString(v interface{}) (string, error) {
	jsn, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(jsn), nil
}

func (o *Objectql) mapToGrpahqlFormat(doc map[string]any) (string, error) {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for k, v := range doc {
		buffer.WriteString(k)
		buffer.WriteString(":")
		err := writeGraphqlArgumentValue(&buffer, v)
		if err != nil {
			return "", err
		}
		buffer.WriteString(" ")
	}
	buffer.WriteString("}")
	return buffer.String(), nil
}

func (o *Objectql) docToGrpahqlArgumentText(objectApi string, doc map[string]any) (string, error) {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return "", fmt.Errorf("can't found object '%s'", objectApi)
	}
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for k, v := range doc {
		field := FindFieldFromObject(object, k)
		if field == nil {
			return "", fmt.Errorf("can't found field '%s' from object '%s'", k, objectApi)
		}

		buffer.WriteString(k)
		buffer.WriteString(":")

		err := writeGraphqlArgumentValue(&buffer, v)
		if err != nil {
			return "", err
		}
		buffer.WriteString(` `) // 加个空格
	}
	buffer.WriteString("}")
	return buffer.String(), nil
}

func writeGraphqlArgumentValue(buffer *bytes.Buffer, value interface{}) error {
	if isNull(value) {
		buffer.WriteString(`null`)
		return nil
	}
	switch n := value.(type) {
	case string:
		buffer.WriteString(`"`)
		buffer.WriteString(escapeString(n))
		buffer.WriteString(`"`)
		return nil
	// case *string:
	// 	buffer.WriteString(`"`)
	// 	buffer.WriteString(escapeString(*n))
	// 	buffer.WriteString(`"`)
	// 	return nil
	case time.Time:
		buffer.WriteString(`"`)
		buffer.WriteString(n.Format(time.RFC3339))
		buffer.WriteString(`"`)
		return nil
	// case *time.Time:
	// 	buffer.WriteString(`"`)
	// 	buffer.WriteString(n.Format(time.RFC3339))
	// 	buffer.WriteString(`"`)
	// 	return nil
	case gtime.Time:
		buffer.WriteString(`"`)
		buffer.WriteString(n.Layout(time.RFC3339))
		buffer.WriteString(`"`)
		return nil
	// case *gtime.Time:
	// 	buffer.WriteString(`"`)
	// 	buffer.WriteString(n.Layout(time.RFC3339))
	// 	buffer.WriteString(`"`)
	// 	return nil
	case nil:
		buffer.WriteString(`null`)
		return nil
	default:
		sourceValue := reflect.ValueOf(value)
		if sourceValue.Type().Kind() == reflect.Array || sourceValue.Type().Kind() == reflect.Slice {
			buffer.WriteString("[")
			for i := 0; i < sourceValue.Len(); i++ {
				err := writeGraphqlArgumentValue(buffer, sourceValue.Index(i).Interface())
				if err != nil {
					return err
				}
				if i < sourceValue.Len()-1 {
					buffer.WriteString(",")
				}
			}
			buffer.WriteString("]")
			return nil
		}
		if sourceValue.Type().Kind() == reflect.Pointer {
			err := writeGraphqlArgumentValue(buffer, sourceValue.Elem().Interface())
			if err != nil {
				return err
			}
			return nil
		}
		if sourceValue.Type().Kind() == reflect.Map {
			buffer.WriteString(`"`)
			buffer.WriteString(escapeString(gconv.String(n)))
			buffer.WriteString(`"`)
			return nil
		}
		buffer.WriteString(escapeString(gconv.String(n)))
	}
	return nil
}

func getObjectFieldsQueryString(object *Object) string {
	var result []string
	for _, field := range object.Fields {
		if strings.Contains(field.Api, "__") {
			continue
		}
		result = append(result, field.Api)
	}
	return strings.Join(result, ",")
}

func escapeString(s string) string {
	res := strconv.Quote(s)
	return strings.Trim(res, `"`)
}

func stringsToGraphqlQuery(arr []string) string {
	var list []string
	for _, v := range arr {
		list = append(list, `"`+v+`"`)
	}
	return "[" + strings.Join(list, ",") + "]"
}

func (o *Objectql) GetObjectInfo(objectApi string) *ObjectInfo {
	object := FindObjectFromList(o.list, objectApi)
	if object == nil {
		return nil
	}
	info := &ObjectInfo{
		Name: object.Name,
		Api:  object.Api,
	}
	for _, field := range object.Fields {
		fapi := field.Api
		if fapi == "_id" {
			continue
		}
		if gstr.HasSuffix(fapi, "__expand") {
			continue
		}
		if gstr.HasSuffix(fapi, "__expands") {
			continue
		}
		info.Fields = append(info.Fields, FieldInfo{
			Name: field.Name,
			Api:  field.Api,
		})
	}
	for _, query := range object.Querys {
		info.Querys = append(info.Querys, HandleInfo{
			Name: query.Name,
			Api:  query.Api,
		})
	}
	for _, mutation := range object.Mutations {
		info.Mutations = append(info.Mutations, HandleInfo{
			Name: mutation.Name,
			Api:  mutation.Api,
		})
	}
	return info
}

func (o *Objectql) GetObjectInfos() []*ObjectInfo {
	var result []*ObjectInfo
	for _, object := range o.list {
		result = append(result, o.GetObjectInfo(object.Api))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Api < result[j].Api
	})
	return result
}
