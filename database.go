package objectql

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const MogSessionKey = "mgo_session"

func (o *Objectql) InitMongodb(ctx context.Context, uri string) (err error) {
	o.mongoClientOpts = options.Client().ApplyURI(uri)
	o.mongoClient, err = mongo.Connect(ctx, o.mongoClientOpts)
	if err != nil {
		return
	}
	o.mongoCollectionOptions = options.Collection().SetWriteConcern(writeconcern.Majority())
	return
}

func (o *Objectql) getCollection(api string) *mongo.Collection {
	return o.mongoClient.Database("test").Collection(api)
}

func (o *Objectql) WithTransaction(ctx context.Context, fn func(ctx context.Context) (interface{}, error)) (interface{}, error) {
	if mongo.SessionFromContext(ctx) != nil {
		return fn(ctx)
	} else {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		// SUPPORT NEXT
		ctx = o.withNextContext(ctx)
		session, err := o.mongoClient.StartSession()
		if err != nil {
			return nil, err
		}
		defer session.EndSession(ctx)
		return session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
			result, err := fn(ctx)
			if err != nil {
				return nil, err
			}
			err = o.runNextHandles(ctx)
			if err != nil {
				return nil, err
			}
			return result, nil
		})
	}
}

func (o *Objectql) mongoFindAll(ctx context.Context, table string, filter bson.M, selects string) ([]bson.M, error) {
	findOptions := options.Find()
	if len(selects) > 0 {
		findOptions.SetProjection(StringArrayToProjection(strings.Split(selects, ",")))
	}
	cursor, err := o.getCollection(table).Find(ctx, filter, findOptions)
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

func (o *Objectql) mongoFindOne(ctx context.Context, table string, filter bson.M, selects string) (bson.M, error) {
	findOneOptions := options.FindOne()
	if len(selects) > 0 {
		findOneOptions.SetProjection(StringArrayToProjection(strings.Split(selects, ",")))
	}
	var result bson.M
	err := o.getCollection(table).FindOne(ctx, filter, findOneOptions).Decode(&result)
	if err != nil && err != mongo.ErrNoDocuments {
		return nil, err
	}
	return result, nil
}

// func (o *Objectql) mongoCount(ctx context.Context, table string, filter bson.M) (int64, error) {
// 	count, err := o.getCollection(table).CountDocuments(ctx, filter)
// 	if err != nil {
// 		return 0, err
// 	}
// 	return count, nil
// }

func (o *Objectql) mongoInsert(ctx context.Context, table string, doc bson.M) (string, error) {
	insertResult, err := o.getCollection(table).InsertOne(ctx, doc)
	if err != nil {
		return "", err
	}
	return insertResult.InsertedID.(primitive.ObjectID).Hex(), nil
}

func (o *Objectql) mongoUpdateById(ctx context.Context, table string, id string, doc bson.M) error {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	_, err = o.getCollection(table).UpdateByID(ctx, objectId, bson.M{
		"$set": doc,
	})
	return err
}

func (o *Objectql) mongoDeleteById(ctx context.Context, table string, id string) error {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	_, err = o.getCollection(table).DeleteOne(ctx, bson.M{"_id": objectId})
	return err
}

func ObjectIdFromHex(id string) primitive.ObjectID {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		panic(err)
	}
	return objectId
}

func StringArrayToProjection(arr []string) bson.M {
	result := bson.M{}
	for _, v := range arr {
		result[v] = 1
	}
	return result
}

func (o *Objectql) mongoCountEx(ctx context.Context, table string, options countExOptions) (int, error) {
	// find object
	object := o.GetObject(table)
	if object == nil {
		return 0, fmt.Errorf("not found object %s", table)
	}

	var fields []string
	// 提取过滤条件里面的字段
	getMatchReferenceFields(&fields, options.Filter)

	// Merge fields
	fields = append(fields, options.Fields...)

	// merge fields into a nested map
	fieldsMap := mergeFields(fields)

	// convert nested map to MongoDB $project stage
	// projectStage := convertToProjectStage(fieldsMap)

	// generate $lookup stages
	var lookupStages []map[string]interface{}
	err := o.generateLookupStages(fieldsMap, table, "", &lookupStages)
	if err != nil {
		return 0, err
	}

	var pipeline []map[string]any
	pipeline = append(pipeline, lookupStages...)
	if len(options.Filter) > 0 {
		pipeline = append(pipeline, map[string]interface{}{
			"$match": options.Filter,
		})
	}
	// if options.Skip > 0 {
	// 	pipeline = append(pipeline, map[string]interface{}{
	// 		"$skip": options.Skip,
	// 	})
	// }
	// if options.Top > 0 {
	// 	pipeline = append(pipeline, map[string]interface{}{
	// 		"$limit": options.Top,
	// 	})
	// }
	pipeline = append(pipeline, M{
		"$group": M{
			"_id": nil,
			"count": M{
				"$sum": 1,
			},
		},
	})

	// writeJSONToFile("count_pipeline.json", pipeline)

	// execute the query
	cursor, err := o.getCollection(table).Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	// convert cursor results to a slice
	var results []M
	if err := cursor.All(ctx, &results); err != nil {
		return 0, err
	}
	if len(results) == 0 {
		return 0, nil
	}
	return gconv.Int(results[0]["count"]), nil
}

func (o *Objectql) mongoFindOneEx(ctx context.Context, table string, options findOneExOptions) (M, error) {
	list, err := o.mongoFindAllEx(ctx, table, findAllExOptions{
		Fields: options.Fields,
		Filter: options.Filter,
		Top:    1,
		Skip:   0,
	})
	if err != nil {
		return nil, err
	}
	if len(list) > 0 {
		return list[0], nil
	}
	return nil, nil
}

func (o *Objectql) mongoFindAllEx(ctx context.Context, table string, options findAllExOptions) ([]M, error) {
	// find object
	object := o.GetObject(table)
	if object == nil {
		return nil, fmt.Errorf("not found object %s", table)
	}

	var fields []string
	// 提取过滤条件里面的字段
	getMatchReferenceFields(&fields, options.Filter)

	// Merge fields
	fields = append(fields, options.Fields...)

	// merge fields into a nested map
	fieldsMap := mergeFields(fields)

	// convert nested map to MongoDB $project stage
	projectStage := convertToProjectStage(fieldsMap)

	// generate $lookup stages
	var lookupStages []map[string]interface{}
	err := o.generateLookupStages(fieldsMap, table, "", &lookupStages)
	if err != nil {
		return nil, err
	}

	var pipeline []map[string]any
	pipeline = append(pipeline, lookupStages...)
	if len(options.Filter) > 0 {
		pipeline = append(pipeline, map[string]interface{}{
			"$match": options.Filter,
		})
	}
	if options.Skip > 0 {
		pipeline = append(pipeline, map[string]interface{}{
			"$skip": options.Skip,
		})
	}
	if options.Top > 0 {
		pipeline = append(pipeline, map[string]interface{}{
			"$limit": options.Top,
		})
	}
	if len(projectStage) > 0 {
		pipeline = append(pipeline, map[string]interface{}{
			"$project": projectStage,
		})
	}
	// writeJSONToFile("findall_pipeline.json", pipeline)
	// execute the query
	cursor, err := o.getCollection(table).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// convert cursor results to a slice
	var results []M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	// remove primitive types
	clear := removePrimitiveTypes(results)
	// format raw database values
	err = o.formatListWithObject(object, clear.([]M))
	if err != nil {
		return nil, err
	}
	return clear.([]M), nil
}

func (o *Objectql) formatListWithObject(object *Object, list []M) error {
	for _, item := range list {
		err := o.formatValueWithObject(object, item)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) formatValueWithObject(object *Object, m primitive.M) error {
	for k, v := range m {
		if isNull(v) {
			continue
		}
		field := FindFieldFromObject(object, k)
		if field == nil {
			continue
		}
		switch n := field.Type.(type) {
		case *ExpandsType:
			list := o.convPrimitiveArrayToMapArray(v.(A))
			o.formatListWithObject(o.GetObject(n.ObjectApi), list)
		case *ExpandType:
			o.formatValueWithObject(o.GetObject(n.ObjectApi), v.(M))
		default:
			r, err := o.formatValueWithFieldType(n, v)
			if err != nil {
				return err
			}
			m[k] = r
		}
	}
	return nil
}

func (o *Objectql) convPrimitiveArrayToMapArray(arr A) []M {
	var result []M
	for _, item := range arr {
		result = append(result, item.(M))
	}
	return result
}

func (o *Objectql) formatValueWithFieldType(tpe Type, v interface{}) (interface{}, error) {
	if isNull(v) {
		return nil, nil
	}
	switch n := tpe.(type) {
	case *ObjectIDType:
		return v.(primitive.ObjectID).Hex(), nil
	case *BoolType:
		return gconv.Bool(v), nil
	case *IntType:
		return gconv.Int(v), nil
	case *FloatType:
		return gconv.Float32(v), nil
	case *StringType:
		return gconv.String(v), nil
	case *DateTimeType:
		return gconv.Time(v), nil
	case *RelateType:
		return v.(primitive.ObjectID).Hex(), nil
	case *FormulaType:
		return o.formatValueWithFieldType(n.Type, v)
	case *AggregationType:
		return o.formatValueWithFieldType(n.Type, v)
	case *ArrayType:
		return o.formatArrayValueWithFieldType(n, v)
	default:
		return nil, fmt.Errorf("formatValueWithFieldType not support type(%v)", tpe)
	}
}

func (o *Objectql) formatArrayValueWithFieldType(tpe *ArrayType, value interface{}) (interface{}, error) {
	sourceValue := reflect.ValueOf(value)
	if sourceValue.Type() != nil && sourceValue.Type().Kind() != reflect.Array && sourceValue.Type().Kind() != reflect.Slice {
		return nil, fmt.Errorf("formatArrayValueWithFieldType can't conv type %T to array", value)
	}
	sliceValue := reflect.MakeSlice(reflect.TypeOf([]any{}), 0, 0)
	for i := 0; i < sourceValue.Len(); i++ {
		evalue, err := o.formatValueWithFieldType(tpe.Type, sourceValue.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		sliceValue = reflect.Append(sliceValue, reflect.ValueOf(evalue))
	}
	return sliceValue.Interface(), nil
}

// 提取Filter里面引用到的字段
func getMatchReferenceFields(arr *[]string, value interface{}) {
	rt := reflect.TypeOf(value)
	switch rt.Kind() {
	case reflect.String:
		// 带$开头的都认为是字段
		str := value.(string)
		if strings.HasPrefix(str, "$") {
			*arr = append(*arr, strings.TrimLeft(str, "$"))
		}
	case reflect.Array, reflect.Slice:
		// 通过反射调用达到更好的兼容性
		rv := reflect.ValueOf(value)
		for i := 0; i < rv.Len(); i++ {
			getMatchReferenceFields(arr, rv.Index(i).Interface())
		}
	case reflect.Map:
		// 通过反射调用达到更好的兼容性
		rv := reflect.ValueOf(value)
		keys := rv.MapKeys()
		for _, key := range keys {
			// key 如果不带$ 则认为是字段
			if key.Kind() == reflect.String && !strings.HasPrefix(key.String(), "$") {
				*arr = append(*arr, key.String())
			}
			value := rv.MapIndex(key)
			getMatchReferenceFields(arr, value.Interface())
		}
	}
}

// MergeFields merges an array of fields into a nested map
func mergeFields(fields []string) map[string]interface{} {
	result := make(map[string]interface{})

	for _, field := range fields {
		parts := strings.Split(field, ".")
		mergeField(result, parts)
	}

	return result
}

// Recursive function to merge fields into a nested map
func mergeField(currentMap map[string]interface{}, parts []string) {
	if len(parts) == 0 {
		return
	}

	firstPart := parts[0]
	restParts := parts[1:]

	if _, exists := currentMap[firstPart]; !exists {
		currentMap[firstPart] = make(map[string]interface{})
	}

	// If it's the last part, set the value to 1
	if len(restParts) == 0 {
		currentMap[firstPart] = 1
	} else {
		// Continue merging the rest of the parts
		mergeField(currentMap[firstPart].(map[string]interface{}), restParts)
	}
}

// Convert nested map to MongoDB $project stage
func convertToProjectStage(fieldsMap map[string]interface{}) map[string]interface{} {
	projectStage := make(map[string]interface{})

	for key, value := range fieldsMap {
		switch v := value.(type) {
		case int:
			// If the value is an integer, set the field to 1
			projectStage[key] = v
		case map[string]interface{}:
			// If the value is a nested map, recursively convert it
			subProject := convertToProjectStage(v)
			for subKey, subValue := range subProject {
				projectStage[key+"."+subKey] = subValue
				// If has suffix append to id field
				if isSuffixField(key) {
					projectStage[removeFieldSuffix(key)] = subValue
				}
			}
		}
	}

	return projectStage
}

// Generate $lookup stages based on the nested map
func (o *Objectql) generateLookupStages(fieldsMap map[string]interface{}, from string, parentKey string, lookupStages *[]map[string]interface{}) error {
	if len(parentKey) > 0 {
		parentKey += "."
	}
	for key, value := range fieldsMap {
		switch v := value.(type) {
		case int:
			// If the value is an integer, do nothing
		case map[string]interface{}:
			// If the value is a nested map, set up $lookup stage
			object := o.GetObject(from)
			if object == nil {
				return fmt.Errorf("generateLookupStages error: not found object %s", from)
			}
			field := FindFieldFromObject(object, key)
			if field == nil {
				return fmt.Errorf("generateLookupStages error: not found field %s in object %s", key, from)
			}
			switch n := field.Type.(type) {
			case *ExpandType:
				table := n.ObjectApi
				lookupStage := map[string]interface{}{
					"$lookup": map[string]interface{}{
						"from":         table,
						"localField":   parentKey + removeFieldSuffix(key),
						"foreignField": "_id",
						"as":           parentKey + key,
					},
				}
				*lookupStages = append(*lookupStages, lookupStage)
				*lookupStages = append(*lookupStages, map[string]interface{}{
					"$unwind": M{
						"path":                       "$" + parentKey + key,
						"preserveNullAndEmptyArrays": true,
					},
				})
				// Recursively generate lookup stages for the nested map
				if err := o.generateLookupStages(v, table, parentKey+key, lookupStages); err != nil {
					return err
				}
			case *ExpandsType:
				table := n.ObjectApi
				idsField := removeFieldSuffix(key)
				// Build sub $lookup
				pipeline := []map[string]interface{}{
					{
						"$match": M{
							"$expr": M{
								"$in": []any{"$_id", "$$" + idsField},
							},
						},
					},
				}
				if err := o.generateLookupStages(v, table, "", &pipeline); err != nil {
					return err
				}
				// Append $lookup
				lookupStage := map[string]interface{}{
					"$lookup": map[string]interface{}{
						"from": table,
						// let: { dogs: { $ifNull: ["$dogs", []] } },
						"let": M{
							idsField: M{
								"$ifNull": []any{"$" + idsField, []any{}},
							},
						},
						"pipeline": pipeline,
						"as":       parentKey + key,
					},
				}
				*lookupStages = append(*lookupStages, lookupStage)
			default:
				return fmt.Errorf("generateLookupStages error: field %s not expand or expands in object %s", key, from)
			}
		}
	}
	return nil
}

func removePrimitiveTypes(v interface{}) interface{} {
	switch n := v.(type) {
	case primitive.A:
		r := A(n)
		for i, v := range n {
			r[i] = removePrimitiveTypes(v)
		}
		return r
	case primitive.M:
		r := M(n)
		for k, v := range n {
			r[k] = removePrimitiveTypes(v)
		}
		return r
	// 支持FindAllEx
	case []M:
		for _, m := range n {
			for k, v := range m {
				m[k] = removePrimitiveTypes(v)
			}
		}
		return n
	default:
		return v
	}
}

func removeFieldSuffix(api string) string {
	index := strings.Index(api, "__")
	if index != -1 {
		return api[:index]
	}
	return api
}

func isSuffixField(api string) bool {
	return strings.Index(api, "__") >= 0
}

type deleteExOptions struct {
	Filter primitive.M
}

type updateExOptions struct {
	Filter primitive.M
	Doc    M
}

type countExOptions struct {
	Fields []string
	Filter primitive.M
}

type findOneExOptions struct {
	Fields []string
	Filter M
}

type findAllExOptions struct {
	Fields []string
	Filter M
	Top    int
	Skip   int
	Sort   []string
}
