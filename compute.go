package objectql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func (o *Objectql) onFieldChange(ctx context.Context, object *Object, id string, field *Field, beforeValues bson.M) error {
	if len(field.relations) > 0 {
		for _, relation := range field.relations {
			var err error
			switch relation.TargetField.Type.(type) {
			case *FormulaType:
				err = o.formulaHandler(ctx, object, id, relation)
			case *AggregationType:
				err = o.aggregationHandler(ctx, object, id, relation, beforeValues)
			default:
				err = fmt.Errorf("target field kind %v not support", relation.TargetField.Type)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *Objectql) formulaHandler(ctx context.Context, object *Object, id string, info *relationFiledInfo) error {
	var objectIds []string
	if info.TargetField.Parent == object {
		// 计算字段在自身
		count, err := o.getCollection(object.Api).CountDocuments(ctx, bson.M{"_id": ObjectIdFromHex(id)})
		if err != nil {
			return err
		}
		// 没有找到这条记录,忽略掉它
		if count == 0 {
			return nil
		}
		objectIds = append(objectIds, id)
	} else {
		// 存在通过字段肯定是相关表
		objectId, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			return err
		}
		result, err := o.mongoFindAll(ctx, info.ThroughField.Parent.Api, bson.M{info.ThroughField.Api: objectId}, "_id")
		if err != nil {
			return err
		}
		// 没有找到相关记录,忽略掉它
		if len(result) == 0 {
			return nil
		}
		for _, item := range result {
			objectIds = append(objectIds, item["_id"].(primitive.ObjectID).Hex())
		}
	}

	runner := formula.NewRunner()
	runner.IdentifierResolver = o.resolverIdentifier
	runner.SelectorExpressionResolver = o.resolveSelectorExpression
	formulaData := info.TargetField.Type.(*FormulaType)
	target := info.TargetField.Parent
	for _, objectId := range objectIds {
		runner.Set("object", target)
		runner.Set("objectId", objectId)
		value, err := runner.Resolve(ctx, formulaData.sourceCode.Expression)
		if err != nil {
			return err
		}
		input, err := formatComputedValue(info.TargetField.Type, value)
		if err != nil {
			return err
		}
		err = o.updateHandle(ctx, target.Api, objectId, bson.M{
			info.TargetField.Api: input,
		}, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) resolverIdentifier(ctx context.Context, name string) (interface{}, error) {
	runner := formula.RunnerFromCtx(ctx)
	object := runner.Get("object").(*Object)
	objectIdStr := runner.Get("objectId").(string)
	// 先找到这个字段
	field := FindFieldFromObject(object, name)
	if field == nil {
		return nil, fmt.Errorf("can't found field '%s' from object '%s'", name, object.Api)
	}
	// 将这个记录查找出来
	one, err := o.mongoFindOne(ctx, object.Api, bson.M{"_id": ObjectIdFromHex(objectIdStr)}, name)
	if err != nil {
		return nil, err
	}
	v, err := formatDatabaseValueToCompute(field, one[name])
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (o *Objectql) resolveSelectorExpression(ctx context.Context, name string) (interface{}, error) {
	arr := strings.Split(name, ".")
	if len(arr) > 2 {
		return nil, fmt.Errorf("select expression len > 2")
	}
	relationFieldApi := arr[0]
	valueFieldApi := arr[1]
	runner := formula.RunnerFromCtx(ctx)
	object := runner.Get("object").(*Object)
	objectId := runner.Get("objectId").(string)
	// 先找到这个字段
	field := FindFieldFromObject(object, relationFieldApi)
	if field == nil {
		return nil, fmt.Errorf("can't found field '%s' from object '%s'", name, object.Api)
	}
	// 获取相关表对象声明
	relateObjectApi := field.Type.(*RelateType).ObjectApi
	valueField, err := FindFieldFromName(o.list, relateObjectApi, valueFieldApi)
	if err != nil {
		return nil, err
	}
	// 将相关表的值查找出来
	one, err := o.mongoFindOne(ctx, object.Api, bson.M{"_id": ObjectIdFromHex(objectId)}, relationFieldApi)
	if err != nil {
		return nil, err
	}
	// 找不到就忽略掉返回一个默认值
	if one == nil || one[relationFieldApi] == nil {
		v, err := getFieldComputeDefaultValue(valueField)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	// 查询相关表对应的值
	relate, err := o.mongoFindOne(ctx, relateObjectApi, bson.M{"_id": one[relationFieldApi]}, valueFieldApi)
	if err != nil {
		return nil, err
	}
	// 找不到就忽略掉返回一个默认值
	if relate == nil {
		v, err := getFieldComputeDefaultValue(valueField)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	v, err := formatDatabaseValueToCompute(valueField, relate[valueFieldApi])
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (o *Objectql) aggregationHandler(ctx context.Context, object *Object, id string, info *relationFiledInfo, beforeValues bson.M) error {
	// 聚合2次, 修改前和修改后
	// 修改前
	if beforeValues != nil && beforeValues[info.ThroughField.Api] != nil {
		objectId := beforeValues[info.ThroughField.Api]
		err := o.aggregateField(ctx, info.TargetField.Parent, objectId.(primitive.ObjectID).Hex(), info.TargetField)
		if err != nil {
			return err
		}
	}
	// 修改后
	data, err := o.mongoFindOne(ctx, object.Api, bson.M{"_id": ObjectIdFromHex(id)}, info.ThroughField.Api)
	if err != nil {
		panic(err)
	}
	if data != nil && data[info.ThroughField.Api] != nil {
		objectId := data[info.ThroughField.Api]
		err := o.aggregateField(ctx, info.TargetField.Parent, objectId.(primitive.ObjectID).Hex(), info.TargetField)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) aggregateField(ctx context.Context, object *Object, id string, field *Field) error {
	adata := field.Type.(*AggregationType)

	// 聚合方法
	funcStr := ""
	switch adata.Kind {
	case Sum:
		funcStr = "$sum"
	case Avg:
		funcStr = "$avg"
	case Min:
		funcStr = "$min"
	case Max:
		funcStr = "$max"
	default:
		return errors.New("not support aggregate kind")
	}
	// 聚合查询
	cursor, err := o.getCollection(adata.Object).Aggregate(ctx, []bson.M{
		{
			"$match": bson.M{
				adata.Relate: ObjectIdFromHex(id),
			},
		},
		{
			"$group": bson.M{
				"_id":    "$item",
				"result": bson.M{funcStr: "$" + adata.Field},
			},
		},
	})
	if err != nil {
		return err
	}
	result, err := readOneFromCuresor(ctx, cursor)
	if err != nil {
		return err
	}
	// 应用修改
	// TODO: 需要根据聚合字段的类型来存储
	var value float64 = 0
	if result != nil {
		value = gconv.Float64(result["result"])
	}
	err = o.updateHandle(ctx, object.Api, id, bson.M{
		field.Api: value,
	}, true)
	if err != nil {
		return err
	}
	return nil
}

func readOneFromCuresor(ctx context.Context, cursor *mongo.Cursor) (bson.M, error) {
	var result bson.M
	if cursor.Next(ctx) {
		err := cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, nil
}
