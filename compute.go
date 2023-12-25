package objectql

import (
	"context"
	"errors"
	"fmt"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/samber/lo"
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
	if len(objectIds) > 0 {
		// 查询相关数据
		formulaData := info.TargetField.Type.(*FormulaType)
		list, err := o.mongoFindAllEx(ctx, object.Api, findAllExOptions{
			Fields: append(formulaData.referenceFields, "_id"),
			Filter: M{
				"_id": M{
					"$in": lo.Map(objectIds, func(item string, index int) primitive.ObjectID {
						return ObjectIdFromHex(item)
					}),
				},
			},
		})
		if err != nil {
			return err
		}
		target := info.TargetField.Parent
		for _, item := range list {
			runner := formula.NewRunner()
			runner.SetThis(item)
			value, err := runner.Resolve(ctx, formulaData.sourceCode.Expression)
			if err != nil {
				return err
			}
			input, err := formatComputedValue(info.TargetField.Type, value)
			if err != nil {
				return err
			}
			err = o.updateHandle(ctx, target.Api, gconv.String(item["_id"]), bson.M{
				info.TargetField.Api: input,
			}, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
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
	// 校验ID值是否正确
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil
	}
	// 如果ID对应的记录不存在，那就不需要计算
	count, err := o.mongoCount(ctx, object.Api, bson.M{
		"_id": objectId,
	})
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}

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
