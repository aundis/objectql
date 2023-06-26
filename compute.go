package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aundis/formula"
	"github.com/gogf/gf/v2/util/gconv"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func (o *Objectql) onFieldChange(object *Object, id string, field *Field, beforeValues bson.M) error {
	if len(field.relations) > 0 {
		for _, relation := range field.relations {
			var err error
			switch relation.TargetField.Type {
			case Formula:
				err = o.formulaHandler(object, id, relation)
			case Aggregation:
				err = o.aggregationHandler(object, id, relation, beforeValues)
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

func (o *Objectql) formulaHandler(object *Object, id string, info *RelationFiledInfo) error {
	var objectIds []string
	if info.TargetField.Parent == object {
		// 计算字段在自身
		count, err := o.getCollection(object.Api).Find(bson.M{"_id": bson.ObjectIdHex(id)}).Count()
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
		var result []bson.M
		err := o.getCollection(info.ThroughField.Parent.Api).Find(bson.M{info.ThroughField.Api: bson.ObjectIdHex(id)}).Select(bson.M{"_id": 1}).All(&result)
		if err != nil {
			return err
		}
		// 没有找到相关记录,忽略掉它
		if len(result) == 0 {
			return nil
		}
		for _, item := range result {
			objectIds = append(objectIds, item["_id"].(bson.ObjectId).Hex())
		}
	}

	runner := formula.NewRunner()
	runner.IdentifierResolver = o.resolverIdentifier
	runner.SelectorExpressionResolver = o.resolveSelectorExpression
	formulaData := info.TargetField.Data.(*FormulaData)
	target := info.TargetField.Parent
	for _, objectId := range objectIds {
		runner.Set("object", target)
		runner.Set("objectId", objectId)
		value, err := runner.Resolve(context.Background(), formulaData.SourceCode.Expression)
		if err != nil {
			return err
		}
		formated, err := formatFormulaReturnValue(info.TargetField, value)
		if err != nil {
			return err
		}
		err = o.Update(target.Api, objectId, bson.M{
			info.TargetField.Api: formated,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) resolverIdentifier(ctx context.Context, name string) (interface{}, error) {
	runner := formula.RunnerFromCtx(ctx)
	object := runner.Get("object").(*Object)
	objectId := runner.Get("objectId").(string)
	// 先找到这个字段
	field := FindFieldFromObject(object, name)
	if field == nil {
		return nil, fmt.Errorf("can't found field '%s' from object '%s'", name, object.Api)
	}
	// 将这个记录查找出来
	var one bson.M
	err := o.getCollection(object.Api).Find(bson.M{"_id": bson.ObjectIdHex(objectId)}).Select(bson.M{name: 1}).One(&one)
	if err != nil {
		return nil, err
	}
	return formatFormulaReturnValue(field, one[name])
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
	relateObjectApi := field.Data.(*RelateData).ObjectApi
	valueField, err := FindFieldFromName(o.list, relateObjectApi, valueFieldApi)
	if err != nil {
		return nil, err
	}
	// 将相关表的值查找出来
	var one bson.M
	err = o.getCollection(object.Api).Find(bson.M{"_id": bson.ObjectIdHex(objectId)}).Select(bson.M{relationFieldApi: 1}).One(&one)
	if err != nil && err != mgo.ErrNotFound {
		return nil, err
	}
	// 找不到就忽略掉返回一个默认值
	if one == nil || one[relationFieldApi] == nil {
		v, err := formatFormulaReturnValue(valueField, nil)
		if err != nil {
			return nil, err
		}
		return formula.FormatValue(v)
	}
	// 查询相关表对应的值
	var relate bson.M
	err = o.getCollection(relateObjectApi).Find(bson.M{"_id": one[relationFieldApi]}).Select(bson.M{valueFieldApi: 1}).One(&relate)
	if err != nil {
		return nil, err
	}
	// 找不到就忽略掉返回一个默认值
	if relate == nil {
		v, err := formatFormulaReturnValue(valueField, nil)
		if err != nil {
			return nil, err
		}
		return formula.FormatValue(v)
	}
	v, err := formatFormulaReturnValue(valueField, relate[valueFieldApi])
	if err != nil {
		return nil, err
	}
	return formula.FormatValue(v)
}

func formatFormulaReturnValue(field *Field, value interface{}) (interface{}, error) {
	switch field.Type {
	case Int, Float, Bool, String:
		return basicFormatFormulaReturnValue(field.Type, value)
	case Relate:
		return basicFormatFormulaReturnValue(String, value)
	case Formula:
		data := field.Data.(*FormulaData)
		return basicFormatFormulaReturnValue(data.Type, value)
	case Aggregation:
		data := field.Data.(*AggregationData)
		return basicFormatFormulaReturnValue(data.Type, value)
	default:
		return nil, fmt.Errorf("formatFormulaReturnValue unknown field type %v", field.Type)
	}
}

func basicFormatFormulaReturnValue(tpe FieldType, value interface{}) (interface{}, error) {
	switch tpe {
	case Int:
		return formula.ToInt(value)
	case Float:
		return formula.ToFloat32(value)
	case Bool:
		return formula.ToBool(value)
	case String:
		return formula.ToString(value)
	default:
		return nil, fmt.Errorf("basicFormatFormulaReturnValue unknown field type %v", tpe)
	}
}

func (o *Objectql) aggregationHandler(object *Object, id string, info *RelationFiledInfo, beforeValues bson.M) error {
	// 聚合2次, 修改前和修改后
	// 修改前
	if beforeValues != nil && beforeValues[info.ThroughField.Api] != nil {
		objectId := beforeValues[info.ThroughField.Api]
		err := o.aggregateField(info.TargetField.Parent, objectId.(bson.ObjectId).Hex(), info.TargetField)
		if err != nil {
			return err
		}
	}
	// 修改后
	data, err := o.GetObjectByID(object.Api, id)
	if err != nil {
		panic(err)
	}
	if data != nil && data[info.ThroughField.Api] != nil {
		objectId := data[info.ThroughField.Api]
		err := o.aggregateField(info.TargetField.Parent, objectId.(bson.ObjectId).Hex(), info.TargetField)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) aggregateField(object *Object, id string, field *Field) error {
	adata := field.Data.(*AggregationData)
	// TODO:这个要放到初始化那边去
	if adata.Resolved == nil {
		field, err := FindFieldFromName(o.list, adata.Object, adata.Relate)
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
		return errors.New("not support aggregate kind")
	}
	// 聚合查询
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
		return err
	}
	// 应用修改
	// TODO: 需要根据聚合字段的类型来存储
	var value float64 = 0
	if result != nil {
		value = gconv.Float64(result["result"])
	}
	err = o.Update(object.Api, id, bson.M{
		field.Api: value,
	})
	if err != nil {
		return err
	}
	return nil
}
