package main

import (
	"context"

	"gopkg.in/mgo.v2/bson"
)

func (o *Objectql) insertHandle(ctx context.Context, api string, doc map[string]interface{}) (string, error) {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return "", NotFoundObjectErr
	}
	// 数据校验层
	// 权限校验
	// insertBefore 事件触发 (可以修改表单内容)
	// 数据库修改
	err := formatInputValue(object.Fields, doc)
	if err != nil {
		return "", err
	}
	objectId := bson.NewObjectId()
	doc["_id"] = objectId
	err = o.getCollection(ctx, api).Insert(doc)
	if err != nil {
		return "", err
	}
	// 数据联动
	for _, field := range object.Fields {
		if _, ok := doc[field.Api]; ok {
			err = o.onFieldChange(ctx, object, objectId.Hex(), field, nil)
			if err != nil {
				return "", err
			}
		}
	}
	// insertAfter 事件触发
	return objectId.Hex(), nil
}

func (o *Objectql) updateHandle(ctx context.Context, api string, id string, doc map[string]interface{}) error {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return NotFoundObjectErr
	}
	// 数据校验
	// 权限校验
	// updateBefore 事件触发 (可以修改表单内容)
	// 保存相关表的字段
	beforeValues, err := o.getObjectBeforeValues(ctx, object, id)
	if err != nil {
		return err
	}
	// 数据库修改
	err = formatInputValue(object.Fields, doc)
	if err != nil {
		return err
	}
	err = o.getCollection(ctx, api).Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{
		"$set": doc,
	})
	if err != nil {
		return err
	}
	// 数据联动
	for _, field := range object.Fields {
		if _, ok := doc[field.Api]; ok {
			err = o.onFieldChange(ctx, object, id, field, beforeValues)
			if err != nil {
				return err
			}
		}
	}
	// updateAfter 事件触发
	return nil
}

func (o *Objectql) deleteHandle(ctx context.Context, api string, id string) error {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return NotFoundObjectErr
	}
	// 数据校验
	// 权限校验
	// deleteBefore 事件触发
	// 保存相关表的字段
	beforeValues, err := o.getObjectBeforeValues(ctx, object, id)
	if err != nil {
		return err
	}
	// 数据库修改
	err = o.getCollection(ctx, api).RemoveId(bson.ObjectIdHex(id))
	if err != nil {
		return err
	}
	// 数据联动
	for _, field := range object.Fields {
		err = o.onFieldChange(ctx, object, id, field, beforeValues)
		if err != nil {
			return err
		}
	}
	// deleteAfter 事件触发
	return nil
}

func (o *Objectql) getObjectBeforeValues(ctx context.Context, object *Object, id string) (map[string]interface{}, error) {
	beforeValues := map[string]interface{}{}
	apis := getObjectRelationObjectApis(object)
	if len(apis) > 0 {
		err := o.getCollection(ctx, object.Api).Find(bson.M{"_id": bson.ObjectIdHex(id)}).Select(stringArrayToMongodbSelects(apis)).One(&beforeValues)
		if err != nil {
			return nil, err
		}
	}
	return beforeValues, nil
}
