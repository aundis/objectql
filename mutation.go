package main

import (
	"gopkg.in/mgo.v2/bson"
)

func (o *Objectql) Insert(api string, doc map[string]interface{}) (string, error) {
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
	err = o.getCollection(api).Insert(doc)
	if err != nil {
		return "", err
	}
	// 数据联动
	for _, field := range object.Fields {
		if _, ok := doc[field.Api]; ok {
			err = o.onFieldChange(object, objectId.Hex(), field, nil)
			if err != nil {
				return "", err
			}
		}
	}
	// insertAfter 事件触发
	return objectId.Hex(), nil
}

func (o *Objectql) Update(api string, id string, doc map[string]interface{}) error {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return NotFoundObjectErr
	}
	// 数据校验
	// 权限校验
	// updateBefore 事件触发 (可以修改表单内容)
	// 保存相关表的字段
	beforeValues, err := o.getObjectBeforeValues(object, id)
	if err != nil {
		return err
	}
	// 数据库修改
	err = formatInputValue(object.Fields, doc)
	if err != nil {
		return err
	}
	err = o.getCollection(api).Update(bson.M{"_id": bson.ObjectIdHex(id)}, bson.M{
		"$set": doc,
	})
	if err != nil {
		return err
	}
	// 数据联动
	for _, field := range object.Fields {
		if _, ok := doc[field.Api]; ok {
			err = o.onFieldChange(object, id, field, beforeValues)
			if err != nil {
				return err
			}
		}
	}
	// updateAfter 事件触发
	return nil
}

func (o *Objectql) Delete(api string, id string) error {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return NotFoundObjectErr
	}
	// 数据校验
	// 权限校验
	// deleteBefore 事件触发
	// 保存相关表的字段
	beforeValues, err := o.getObjectBeforeValues(object, id)
	if err != nil {
		return err
	}
	// 数据库修改
	err = o.getCollection(api).RemoveId(bson.ObjectIdHex(id))
	if err != nil {
		return err
	}
	// 数据联动
	for _, field := range object.Fields {
		err = o.onFieldChange(object, id, field, beforeValues)
		if err != nil {
			return err
		}
	}
	// deleteAfter 事件触发
	return nil
}

func (o *Objectql) getObjectBeforeValues(object *Object, id string) (map[string]interface{}, error) {
	beforeValues := map[string]interface{}{}
	apis := getObjectRelationObjectApis(object)
	if len(apis) > 0 {
		c := session.DB("test").C(object.Api)
		err := c.Find(bson.M{"_id": bson.ObjectIdHex(id)}).Select(stringArrayToMongodbSelects(apis)).One(&beforeValues)
		if err != nil {
			return nil, err
		}
	}
	return beforeValues, nil
}
