package main

import (
	"context"

	"github.com/aundis/graphql"
	"go.mongodb.org/mongo-driver/bson"
)

func (o *Objectql) insertHandle(ctx context.Context, api string, doc map[string]interface{}) (string, error) {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return "", NotFoundObjectErr
	}
	// 对象权限校验
	err := o.checkObjectPermission(ctx, object.Api, ObjectInsert)
	if err != nil {
		return "", err
	}
	// 数据校验层
	err = o.validateDocument(object, doc)
	if err != nil {
		return "", err
	}
	// insertBefore 事件触发 (可以修改表单内容)
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerInsertBefore(ctx, api, doc)
		if err != nil {
			return "", err
		}
	}
	// 数据校验层(数据可能被修改了,所以在校验一次)
	err = o.validateDocument(object, doc)
	if err != nil {
		return "", err
	}
	// 字段权限校验
	err = o.checkObjectFieldPermissionWithDocument(ctx, object, doc, FieldUpdate)
	if err != nil {
		return "", err
	}
	// 数据库修改
	err = formatInputValue(object.Fields, doc)
	if err != nil {
		return "", err
	}
	objectIdStr, err := o.mongoInsert(ctx, api, doc)
	if err != nil {
		return "", err
	}
	// 数据联动
	for _, field := range object.Fields {
		if _, ok := doc[field.Api]; ok {
			err = o.onFieldChange(ctx, object, objectIdStr, field, nil)
			if err != nil {
				return "", err
			}
		}
	}
	// insertAfter 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerInsertAfter(ctx, api, objectIdStr, doc)
		if err != nil {
			return "", err
		}
	}
	return objectIdStr, nil
}

func (o *Objectql) updateHandle(ctx context.Context, api string, id string, doc map[string]interface{}, permissionBlock bool) error {
	var err error
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return NotFoundObjectErr
	}
	// 对象权限校验
	if !permissionBlock {
		err = o.checkObjectPermission(ctx, object.Api, ObjectInsert)
		if err != nil {
			return err
		}
	}
	// 数据校验
	err = o.validateDocument(object, doc)
	if err != nil {
		return err
	}
	// updateBefore 事件触发 (可以修改表单内容)
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerUpdateBefore(ctx, api, id, doc)
		if err != nil {
			return err
		}
	}
	// 数据校验(数据可能被修改了,所以再校验一次)
	err = o.validateDocument(object, doc)
	if err != nil {
		return err
	}
	// 字段权限校验
	if !permissionBlock {
		err = o.checkObjectFieldPermissionWithDocument(ctx, object, doc, FieldUpdate)
		if err != nil {
			return err
		}
	}
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
	err = o.mongoUpdateById(ctx, api, id, doc)
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
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerUpdateAfter(ctx, api, id, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) deleteHandle(ctx context.Context, api string, id string) error {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return NotFoundObjectErr
	}
	// 对象权限校验
	err := o.checkObjectPermission(ctx, object.Api, ObjectInsert)
	if err != nil {
		return err
	}
	// deleteBefore 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerDeleteBefore(ctx, api, id)
		if err != nil {
			return err
		}
	}
	// 保存相关表的字段
	beforeValues, err := o.getObjectBeforeValues(ctx, object, id)
	if err != nil {
		return err
	}
	// 数据库修改
	err = o.mongoDeleteById(ctx, api, id)
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
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerDeleteAfter(ctx, api, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) graphqlMutationQueryOne(ctx context.Context, p graphql.ResolveParams, object *Object, id string) (interface{}, error) {
	options, err := o.parseMongoFindOneOptinos(ctx, p)
	if err != nil {
		return nil, err
	}

	var one bson.M
	err = o.getCollection(object.Api).FindOne(ctx, bson.M{"_id": ObjectIdFromHex(id)}, options).Decode(&one)
	if err != nil {
		return nil, err
	}
	return one, nil
}
