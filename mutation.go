package objectql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aundis/graphql"
	"github.com/gogf/gf/v2/util/gconv"
)

func (o *Objectql) insertHandle(ctx context.Context, api string, doc map[string]interface{}) (string, error) {
	res, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		return o.insertHandleRaw(ctx, api, doc)
	})
	return gconv.String(res), err
}

func (o *Objectql) insertHandleRaw(ctx context.Context, api string, doc map[string]interface{}) (string, error) {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return "", ErrNotFoundObject
	}
	// 对象权限校验
	err := o.checkObjectPermission(ctx, object.Api, ObjectInsert)
	if err != nil {
		return "", err
	}
	// 设定默认值
	o.initDefaultValues(object.Fields, doc)
	// insertBefore 事件触发 (可以修改表单内容)
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerInsertBefore(ctx, api, NewVar(doc))
		if err != nil {
			return "", err
		}
	}
	// 数据校验层
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
	// 添加创建时间
	doc["createTime"] = time.Now()
	// 添加拥有者
	if len(o.operatorObject) > 0 && o.getOperator != nil {
		owner, err := o.getOperator(ctx)
		if err != nil {
			return "", err
		}
		doc["owner"] = owner
	}
	err = formatDocumentToDatabase(object.Fields, doc)
	if err != nil {
		return "", err
	}
	// check bool require
	err = o.checkInsertFieldBoolRequires(object, doc)
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
	// after 数据查询
	var after *Var
	if ctx.Value(blockEventsKey) != true {
		after, _, err = o.queryEventObjectEntity(ctx, object, objectIdStr, InsertAfter)
		if err != nil {
			return "", err
		}
	}
	// require 校验
	err = o.checkFieldFormulaOrHandledRequires(ctx, object, after)
	if err != nil {
		return "", err
	}
	// validate 校验
	err = o.checkFieldFormulaOrHandledValidates(ctx, object, after)
	if err != nil {
		return "", err
	}
	// insertAfter 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerInsertAfter(ctx, api, objectIdStr, NewVar(doc))
		if err != nil {
			return "", err
		}
	}
	// insertAfterEx 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerInsertAfterEx(ctx, api, objectIdStr, NewVar(doc), after)
		if err != nil {
			return "", err
		}
	}
	// fieldChange 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerChange(ctx, object, NewVar(nil), after, InsertAfter)
		if err != nil {
			return "", err
		}
	}
	return objectIdStr, nil
}

func (o *Objectql) initDefaultValues(fields []*Field, doc map[string]interface{}) {
	for _, field := range fields {
		if field.Default == nil || !isNull(doc[field.Api]) {
			continue
		}
		if field.Default == Null {
			doc[field.Api] = nil
		} else {
			doc[field.Api] = field.Default
		}
	}
}

// permissionBlock 用于内部公式计算的时候屏蔽权限的校验
func (o *Objectql) updateHandle(ctx context.Context, api string, id string, doc map[string]interface{}, permissionBlock bool) error {
	_, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		return nil, o.updateHandleRaw(ctx, api, id, doc, permissionBlock)
	})
	return err
}

func (o *Objectql) updateHandleRaw(ctx context.Context, api string, id string, doc map[string]interface{}, permissionBlock bool) error {
	var err error
	// var err error
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return ErrNotFoundObject
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
	// before 值查询
	var before *Var
	if ctx.Value(blockEventsKey) != true {
		var exists bool
		before, exists, err = o.queryEventObjectEntity(ctx, object, id, UpdateBefore)
		if err != nil {
			return err
		}
		// TODO: 表示指定的ID记录不存在
		if !exists {
			return nil
		}
	}
	// updateBefore 事件触发 (可以修改表单内容)
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerUpdateBefore(ctx, api, id, NewVar(doc))
		if err != nil {
			return err
		}
	}
	// updateBeforeEx 事件触发 (可以修改表单内容)
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerUpdateBeforeEx(ctx, api, id, NewVar(doc), before)
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
	// 添加修改时间
	doc["updateTime"] = time.Now()
	// 数据库修改
	err = formatDocumentToDatabase(object.Fields, doc)
	if err != nil {
		return err
	}
	// check bool require
	err = o.checkUpdateFieldBoolRequires(object, doc)
	if err != nil {
		return err
	}
	// 写入到数据库
	count, err := o.mongoUpdateById(ctx, api, id, doc)
	if err != nil {
		return err
	}
	// TODO: 表示指定的ID记录不存在
	if count == 0 {
		return nil
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
	// after 值查询
	var after *Var
	if ctx.Value(blockEventsKey) != true {
		after, _, err = o.queryEventObjectEntity(ctx, object, id, UpdateAfter)
		if err != nil {
			return err
		}
	}
	// require 校验
	err = o.checkFieldFormulaOrHandledRequires(ctx, object, after)
	if err != nil {
		return err
	}
	// validate 校验
	err = o.checkFieldFormulaOrHandledValidates(ctx, object, after)
	if err != nil {
		return err
	}
	// updateAfter 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerUpdateAfter(ctx, api, id, NewVar(doc))
		if err != nil {
			return err
		}
	}
	// updateAfterEx 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerUpdateAfterEx(ctx, api, id, NewVar(doc), after)
		if err != nil {
			return err
		}
	}
	// fieldChange 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerChange(ctx, object, before, after, UpdateAfter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) deleteHandle(ctx context.Context, api string, id string) error {
	_, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		return nil, o.deleteHandleRaw(ctx, api, id)
	})
	return err
}

func (o *Objectql) deleteHandleRaw(ctx context.Context, api string, id string) error {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return ErrNotFoundObject
	}
	// 对象权限校验
	err := o.checkObjectPermission(ctx, object.Api, ObjectInsert)
	if err != nil {
		return err
	}
	// before 数据查询
	var before *Var
	if ctx.Value(blockEventsKey) != true {
		var exists bool
		before, exists, err = o.queryEventObjectEntity(ctx, object, id, DeleteBefore)
		if err != nil {
			return err
		}
		// TODO: 表示指定的ID记录不存在
		if !exists {
			return nil
		}
	}
	// deleteBefore 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerDeleteBefore(ctx, api, id)
		if err != nil {
			return err
		}
	}
	// deleteBeforeEx 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerDeleteBeforeEx(ctx, api, id, before)
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
	count, err := o.mongoDeleteById(ctx, api, id)
	if err != nil {
		return err
	}
	if count == 0 {
		// TODO: 表示指定的ID记录不存在
		return nil
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
	// deleteAfterEx 事件触发(这里用的是before数据)
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerDeleteAfterEx(ctx, api, id, before)
		if err != nil {
			return err
		}
	}
	// fieldChange 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerChange(ctx, object, before, NewVar(nil), DeleteAfter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) queryEventObjectEntity(ctx context.Context, object *Object, id string, position EventPosition) (*Var, bool, error) {
	// event 中需要查询的字段
	qFields := o.getListenQueryFields(ctx, object.Api, position)
	// require, validate 中需要查询的字段
	if position == InsertAfter || position == UpdateAfter {
		qFields = append(qFields, o.getObjectRequireQueryFields(object)...)
		qFields = append(qFields, o.getObjectValidateQueryFields(object)...)
	}
	if len(qFields) == 0 {
		return nil, true, nil
	}
	qFields = append(qFields, "_id")
	one, err := o.mongoFindOneEx(ctx, object.Api, findOneExOptions{
		Fields: qFields,
		Filter: M{
			"_id": ObjectIdFromHex(id),
		},
	})
	if err != nil {
		return nil, false, err
	}
	if one == nil {
		return nil, false, nil
	}
	return NewVar(one), true, nil
}

func (o *Objectql) graphqlMutationQueryOne(ctx context.Context, p graphql.ResolveParams, object *Object, id string) (interface{}, error) {
	one, err := o.mongoFindOneEx(ctx, object.Api, findOneExOptions{
		Fields: o.parseMongoQueryFields(p),
		Filter: map[string]any{
			"_id": ObjectIdFromHex(id),
		},
	})
	if err != nil {
		return nil, err
	}
	return one, nil
}

func buildFieldQueryString(obj map[string]interface{}, prefix string) string {
	var query string
	for key, value := range obj {
		fullPath := key
		if prefix != "" {
			fullPath = fmt.Sprintf("%s.%s", prefix, key)
		}
		query += key
		if len(value.(map[string]interface{})) > 0 {
			query += fmt.Sprintf(" { %s }", buildFieldQueryString(value.(map[string]interface{}), fullPath))
		} else {
			query += " "
		}
	}
	return query
}

func strings2GraphqlFieldQuery(arr ...string) string {
	result := make(map[string]interface{})

	for _, item := range arr {
		parts := strings.Split(item, ".")
		current := result

		for _, part := range parts {
			if current[part] == nil {
				current[part] = make(map[string]interface{})
			}
			current = current[part].(map[string]interface{})
		}
	}

	return buildFieldQueryString(result, "")
}
