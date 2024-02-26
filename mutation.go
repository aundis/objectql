package objectql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aundis/graphql"
	"github.com/gogf/gf/v2/util/gconv"
)

func (o *Objectql) insertHandle(ctx context.Context, api string, doc map[string]interface{}, pos *IndexPosition) (string, error) {
	res, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		return o.insertHandleRaw(ctx, api, doc, pos)
	})
	return gconv.String(res), err
}

func (o *Objectql) insertHandleRaw(ctx context.Context, api string, doc map[string]interface{}, pos *IndexPosition) (string, error) {
	doc = copyStrAnyMap(doc)
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
	// check priamry require
	err = o.checkInsertPrimaryFieldRequires(object, doc)
	if err != nil {
		return "", err
	}
	// 写索引位置
	if object.Index {
		err = o.initInsertRowIndex(ctx, object, doc, pos)
		if err != nil {
			return "", err
		}
	}
	// 写入到数据库
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
	// 触发 immediate 的公式字段
	err = o.triggerImmediateFormulaFields(ctx, object, objectIdStr)
	if err != nil {
		return "", err
	}
	// after 数据查询
	var after *Var
	if ctx.Value(blockEventsKey) != true {
		after, _, err = o.queryEventObjectEntity(ctx, object, objectIdStr, doc, InsertAfter)
		if err != nil {
			return "", err
		}
	}
	// priamry 校验
	err = o.checkPrimaryDuplicate(ctx, object, after)
	if err != nil {
		return "", err
	}
	// require 校验
	err = o.checkInsertFieldFormulaOrHandledRequires(ctx, object, after)
	if err != nil {
		return "", err
	}
	// validate 校验
	err = o.checkFieldFormulaOrHandledValidates(ctx, object, doc, after)
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
	// indexChange 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerIndexChange(ctx, object.Api, objectIdStr, NewVar(nil), after, InsertAfter)
		if err != nil {
			return "", err
		}
	}
	return objectIdStr, nil
}

func (o *Objectql) triggerImmediateFormulaFields(ctx context.Context, object *Object, id string) error {
	if len(object.immediateFormulaFields) == 0 {
		return nil
	}
	for _, field := range object.immediateFormulaFields {
		err := o.formulaHandler(ctx, object, id, &relationFiledInfo{
			ThroughField: nil,
			TargetField:  field,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) initInsertRowIndex(ctx context.Context, object *Object, doc map[string]interface{}, pos *IndexPosition) error {
	if pos != nil {
		// 插入到指定位置
		filter, err := o.getGroupFilterFromDoc(ctx, object, doc)
		if err != nil {
			return err
		}
		realIndex, err := o.getRealIndex(ctx, object, filter, *pos)
		if err != nil {
			return err
		}
		add := pos.Dir
		if add == 0 {
			firstIndex, err := o.getRealIndex(ctx, object, filter, *pos)
			if err != nil {
				return err
			}
			if err != nil {
				return err
			}
			if realIndex == firstIndex {
				add = 1
			} else {
				add = -1
			}
		}
		err = o.indexOffset(ctx, object.Api, filter, realIndex, add)
		if err != nil {
			return err
		}
		doc["__index"] = realIndex
	} else {
		// 插入到末尾
		gropu, err := o.documentToGroupFilter(object, doc)
		if err != nil {
			return err
		}
		max, err := o.getMaxIndex(ctx, object, gropu)
		if err != nil {
			return err
		}
		toIndex := max + 1
		doc["__index"] = toIndex
	}
	return nil
}

func (o *Objectql) getGroupFilterFromDoc(ctx context.Context, object *Object, doc map[string]interface{}) (M, error) {
	result := M{}
	for _, gapi := range object.IndexGroup {
		f := FindFieldFromObject(object, gapi)
		if f == nil {
			return nil, fmt.Errorf(`%s not found index group field %s`, object.Api, gapi)
		}
		mongoV, err := formatValueToDatabase(f.Type, doc[gapi])
		if err != nil {
			return nil, err
		}
		result[gapi] = mongoV
	}
	return result, nil
}

func (o *Objectql) documentToGroupFilter(object *Object, doc M) (M, error) {
	matchValues := M{}
	for _, fapi := range object.IndexGroup {
		f := FindFieldFromObject(object, fapi)
		if f == nil {
			return nil, fmt.Errorf(`%s not found index group field %s`, object.Api, fapi)
		}
		mongoV, err := formatValueToDatabase(f.Type, doc[fapi])
		if err != nil {
			return nil, err
		}
		matchValues[fapi] = mongoV
	}
	return matchValues, nil
}

func (o *Objectql) getMaxIndex(ctx context.Context, object *Object, group M) (int, error) {
	var pipeline []M
	if len(group) > 0 {
		pipeline = append(pipeline, M{
			"$match": group,
		})
	}
	pipeline = append(pipeline, M{
		"$group": M{
			"_id":    "$item",
			"result": M{"$max": "$__index"},
		},
	})

	cursor, err := o.getCollection(object.Api).Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	result, err := readOneFromCuresor(ctx, cursor)
	if err != nil {
		return 0, err
	}
	// 解析最大索引值
	var value int = 0
	if result != nil {
		value = gconv.Int(result["result"])
	}
	return value, nil
}

func (o *Objectql) initDefaultValues(fields []*Field, doc map[string]interface{}) {
	for _, field := range fields {
		if field.Default == nil || !isNull(doc[field.Api]) {
			continue
		}
		if field.Default != nil {
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
	doc = copyStrAnyMap(doc)
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
		before, exists, err = o.queryEventObjectEntity(ctx, object, id, doc, UpdateBefore)
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
	// check priamry require
	err = o.checkUpdatePrimaryFieldBoolRequires(object, doc)
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
		after, _, err = o.queryEventObjectEntity(ctx, object, id, doc, UpdateAfter)
		if err != nil {
			return err
		}
	}
	// priamry 校验
	err = o.checkPrimaryDuplicate(ctx, object, after)
	if err != nil {
		return err
	}
	// require 校验
	err = o.checkUpdateFieldFormulaOrHandledRequires(ctx, object, doc, after)
	if err != nil {
		return err
	}
	// validate 校验
	err = o.checkFieldFormulaOrHandledValidates(ctx, object, doc, after)
	if err != nil {
		return err
	}
	// updateable 校验
	err = o.checkFieldFormulaOrHandledUpdateables(ctx, object, doc, after, before)
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
		before, exists, err = o.queryEventObjectEntity(ctx, object, id, nil, DeleteBefore)
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
	// indexChange 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerIndexChange(ctx, object.Api, id, before, NewVar(nil), DeleteAfter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) moveHandle(ctx context.Context, api string, id string, pos IndexPosition) error {
	_, err := o.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		return nil, o.moveHandleRaw(ctx, api, id, pos)
	})
	return err
}

func (o *Objectql) moveHandleRaw(ctx context.Context, api string, id string, pos IndexPosition) error {
	object := FindObjectFromList(o.list, api)
	if object == nil {
		return ErrNotFoundObject
	}
	var err error
	// 查询出当前index和分组字段值
	one, err := o.mongoFindOneById(ctx, object.Api, id, strings.Join(append(object.IndexGroup, "__index"), ","))
	if err != nil {
		return err
	}
	currentIndex := gconv.Int(one["__index"])
	// 分组筛选
	groupMatchValues := M{}
	if len(object.IndexGroup) > 0 {
		for _, fapi := range object.IndexGroup {
			f := FindFieldFromObject(object, fapi)
			if f == nil {
				return fmt.Errorf(`%s not found index group field %s`, object.Api, fapi)
			}
			groupMatchValues[fapi] = one[fapi]
		}
	}
	// 找到真正的索引位置（位置有分绝对位置和相对位置）
	realIndex, err := o.getRealIndex(ctx, object, groupMatchValues, pos)
	if err != nil {
		return err
	}
	// before 查询
	var before *Var
	if ctx.Value(blockEventsKey) != true {
		before, _, err = o.queryEventObjectEntity(ctx, object, id, nil, IndexMoveBefore)
		if err != nil {
			return err
		}
	}
	// before事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerIndexMoveBefore(ctx, api, id, realIndex, before)
		if err != nil {
			return err
		}
	}
	// 计算add
	add := pos.Dir
	if add == 0 {
		if currentIndex < realIndex {
			add = -1
		} else {
			add = 1
		}
	}
	// 修改数据库 1. 调整后面部分的索引，空出目标位置
	err = o.indexOffset(ctx, object.Api, groupMatchValues, realIndex, add)
	if err != nil {
		return err
	}
	// 修改数据库 2. 修改指定_id行位置修改为目标位置
	_, err = o.mongoUpdateById(ctx, object.Api, id, M{"__index": realIndex})
	if err != nil {
		return err
	}
	// after 查询
	var after *Var
	if ctx.Value(blockEventsKey) != true {
		after, _, err = o.queryEventObjectEntity(ctx, object, id, nil, IndexMoveAfter)
		if err != nil {
			return err
		}
	}
	// after 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerIndexMoveAfter(ctx, api, id, realIndex, after, before)
		if err != nil {
			return err
		}
	}
	// change 事件触发
	if ctx.Value(blockEventsKey) != true {
		err = o.triggerIndexChange(ctx, api, id, after, before, IndexMoveAfter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) getRealIndex(ctx context.Context, object *Object, group M, pos IndexPosition) (int, error) {
	if !pos.Absolute {
		return pos.Index, nil
	}
	list, err := o.mongoFindAllEx(ctx, object.Api, findAllExOptions{
		Fields: []string{"_id", "__index"},
		Sort:   []string{"+__index"},
		Filter: group,
		Skip:   pos.Index - 1,
		Top:    1,
	})
	if err != nil {
		return 0, err
	}
	if len(list) > 0 {
		return gconv.Int(list[0]["__index"]), nil
	} else {
		max, err := o.getMaxIndex(ctx, object, group)
		if err != nil {
			return 0, err
		}
		return max + 1, nil
	}
}

func (o *Objectql) indexOffset(ctx context.Context, table string, group M, index int, add int) error {
	filter := M{}
	for k, v := range group {
		filter[k] = v
	}
	if add < 0 {
		filter["__index"] = M{
			"$lte": index,
		}
	} else {
		filter["__index"] = M{
			"$gte": index,
		}
	}

	_, err := o.mongoUpdateMany(ctx, table, filter, M{
		"$inc": M{
			"__index": add,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (o *Objectql) queryEventObjectEntity(ctx context.Context, object *Object, id string, doc M, position EventPosition) (*Var, bool, error) {
	// event 中需要查询的字段
	qFields := o.getListenQueryFields(ctx, object.Api, position)
	// require, validate 中需要查询的字段
	if position == InsertAfter {
		qFields = append(qFields, o.getObjectRequireQueryFields(object)...)
		qFields = append(qFields, o.getEffectValidateFieldsQuerys(object, doc)...)
		qFields = append(qFields, o.getObjectPrimaryFieldQuerys(object)...)
	}
	if position == UpdateAfter {
		qFields = append(qFields, o.getEffectRequireFieldsQuerys(object, doc)...)
		qFields = append(qFields, o.getEffectValidateFieldsQuerys(object, doc)...)
		qFields = append(qFields, o.getObjectPrimaryFieldQuerys(object)...)
	}
	// updateable 中需要查询的字段
	if position == UpdateBefore || position == UpdateAfter {
		qFields = append(qFields, o.getObjectUpdateableQueryFields(object, doc)...)
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
