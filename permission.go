package objectql

import (
	"context"
	"fmt"

	"github.com/gogf/gf/v2/text/gstr"
)

type PermissionKind int

const (
	ObjectInsert PermissionKind = iota
	ObjectUpdate
	ObjectDelete
	ObjectQuery
	FieldQuery
	FieldUpdate
)

type ObjectPermissionCheckHandler func(ctx context.Context, object string, kind PermissionKind) (bool, error)
type ObjectFieldPermissionCheckHandler func(ctx context.Context, object string, field string, kind PermissionKind) (bool, error)
type ObjectHandlePermissionCheckHandler func(ctx context.Context, object string, name string) (bool, error)

func (o *Objectql) SetObjectPermissionCheckHandler(fn ObjectPermissionCheckHandler) {
	o.objectPermissionCheckHandler = fn
}

func (o *Objectql) SetObjectFieldPermissionCheckHandler(fn ObjectFieldPermissionCheckHandler) {
	o.objectFieldPermissionCheckHandler = fn
}

func (o *Objectql) SetObjectHandlePermissionCheckHandler(fn ObjectHandlePermissionCheckHandler) {
	o.objectHandlePermissionCheckHandler = fn
}

func (o *Objectql) checkObjectPermission(ctx context.Context, object string, kind PermissionKind) error {
	if o.objectPermissionCheckHandler != nil && !o.IsRootPermission(ctx) {
		has, err := o.objectPermissionCheckHandler(ctx, object, kind)
		if err != nil {
			return err
		}
		if !has {
			return fmt.Errorf("not object %s permission(%v)", object, kind)
		}
	}
	return nil
}

func (o *Objectql) checkObjectFieldPermissionWithDocument(ctx context.Context, object *Object, doc map[string]interface{}, kind PermissionKind) error {
	if o.objectFieldPermissionCheckHandler != nil && !o.IsRootPermission(ctx) {
		for _, field := range object.Fields {
			if _, ok := doc[field.Api]; ok {
				err := o.checkObjectFieldPermission(ctx, object.Api, field.Api, kind)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (o *Objectql) checkObjectFieldPermission(ctx context.Context, object string, field string, kind PermissionKind) error {
	if o.objectFieldPermissionCheckHandler != nil && !o.IsRootPermission(ctx) {
		has, err := o.objectFieldPermissionCheckHandler(ctx, object, field, kind)
		if err != nil {
			return err
		}
		if !has {
			return fmt.Errorf("not field %s.%s permission(%v)", object, field, kind)
		}
	}
	return nil
}

func (o *Objectql) hasObjectFieldPermission(ctx context.Context, object string, field string, kind PermissionKind) (bool, error) {
	if o.objectFieldPermissionCheckHandler != nil && !o.IsRootPermission(ctx) {
		if field == "_id" {
			return true, nil
		}
		if gstr.HasSuffix(field, "__expand") {
			field = gstr.Replace(field, "__expand", "")
		}
		if gstr.HasSuffix(field, "__expands") {
			field = gstr.Replace(field, "__expands", "")
		}
		return o.objectFieldPermissionCheckHandler(ctx, object, field, kind)
	}
	return true, nil
}

func (o *Objectql) checkObjectHandlePermission(ctx context.Context, object string, name string) error {
	if o.objectHandlePermissionCheckHandler != nil && !o.IsRootPermission(ctx) {
		has, err := o.objectHandlePermissionCheckHandler(ctx, object, name)
		if err != nil {
			return err
		}
		if !has {
			return fmt.Errorf("not handle %s.%s permission", object, name)
		}
	}
	return nil
}

type rootPermissionKeyType string

var rootPermissionKey rootPermissionKeyType = "objectql_rootPermissionKey"

func (o *Objectql) WithRootPermission(ctx context.Context) context.Context {
	return context.WithValue(ctx, rootPermissionKey, true)
}

func (o *Objectql) RemoveRootPermission(ctx context.Context) context.Context {
	return context.WithValue(ctx, rootPermissionKey, false)
}

func (o *Objectql) IsRootPermission(ctx context.Context) bool {
	return ctx.Value(rootPermissionKey) == true
}
