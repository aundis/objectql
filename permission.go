package objectql

import (
	"context"
	"fmt"
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

var rootPermissionKey = "objectql_rootPermissionKey"

func (o *Objectql) WithRootPermission(ctx context.Context) context.Context {
	return context.WithValue(ctx, rootPermissionKey, true)
}

func (o *Objectql) IsRootPermission(ctx context.Context) bool {
	return ctx.Value(rootPermissionKey) == true
}
