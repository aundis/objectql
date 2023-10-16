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

type ObjectPermissionCheckHandler func(ctx context.Context, object string, kind PermissionKind) bool
type ObjectFieldPermissionCheckHandler func(ctx context.Context, object string, field string, kind PermissionKind) bool
type ObjectHandlePermissionCheckHandler func(ctx context.Context, object string, name string) bool

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
	if o.objectPermissionCheckHandler != nil {
		if !o.objectPermissionCheckHandler(ctx, object, ObjectInsert) {
			return fmt.Errorf("not object %s permission(%v)", object, kind)
		}
	}
	return nil
}

func (o *Objectql) checkObjectFieldPermissionWithDocument(ctx context.Context, object *Object, doc map[string]interface{}, kind PermissionKind) error {
	if o.objectFieldPermissionCheckHandler != nil {
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
	if o.objectFieldPermissionCheckHandler != nil {
		if !o.objectFieldPermissionCheckHandler(ctx, object, field, kind) {
			return fmt.Errorf("not field %s.%s permission(%v)", object, field, kind)
		}
	}
	return nil
}
