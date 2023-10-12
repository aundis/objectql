package objectql

import (
	"fmt"
	"reflect"
)

func (o *Objectql) bindObjectMethod(object *Object, v any) error {
	if v == nil {
		return nil
	}
	rt := unPointerType(reflect.TypeOf(v)).Kind()
	if rt != reflect.Struct {
		return fmt.Errorf("object '%s' bind value must is struct", object.Api)
	}
	if err := o.bindObjectListen(object, v); err != nil {
		return err
	}
	if err := o.bindObjectQueryOrMutation(object, v); err != nil {
		return err
	}
	return nil
}

func (o *Objectql) bindObjectListen(object *Object, v any) error {
	rt := reflect.TypeOf(v)
	rv := reflect.ValueOf(v)
	for i := 0; i < rt.NumMethod(); i++ {
		method := rt.Method(i)
		switch method.Name {
		case "InsertBefore":
			mv := rv.MethodByName(method.Name)
			if fun, ok := mv.Interface().(InsertBeforeHandler); ok {
				o.ListenInsertBefore(object.Api, fun)
			} else {
				return fmt.Errorf("bind object '%s' InsertBefore error method signature %T", object.Api, mv)
			}
		case "InsertAfter":
			mv := rv.MethodByName(method.Name)
			if fun, ok := mv.Interface().(InsertAfterHandler); ok {
				o.ListenInsertAfter(object.Api, fun)
			} else {
				return fmt.Errorf("bind object '%s' InsertAfter error method signature %T", object.Api, mv)
			}
		case "UpdateBefore":
			mv := rv.MethodByName(method.Name)
			if fun, ok := mv.Interface().(UpdateBeoferHandler); ok {
				o.ListenUpdateBefore(object.Api, fun)
			} else {
				return fmt.Errorf("bind object '%s' UpdateBefore error method signature %T", object.Api, mv.Interface())
			}
		case "UpdateAfter":
			mv := rv.MethodByName(method.Name)
			if fun, ok := mv.Interface().(UpdateAfterHandler); ok {
				o.ListenUpdateAfter(object.Api, fun)
			} else {
				return fmt.Errorf("bind object '%s' UpdateAfter error method signature %T", object.Api, mv.Interface())
			}

		case "DeleteBefore":
			mv := rv.MethodByName(method.Name)
			if fun, ok := mv.Interface().(DeleteBeforeHandler); ok {
				o.ListenDeleteBefore(object.Api, fun)
			} else {
				return fmt.Errorf("bind object '%s' DeleteBefore error method signature %T", object.Api, mv.Interface())
			}
		case "DeleteAfter":
			mv := rv.MethodByName(method.Name)
			if fun, ok := mv.Interface().(DeleteAfterHandler); ok {
				o.ListenDeleteAfter(object.Api, fun)
			} else {
				return fmt.Errorf("bind object '%s' DeleteAfter error method signature %T", object.Api, mv.Interface())
			}
		}
	}
	return nil
}

func (o *Objectql) bindObjectQueryOrMutation(object *Object, v any) error {
	rt := reflect.TypeOf(v)
	rv := reflect.ValueOf(v)
	for i := 0; i < rt.NumMethod(); i++ {
		method := rt.Method(i)
		mv := rv.Method(i).Interface()
		if tag, ok := parseQueryOrMutationMethod(reflect.TypeOf(mv)); ok {
			switch tag.Get("kind") {
			case "query":
				mv := rv.MethodByName(method.Name)
				object.Querys = append(object.Querys, &Handle{
					Name:    tag.Get("name"),
					Api:     firstLower(method.Name),
					Comment: tag.Get("comment"),
					Resolve: mv.Interface(),
				})
			case "mutation":
				mv := rv.MethodByName(method.Name)
				object.Mutations = append(object.Mutations, &Handle{
					Name:    tag.Get("name"),
					Api:     firstLower(method.Name),
					Comment: tag.Get("comment"),
					Resolve: mv.Interface(),
				})
			default:
				return fmt.Errorf("bind object '%s' query or mutation error, not found kind %s", object.Api, tag.Get("kind"))
			}
		}
	}
	return nil
}

func parseQueryOrMutationMethod(method reflect.Type) (reflect.StructTag, bool) {
	if method.NumIn() < 2 {
		return "", false
	}
	if method.NumOut() < 2 {
		return "", false
	}
	reqType := unPointerType(method.In(1))
	if reqType.Kind() != reflect.Struct {
		return "", false
	}
	field, ok := reqType.FieldByName("Meta")
	if !ok {
		return "", false
	}
	return field.Tag, true
}
