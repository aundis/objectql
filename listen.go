package objectql

import (
	"context"

	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/container/gmap"
)

type EventKind int

const (
	InsertBefore EventKind = iota
	InsertAfter
	UpdateBefore
	UpdateAfter
	DeleteBefore
	DeleteAfter

	// EX
	InsertAfterEx
	UpdateBeforeEx
	UpdateAfterEx
	DeleteBeforeEx
	DeleteAfterEx
)

type InsertBeforeHandler = func(ctx context.Context, doc *Var) error
type InsertAfterHandler = func(ctx context.Context, id string, doc *Var) error
type UpdateBeoferHandler = func(ctx context.Context, id string, doc *Var) error
type UpdateAfterHandler = func(ctx context.Context, id string, doc *Var) error
type DeleteBeforeHandler = func(ctx context.Context, id string) error
type DeleteAfterHandler = func(ctx context.Context, id string) error

func (o *Objectql) ListenInsertBefore(table string, fn InsertBeforeHandler) {
	o.listen(table, InsertBefore, fn)
}

func (o *Objectql) ListenInsertAfter(table string, fn InsertAfterHandler) {
	o.listen(table, InsertAfter, fn)
}

func (o *Objectql) ListenUpdateBefore(table string, fn UpdateBeoferHandler) {
	o.listen(table, UpdateBefore, fn)
}

func (o *Objectql) ListenUpdateAfter(table string, fn UpdateAfterHandler) {
	o.listen(table, UpdateAfter, fn)
}

func (o *Objectql) ListenDeleteBefore(table string, fn DeleteBeforeHandler) {
	o.listen(table, DeleteBefore, fn)
}

func (o *Objectql) ListenDeleteAfter(table string, fn DeleteAfterHandler) {
	o.listen(table, DeleteAfter, fn)
}

func (o *Objectql) listen(table string, kind EventKind, value any) {
	if !o.eventMap.Contains(table) {
		o.eventMap.Set(table, gmap.NewIntAnyMap(true))
	}
	handleMap := o.eventMap.Get(table).(*gmap.IntAnyMap)
	if !handleMap.Contains(int(kind)) {
		handleMap.Set(int(kind), garray.NewArray(true))
	}
	array := handleMap.Get(int(kind)).(*garray.Array)
	array.Append(value)
}

func (o *Objectql) UnListenInsertBefore(table string, fn InsertBeforeHandler) {
	o.unListen(table, InsertBefore, fn)
}

func (o *Objectql) UnListenInsertAfter(table string, fn InsertAfterHandler) {
	o.unListen(table, InsertAfter, fn)
}

func (o *Objectql) UnListenUpdateBefore(table string, fn UpdateBeoferHandler) {
	o.unListen(table, UpdateBefore, fn)
}

func (o *Objectql) UnListenUpdateAfter(table string, fn UpdateAfterHandler) {
	o.unListen(table, UpdateAfter, fn)
}

func (o *Objectql) UnListenDeleteBefore(table string, fn DeleteBeforeHandler) {
	o.unListen(table, DeleteBefore, fn)
}

func (o *Objectql) UnListenDeleteAfter(table string, fn DeleteAfterHandler) {
	o.unListen(table, DeleteAfter, fn)
}

func (o *Objectql) unListen(table string, kind EventKind, value any) {
	if !o.eventMap.Contains(table) {
		return
	}
	handleMap := o.eventMap.Get(table).(*gmap.IntAnyMap)
	if !handleMap.Contains(int(kind)) {
		return
	}
	array := handleMap.Get(int(kind)).(*garray.Array)
	array.RemoveValue(value)
}

func (o *Objectql) triggerInsertBefore(ctx context.Context, table string, doc *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, InsertBefore) {
		err := handle.(InsertBeforeHandler)(ctx, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerInsertAfter(ctx context.Context, table string, id string, doc *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, InsertAfter) {
		err := handle.(InsertAfterHandler)(ctx, id, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerUpdateBefore(ctx context.Context, table string, id string, doc *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, UpdateBefore) {
		err := handle.(UpdateBeoferHandler)(ctx, id, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerUpdateAfter(ctx context.Context, table string, id string, doc *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, UpdateAfter) {
		err := handle.(UpdateAfterHandler)(ctx, id, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerDeleteBefore(ctx context.Context, table string, id string) error {
	for _, handle := range o.getEventHanders(ctx, table, DeleteBefore) {
		err := handle.(DeleteBeforeHandler)(ctx, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerDeleteAfter(ctx context.Context, table string, id string) error {
	for _, handle := range o.getEventHanders(ctx, table, DeleteAfter) {
		err := handle.(DeleteAfterHandler)(ctx, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) getEventHanders(ctx context.Context, table string, kind EventKind) []interface{} {
	if !o.eventMap.Contains(table) {
		return nil
	}
	handleMap := o.eventMap.Get(table).(*gmap.IntAnyMap)
	if !handleMap.Contains(int(kind)) {
		return nil
	}
	array := handleMap.Get(int(kind)).(*garray.Array)
	return array.Slice()
}
