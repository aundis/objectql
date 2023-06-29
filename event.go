package main

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
)

type InsertBeforeHandler func(ctx context.Context, doc map[string]interface{}) error
type InsertAfterHandler func(ctx context.Context, id string) error
type UpdateBeoferHandler func(ctx context.Context, id string, doc map[string]interface{}) error
type UpdateAfterHandler func(ctx context.Context, id string) error
type DeleteBeforeHandler func(ctx context.Context, id string) error
type DeleteAfterHandler func(ctx context.Context, id string) error

func (o *Objectql) ListenInsertBefore(table string, fn InsertBeforeHandler) {
	o.Listen(table, InsertBefore, fn)
}

func (o *Objectql) ListenInsertAfter(table string, fn InsertAfterHandler) {
	o.Listen(table, InsertAfter, fn)
}

func (o *Objectql) ListenUpdateBefore(table string, fn UpdateBeoferHandler) {
	o.Listen(table, UpdateBefore, fn)
}

func (o *Objectql) ListenUpdateAfter(table string, fn UpdateAfterHandler) {
	o.Listen(table, UpdateAfter, fn)
}

func (o *Objectql) ListenDeleteBefore(table string, fn DeleteBeforeHandler) {
	o.Listen(table, DeleteBefore, fn)
}

func (o *Objectql) ListenDeleteAfter(table string, fn DeleteAfterHandler) {
	o.Listen(table, DeleteAfter, fn)
}

func (o *Objectql) Listen(table string, kind EventKind, fn interface{}) {
	if !o.eventMap.Contains(table) {
		o.eventMap.Set(table, gmap.NewIntAnyMap(true))
	}
	handleMap := o.eventMap.Get(table).(*gmap.IntAnyMap)
	if !handleMap.Contains(int(kind)) {
		handleMap.Set(int(kind), garray.NewArray(true))
	}
	array := handleMap.Get(int(kind)).(*garray.Array)
	array.Append(fn)
}

func (o *Objectql) UnListen(table string, kind EventKind, fn interface{}) {
	if !o.eventMap.Contains(table) {
		return
	}
	handleMap := o.eventMap.Get(table).(*gmap.IntAnyMap)
	if !handleMap.Contains(int(kind)) {
		return
	}
	array := handleMap.Get(int(kind)).(*garray.Array)
	array.RemoveValue(fn)
}

func (o *Objectql) triggerInsertBefore(ctx context.Context, table string, doc map[string]interface{}) error {
	for _, handle := range o.getEventHanders(ctx, table, InsertBefore) {
		err := handle.(InsertBeforeHandler)(ctx, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerInsertAfter(ctx context.Context, table string, id string) error {
	for _, handle := range o.getEventHanders(ctx, table, InsertBefore) {
		err := handle.(InsertAfterHandler)(ctx, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerUpdateBefore(ctx context.Context, table string, id string, doc map[string]interface{}) error {
	for _, handle := range o.getEventHanders(ctx, table, InsertBefore) {
		err := handle.(UpdateBeoferHandler)(ctx, id, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerUpdateAfter(ctx context.Context, table string, id string) error {
	for _, handle := range o.getEventHanders(ctx, table, InsertBefore) {
		err := handle.(UpdateAfterHandler)(ctx, id)
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
	for _, handle := range o.getEventHanders(ctx, table, InsertBefore) {
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
