package objectql

import (
	"context"
)

// EX
type InsertAfterExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, doc *Var, entity *Var) error
}

type UpdateBeforeExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, doc *Var, entity *Var) error
}

type UpdateAfterExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, doc *Var, entity *Var) error
}

type DeleteBeforeExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, entity *Var) error
}

type DeleteAfterExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, entity *Var) error
}

// EX

func (o *Objectql) ListenInsertAfterEx(table string, handle *InsertAfterExHandler) {
	o.listen(table, InsertAfterEx, handle)
}

func (o *Objectql) ListenUpdateBeforeEx(table string, handle *UpdateBeforeExHandler) {
	o.listen(table, UpdateBeforeEx, handle)
}

func (o *Objectql) ListenUpdateAfterEx(table string, handle *UpdateAfterExHandler) {
	o.listen(table, UpdateAfterEx, handle)
}

func (o *Objectql) ListenDeleteBeforeEx(table string, handle *DeleteBeforeExHandler) {
	o.listen(table, DeleteBeforeEx, handle)
}

func (o *Objectql) ListenDeleteAfterEx(table string, handle *DeleteAfterExHandler) {
	o.listen(table, DeleteAfterEx, handle)
}

func (o *Objectql) UnListenInsertAfterEx(table string, handle *InsertAfterExHandler) {
	o.unListen(table, InsertAfterEx, handle)
}

func (o *Objectql) UnListenUpdateBeforeEx(table string, handle *UpdateBeforeExHandler) {
	o.unListen(table, UpdateBeforeEx, handle)
}

func (o *Objectql) UnListenUpdateAfterEx(table string, handle *UpdateAfterExHandler) {
	o.unListen(table, UpdateAfterEx, handle)
}

func (o *Objectql) UnListenDeleteBeforeEx(table string, handle *DeleteBeforeExHandler) {
	o.unListen(table, DeleteBeforeEx, handle)
}

func (o *Objectql) UnListenDeleteAfterEx(table string, handle *DeleteAfterExHandler) {
	o.unListen(table, DeleteAfterEx, handle)
}

// TRIGGER

func (o *Objectql) triggerInsertAfterEx(ctx context.Context, table string, id string, doc *Var, entity *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, InsertAfterEx) {
		ins := handle.(*InsertAfterExHandler)
		return ins.Handle(ctx, id, doc, entity)
	}
	return nil
}

func (o *Objectql) triggerUpdateBeforeEx(ctx context.Context, table string, id string, doc *Var, entity *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, UpdateBeforeEx) {
		ins := handle.(*UpdateBeforeExHandler)
		return ins.Handle(ctx, id, doc, entity)
	}
	return nil
}

func (o *Objectql) triggerUpdateAfterEx(ctx context.Context, table string, id string, doc *Var, entity *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, UpdateAfterEx) {
		ins := handle.(*UpdateAfterExHandler)
		return ins.Handle(ctx, id, doc, entity)
	}
	return nil
}

func (o *Objectql) triggerDeleteBeforeEx(ctx context.Context, table string, id string, entity *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, DeleteBeforeEx) {
		ins := handle.(*DeleteBeforeExHandler)
		return ins.Handle(ctx, id, entity)
	}
	return nil
}

func (o *Objectql) triggerDeleteAfterEx(ctx context.Context, table string, id string, entity *Var) error {
	for _, handle := range o.getEventHanders(ctx, table, DeleteAfterEx) {
		ins := handle.(*DeleteAfterExHandler)
		return ins.Handle(ctx, id, entity)
	}
	return nil
}

func (o *Objectql) getListenQueryFields(ctx context.Context, table string, kinds ...EventKind) []string {
	var result []string
	for _, kind := range kinds {
		for _, handle := range o.getEventHanders(ctx, table, kind) {
			switch n := handle.(type) {
			case *InsertAfterExHandler:
				result = append(result, n.Fields...)
			case *UpdateBeforeExHandler:
				result = append(result, n.Fields...)
			case *UpdateAfterExHandler:
				result = append(result, n.Fields...)
			case *DeleteBeforeExHandler:
				result = append(result, n.Fields...)
			case *DeleteAfterExHandler:
				result = append(result, n.Fields...)
			}
		}
	}
	return result
}
