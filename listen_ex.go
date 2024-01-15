package objectql

import (
	"context"
)

// EX
type InsertAfterExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, doc *Var, cur *Var) error
}

type UpdateBeforeExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, doc *Var, cur *Var) error
}

type UpdateAfterExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, doc *Var, cur *Var) error
}

type DeleteBeforeExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, cur *Var) error
}

type DeleteAfterExHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, cur *Var) error
}

// EX

func (o *Objectql) ListenInsertAfterEx(table string, handle *InsertAfterExHandler) {
	o.listen(table, kInsertAfterEx, handle)
}

func (o *Objectql) ListenUpdateBeforeEx(table string, handle *UpdateBeforeExHandler) {
	o.listen(table, kUpdateBeforeEx, handle)
}

func (o *Objectql) ListenUpdateAfterEx(table string, handle *UpdateAfterExHandler) {
	o.listen(table, kUpdateAfterEx, handle)
}

func (o *Objectql) ListenDeleteBeforeEx(table string, handle *DeleteBeforeExHandler) {
	o.listen(table, kDeleteBeforeEx, handle)
}

func (o *Objectql) ListenDeleteAfterEx(table string, handle *DeleteAfterExHandler) {
	o.listen(table, kDeleteAfterEx, handle)
}

func (o *Objectql) UnListenInsertAfterEx(table string, handle *InsertAfterExHandler) {
	o.unListen(table, kInsertAfterEx, handle)
}

func (o *Objectql) UnListenUpdateBeforeEx(table string, handle *UpdateBeforeExHandler) {
	o.unListen(table, kUpdateBeforeEx, handle)
}

func (o *Objectql) UnListenUpdateAfterEx(table string, handle *UpdateAfterExHandler) {
	o.unListen(table, kUpdateAfterEx, handle)
}

func (o *Objectql) UnListenDeleteBeforeEx(table string, handle *DeleteBeforeExHandler) {
	o.unListen(table, kDeleteBeforeEx, handle)
}

func (o *Objectql) UnListenDeleteAfterEx(table string, handle *DeleteAfterExHandler) {
	o.unListen(table, kDeleteAfterEx, handle)
}

// TRIGGER

func (o *Objectql) triggerInsertAfterEx(ctx context.Context, table string, id string, doc *Var, entity *Var) error {
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kInsertAfterEx) {
		ins := handle.(*InsertAfterExHandler)
		return ins.Handle(ctx, id, doc, entity)
	}
	return nil
}

func (o *Objectql) triggerUpdateBeforeEx(ctx context.Context, table string, id string, doc *Var, entity *Var) error {
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kUpdateBeforeEx) {
		ins := handle.(*UpdateBeforeExHandler)
		return ins.Handle(ctx, id, doc, entity)
	}
	return nil
}

func (o *Objectql) triggerUpdateAfterEx(ctx context.Context, table string, id string, doc *Var, entity *Var) error {
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kUpdateAfterEx) {
		ins := handle.(*UpdateAfterExHandler)
		return ins.Handle(ctx, id, doc, entity)
	}
	return nil
}

func (o *Objectql) triggerDeleteBeforeEx(ctx context.Context, table string, id string, entity *Var) error {
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kDeleteBeforeEx) {
		ins := handle.(*DeleteBeforeExHandler)
		return ins.Handle(ctx, id, entity)
	}
	return nil
}

func (o *Objectql) triggerDeleteAfterEx(ctx context.Context, table string, id string, entity *Var) error {
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kDeleteAfterEx) {
		ins := handle.(*DeleteAfterExHandler)
		return ins.Handle(ctx, id, entity)
	}
	return nil
}

func (o *Objectql) getListenQueryFields(ctx context.Context, table string, position EventPosition) []string {
	var result []string
	var kinds []eventKind
	switch position {
	// case InsertBefore:
	case InsertAfter:
		kinds = []eventKind{kInsertAfterEx, kFieldChange, kIndexChange}
	case UpdateBefore:
		kinds = []eventKind{kUpdateBeforeEx, kFieldChange}
	case UpdateAfter:
		kinds = []eventKind{kUpdateAfterEx, kFieldChange}
	case DeleteBefore:
		kinds = []eventKind{kDeleteBeforeEx, kDeleteAfterEx, kFieldChange, kIndexChange}
		// case DeleteAfter:
	case IndexMoveBefore:
		kinds = []eventKind{kIndexMoveBefore, kFieldChange}
	case IndexMoveAfter:
		kinds = []eventKind{kIndexMoveAfter, kFieldChange}
	}

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
			case *IndexMoveBeforeHandler:
				result = append(result, n.Fields...)
			case *IndexMoveAfterHandler:
				result = append(result, n.Fields...)
			case *ListenChangeHandler:
				if n.Position&position != 0 {
					result = append(result, n.Query...)
					result = append(result, n.Listen...)
				}
			case *IndexChangeHandler:
				if n.Position&position != 0 {
					result = append(result, n.Fields...)
				}
			}
		}
	}
	return result
}
