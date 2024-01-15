package objectql

import (
	"context"
)

type IndexMoveBeforeHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, toIndex int, cur *Var) error
}

type IndexMoveAfterHandler struct {
	Fields []string
	Handle func(ctx context.Context, id string, toIndex int, cur *Var, before *Var) error
}

type IndexChangeHandler struct {
	Fields   []string
	Position EventPosition
	Handle   func(ctx context.Context, id string, cur *Var, before *Var) error
}

func (o *Objectql) ListenIndexMoveBefore(table string, handle *IndexMoveBeforeHandler) {
	o.listen(table, kIndexMoveBefore, handle)
}

func (o *Objectql) ListenIndexMoveAfter(table string, handle *IndexMoveAfterHandler) {
	o.listen(table, kIndexMoveAfter, handle)
}

func (o *Objectql) ListenIndexChange(table string, handle *IndexChangeHandler) {
	if handle.Position == 0 {
		handle.Position = InsertFull | DeleteFull | MoveFull
	}
	o.listen(table, kIndexChange, handle)
}

func (o *Objectql) UnListenIndexMoveBefore(table string, handle *IndexMoveBeforeHandler) {
	o.unListen(table, kIndexMoveBefore, handle)
}

func (o *Objectql) UnListenIndexMoveAfter(table string, handle *IndexMoveAfterHandler) {
	o.unListen(table, kIndexMoveAfter, handle)
}

func (o *Objectql) UnListenIndexChange(table string, handle *IndexChangeHandler) {
	o.unListen(table, kIndexChange, handle)
}

func (o *Objectql) triggerIndexMoveBefore(ctx context.Context, table string, id string, toIndex int, cur *Var) error {
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kIndexMoveBefore) {
		err := handle.(IndexMoveBeforeHandler).Handle(ctx, id, toIndex, cur)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerIndexMoveAfter(ctx context.Context, table string, id string, toIndex int, cur *Var, before *Var) error {
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kIndexMoveAfter) {
		err := handle.(IndexMoveAfterHandler).Handle(ctx, id, toIndex, cur, before)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Objectql) triggerIndexChange(ctx context.Context, table string, id string, before *Var, after *Var, position EventPosition) error {
	ctx = o.WithRootPermission(ctx)
	ctx = o.WithRootPermission(ctx)
	for _, handle := range o.getEventHanders(ctx, table, kIndexChange) {
		ins := handle.(*IndexChangeHandler)
		if ins.Position&position == 0 {
			continue
		}
		err := ins.Handle(ctx, id, after, before)
		if err != nil {
			return err
		}
	}
	return nil
}
