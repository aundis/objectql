package objectql

import "context"

type ListenChangeHandler struct {
	Object string
	Listen []string
	Fields []string
	Handle func(ctx context.Context, change map[string]bool, entity *Var, before *Var) error
}
