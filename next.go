package objectql

import (
	"context"

	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gctx"
)

const nextArrayContextKey = "objectql_next_array_key"
const nextMapContextKey = "objectql_next_map_key"

type nextHandle struct {
	async bool
	fn    func(context.Context) error
}

func (o *Objectql) withNextContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, nextArrayContextKey, garray.New(true))
	ctx = context.WithValue(ctx, nextMapContextKey, gmap.NewStrAnyMap(true))
	return ctx
}

func (o *Objectql) Next(ctx context.Context, fn func(context.Context) error, keys ...string) {
	o.appendNextQueue(ctx, fn, false, keys...)
}

func (o *Objectql) AsyncNext(ctx context.Context, fn func(context.Context) error, keys ...string) {
	o.appendNextQueue(ctx, fn, true, keys...)
}

func (o *Objectql) appendNextQueue(ctx context.Context, fn func(context.Context) error, async bool, keys ...string) {
	if len(keys) > 0 {
		// 同key只执行最后设定的fn
		m, ok := ctx.Value(nextArrayContextKey).(*gmap.StrAnyMap)
		if !ok {
			return
		}
		m.Set(keys[0], &nextHandle{
			async: async,
			fn:    fn,
		})
	} else {
		// 按顺序添加执行
		arr, ok := ctx.Value(nextArrayContextKey).(*garray.Array)
		if !ok {
			return
		}
		arr.Append(&nextHandle{
			async: async,
			fn:    fn,
		})
	}
}

func (o *Objectql) runNextHandles(ctx context.Context) error {
	narr := ctx.Value(nextArrayContextKey).(*garray.Array)
	nmap := ctx.Value(nextMapContextKey).(*gmap.StrAnyMap)
	nextHandles := o.getNextHandles(narr, nmap, false)
	for _, handle := range nextHandles {
		err := handle.fn(ctx)
		if err != nil {
			return err
		}
	}
	o.runAsyncNextHandles(o.getNextHandles(narr, nmap, true))
	return nil
}

func (o *Objectql) runAsyncNextHandles(handles []*nextHandle) {
	go func() {
		ctx := gctx.New()
		for _, handle := range handles {
			err := handle.fn(ctx)
			if err != nil {
				g.Log().Error(ctx, "run async next handle error:", err)
			}
		}
	}()
}

func (o *Objectql) getNextHandles(arr *garray.Array, nmap *gmap.StrAnyMap, async bool) []*nextHandle {
	var result []*nextHandle
	arr.LockFunc(func(array []interface{}) {
		for _, v := range array {
			if v.(*nextHandle).async == async {
				result = append(result, v.(*nextHandle))
			}
		}
	})
	nmap.LockFunc(func(m map[string]interface{}) {
		for _, v := range m {
			if v.(*nextHandle).async == async {
				result = append(result, v.(*nextHandle))
			}
		}
	})
	return result
}
