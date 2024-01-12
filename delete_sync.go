package objectql

import (
	"context"
	"fmt"
)

func (o *Objectql) initObjectDeleteSync(object *Object) error {
	for _, f := range object.Fields {
		if f.DeleteSync {
			if !IsRelateType(f.Type) {
				return fmt.Errorf("%s.%s deleteSync options can only be used on relate type field", object.Api, f.Api)
			}

			fapi := f.Api
			relateType := f.Type.(*RelateType)
			o.ListenDeleteAfter(relateType.ObjectApi, func(ctx context.Context, id string) error {
				return o.Delete(ctx, object.Api, DeleteOptions{
					Filter: M{
						fapi: M{
							"$toId": id,
						},
					},
				})
			})
		}
	}
	return nil
}
