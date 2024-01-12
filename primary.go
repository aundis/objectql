package objectql

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

func (o *Objectql) checkInsertPrimaryFieldRequires(object *Object, doc M) error {
	for _, f := range object.Fields {
		if f.Primary {
			if v, ok := doc[f.Api]; !ok || isNull(v) {
				return fmt.Errorf("object %s primary field %s is require", object.Api, f.Api)
			}
		}
	}
	return nil
}

func (o *Objectql) checkUpdatePrimaryFieldBoolRequires(object *Object, doc M) error {
	for _, f := range object.Fields {
		if f.Primary {
			if v, ok := doc[f.Api]; ok && isNull(v) {
				return fmt.Errorf("object %s primary field %s is require", object.Api, f.Api)
			}
		}
	}
	return nil
}

func (o *Objectql) checkPrimaryDuplicate(ctx context.Context, object *Object, after *Var) error {
	if !o.hasPrimaryField(object) {
		return nil
	}

	filter := bson.M{}
	for _, f := range object.Fields {
		if f.Primary {
			filter[f.Api] = o.toMongoFilterValue(f, after.Any(f.Api))
		}
	}

	count, err := o.mongoCount(ctx, object.Api, filter)
	if err != nil {
		return err
	}
	if count > 1 {
		return fmt.Errorf("object %s primary duplicate", object.Api)
	}
	return nil
}

func (o *Objectql) toMongoFilterValue(field *Field, v interface{}) interface{} {
	switch field.Type.(type) {
	case *RelateType:
		return M{
			"$toId": v,
		}
	case *DateTimeType, *DateType, *TimeType:
		return M{
			"$toDate": v,
		}
	default:
		return v

	}
}

func (o *Objectql) getObjectPrimaryFieldQuerys(object *Object) []string {
	var result []string
	for _, f := range object.Fields {
		if f.Primary {
			result = append(result, f.Api)
		}
	}
	return result
}

func (o *Objectql) hasPrimaryField(object *Object) bool {
	if object.hasPrimary != nil {
		return object.hasPrimary.(bool)
	}
	for _, f := range object.Fields {
		if f.Primary {
			object.hasPrimary = true
			return true
		}
	}
	object.hasPrimary = false
	return false
}
