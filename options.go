package objectql

import (
	"go.mongodb.org/mongo-driver/bson"
)

type Fields = []interface{}

type InsertOptions struct {
	Doc    bson.M
	Fields Fields
}

type UpdateOptions struct {
	Condition bson.M
	Doc       bson.M
	Fields    Fields
}

type UpdateByIdOptions struct {
	Doc    bson.M
	Fields Fields
}

type FindListOptions struct {
	Condition bson.M
	Top       int
	Skip      int
	Sort      []string
	Fields    Fields
}

type FindOneOptions struct {
	Condition bson.M
	Top       int
	Skip      int
	Sort      []string
	Fields    Fields
}
