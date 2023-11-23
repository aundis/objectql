package objectql

import (
	"time"

	"github.com/gogf/gf/v2/util/gconv"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func NewVar(v any) *Var {
	var cache map[string]any
	if m, ok := v.(map[string]any); ok {
		cache = m
	}
	return &Var{
		v:     v,
		cache: cache,
	}
}

type Var struct {
	v     any
	cache map[string]any
}

func (e *Var) ToInt() int {
	return gconv.Int(e.v)
}

func (e *Var) ToString() string {
	return gconv.String(e.v)
}

func (e *Var) ToBool() bool {
	return gconv.Bool(e.v)
}

func (e *Var) ToFloat32() float32 {
	return gconv.Float32(e.v)
}

func (e *Var) ToFloat64() float64 {
	return gconv.Float64(e.v)
}

func (e *Var) ToTime() time.Time {
	return gconv.Time(e.v)
}

func (e *Var) ToInts() []int {
	return gconv.Ints(e.v)
}

func (e *Var) ToStrings() []string {
	return gconv.Strings(e.v)
}

func (e *Var) ToFloat32s() []float32 {
	return gconv.Float32s(e.v)
}

func (e *Var) ToFloat64s() []float64 {
	return gconv.Float64s(e.v)
}

func (e *Var) ToStrAnyMap() map[string]any {
	return e.cache
}

func (e *Var) ToAny() any {
	return e.v
}

func (e *Var) HasKey(n string) bool {
	if e.cache == nil {
		return false
	}
	_, ok := e.cache[n]
	return ok
}

func (e *Var) HasSomeKey(ns ...string) bool {
	for _, n := range ns {
		if e.HasKey(n) {
			return true
		}
	}
	return false
}

func (e *Var) HasKeys(ns ...string) bool {
	for _, n := range ns {
		if !e.HasKey(n) {
			return false
		}
	}
	return true
}

func (e *Var) Int(n string) int {
	return gconv.Int(e.mapValue(n))
}

func (e *Var) String(n string) string {
	v := e.mapValue(n)
	switch r := v.(type) {
	case primitive.ObjectID:
		return r.Hex()
	case *primitive.ObjectID:
		return r.Hex()
	default:
		return gconv.String(e.mapValue(n))
	}
}

func (e *Var) Bool(n string) bool {
	return gconv.Bool(e.mapValue(n))
}

func (e *Var) Float32(n string) float32 {
	return gconv.Float32(e.mapValue(n))
}

func (e *Var) Float64(n string) float64 {
	return gconv.Float64(e.mapValue(n))
}

func (e *Var) Time(n string) time.Time {
	return gconv.Time(e.mapValue(n))
}

func (e *Var) Ints(n string) []int {
	return gconv.Ints(e.mapValue(n))
}

func (e *Var) Strings(n string) []string {
	return gconv.Strings(e.mapValue(n))
}

func (e *Var) Float32s(n string) []float32 {
	return gconv.Float32s(e.mapValue(n))
}

func (e *Var) Float64s(n string) []float64 {
	return gconv.Float64s(e.mapValue(n))
}

func (e *Var) Var(n string) *Var {
	return NewVar(e.mapValue(n))
}

func (e *Var) IsNil() bool {
	return isNull(e.v)
}

func (e *Var) mapValue(k string) any {
	if e.cache != nil {
		return e.cache[k]
	}
	return nil
}

func VarsToAnys(arr []*Var) []any {
	var result []any
	for _, item := range arr {
		result = append(result, item.ToAny())
	}
	return result
}

// func ValuesTo RawArrayToEntityArray(arr []map[string]any) []Entity {
// 	var result []Entity
// 	for _, item := range arr {
// 		result = append(result, Entity{v: item})
// 	}
// 	return result
// }
