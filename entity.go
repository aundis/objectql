package objectql

import (
	"time"

	"github.com/gogf/gf/v2/util/gconv"
)

type Entity struct {
	v map[string]any
}

func (e Entity) Int(n string) int {
	return gconv.Int(e.v[n])
}

func (e Entity) String(n string) string {
	return gconv.String(e.v[n])
}

func (e Entity) Bool(n string) bool {
	return gconv.Bool(e.v[n])
}

func (e Entity) Float32(n string) float32 {
	return gconv.Float32(e.v[n])
}

func (e Entity) Float64(n string) float64 {
	return gconv.Float64(e.v[n])
}

func (e Entity) Time(n string) time.Time {
	return gconv.Time(e.v[n])
}

func (e Entity) Ints(n string) []int {
	return gconv.Ints(e.v[n])
}

func (e Entity) Strings(n string) []string {
	return gconv.Strings(e.v[n])
}

func (e Entity) Float32s(n string) []float32 {
	return gconv.Float32s(e.v[n])
}

func (e Entity) Float64s(n string) []float64 {
	return gconv.Float64s(e.v[n])
}

func (e Entity) IsNil() bool {
	return e.v == nil
}

func (e Entity) Raw() map[string]any {
	return e.v
}

func EntityArrayToRawArray(arr []Entity) []map[string]any {
	var result []map[string]any
	for _, item := range arr {
		result = append(result, item.Raw())
	}
	return result
}

func RawArrayToEntityArray(arr []map[string]any) []Entity {
	var result []Entity
	for _, item := range arr {
		result = append(result, Entity{v: item})
	}
	return result
}
