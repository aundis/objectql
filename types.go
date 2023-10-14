package objectql

import (
	"reflect"

	"github.com/aundis/formula"
)

type Object struct {
	Name      string
	Api       string
	Fields    []*Field
	Comment   string
	Bind      any
	Querys    []*Handle
	Mutations []*Handle
}

type Handle struct {
	Name    string
	Api     string
	Comment string
	Resolve any
	req     reflect.Type
	res     reflect.Type
}

type Field struct {
	Parent    *Object
	Type      Type
	Name      string
	Api       string
	Comment   string
	valueApi  string
	relations []*relationFiledInfo
}

type relationFiledInfo struct {
	ThroughField *Field
	TargetField  *Field
}

type Type interface {
	aType()
}

type ObjectIDType struct{}
type IntType struct{}
type StringType struct{}
type BoolType struct{}
type FloatType struct{}
type DateTimeType struct{}

func (t *ObjectIDType) aType() {}
func (t *IntType) aType()      {}
func (t *StringType) aType()   {}
func (t *BoolType) aType()     {}
func (t *FloatType) aType()    {}
func (t *DateTimeType) aType() {}

var ObjectID = &ObjectIDType{}
var Int = &IntType{}
var String = &StringType{}
var Bool = &BoolType{}
var Float = &FloatType{}
var DateTime = &DateTimeType{}

type ExpandType struct {
	ObjectApi string
	FieldApi  string
}

func (t *ExpandType) aType() {}

type ExpandsType struct {
	ObjectApi string
	FieldApi  string
}

func (t *ExpandsType) aType() {}

type RelateType struct {
	ObjectApi string
}

func NewRelate(api string) *RelateType {
	return &RelateType{ObjectApi: api}
}

func (t *RelateType) aType() {}

type FormulaType struct {
	Formula    string
	Type       Type
	sourceCode *formula.SourceCode
}

func (t *FormulaType) aType() {}

func NewFormula(tpe Type, formula string) *FormulaType {
	return &FormulaType{
		Formula:    formula,
		Type:       tpe,
		sourceCode: nil,
	}
}

type ArrayType struct {
	Type Type
}

func (t *ArrayType) aType() {}

func NewArrayType(tpe Type) *ArrayType {
	return &ArrayType{
		Type: tpe,
	}
}

type AggregationType struct {
	Object    string
	Relate    string
	Field     string
	Type      Type
	Kind      AggregationKind
	Condition string
	resolved  *Field
}

func (t *AggregationType) aType() {}

type AggregationKind = int

const (
	Sum AggregationKind = iota
	Max
	Min
	Avg
	// Count
)

// COMMAND
type Command struct {
	Call   string   `json:"call"`
	Args   any      `json:"args"`
	Fields []string `json:"fields"`
	Result string   `json:"result"`
}

type FindOneByIdArgs struct {
	ID     string `json:"id"`
	Direct bool   `json:"direct"`
}

type FindOneArgs struct {
	Filter map[string]any `json:"filter"`
	Sort   []string       `json:"sort"`
	Direct bool           `json:"direct"`
}

type FindListArgs struct {
	Filter map[string]any `json:"filter"`
	Top    int            `json:"top"`
	Skip   int            `json:"skip"`
	Sort   []string       `json:"sort"`
	Direct bool           `json:"direct"`
}

type CountArgs struct {
	Filter map[string]any `json:"filter"`
	Direct bool           `json:"direct"`
}

type InsertArgs struct {
	Doc    map[string]any `json:"doc"`
	Direct bool           `json:"direct"`
}

type UpdateByIdArgs struct {
	ID     string         `json:"id"`
	Doc    map[string]any `json:"doc"`
	Direct bool           `json:"direct"`
}

type UpdateArgs struct {
	Filter map[string]any `json:"filter"`
	Doc    map[string]any `json:"doc"`
	Direct bool           `json:"direct"`
}

type DeleteByIdArgs struct {
	ID     string `json:"id"`
	Direct bool   `json:"direct"`
}

type DeleteArgs struct {
	Filter map[string]any `json:"filter"`
	Direct bool           `json:"direct"`
}

type ObjectInfo struct {
	Name      string       `json:"name"`
	Api       string       `json:"api"`
	Fields    []FieldInfo  `json:"fields"`
	Querys    []HandleInfo `json:"querys"`
	Mutations []HandleInfo `json:"mutations"`
}

type FieldInfo struct {
	Name string `json:"name"`
	Api  string `json:"api"`
}

type HandleInfo struct {
	Name string `json:"name"`
	Api  string `json:"api"`
}

// OPTIONS

type FindOneByIdOptions struct {
	ID     string   `json:"id"`
	Fields []string `json:"fields"`
	Direct bool     `json:"direct"`
}

type FindOneOptions struct {
	Filter map[string]any `json:"filter"`
	Sort   []string       `json:"sort"`
	Fields []string       `json:"fields"`
	Direct bool           `json:"direct"`
}

type FindListOptions struct {
	Filter map[string]any `json:"filter"`
	Top    int            `json:"top"`
	Skip   int            `json:"skip"`
	Sort   []string       `json:"sort"`
	Fields []string       `json:"fields"`
	Direct bool           `json:"direct"`
}

type CountOptions struct {
	Filter map[string]any `json:"filter"`
	Fields []string       `json:"fields"`
	Direct bool           `json:"direct"`
}

type InsertOptions struct {
	Doc    map[string]any `json:"doc"`
	Fields []string       `json:"fields"`
	Direct bool           `json:"direct"`
}

type UpdateByIdOptions struct {
	ID     string         `json:"id"`
	Doc    map[string]any `json:"doc"`
	Fields []string       `json:"fields"`
	Direct bool           `json:"direct"`
}

type UpdateOptions struct {
	Filter map[string]any `json:"filter"`
	Doc    map[string]any `json:"doc"`
	Fields []string       `json:"fields"`
	Direct bool           `json:"direct"`
}

type DeleteByIdOptions struct {
	ID     string `json:"id"`
	Direct bool   `json:"direct"`
}

type DeleteOptions struct {
	Filter map[string]any `json:"filter"`
	Direct bool           `json:"direct"`
}
