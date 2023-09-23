package objectql

import (
	"github.com/aundis/formula"
)

type Object struct {
	Name    string
	Api     string
	Fields  []*Field
	Comment string
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

func IsObjectIDType(tpe Type) bool {
	_, ok := tpe.(*ObjectIDType)
	return ok
}

func IsIntType(tpe Type) bool {
	_, ok := tpe.(*IntType)
	return ok
}

func IsStringType(tpe Type) bool {
	_, ok := tpe.(*StringType)
	return ok
}

func IsBoolType(tpe Type) bool {
	_, ok := tpe.(*BoolType)
	return ok
}

func IsFloatType(tpe Type) bool {
	_, ok := tpe.(*FloatType)
	return ok
}

func IsDateTimeType(tpe Type) bool {
	_, ok := tpe.(*DateTimeType)
	return ok
}

func IsRelateType(tpe Type) bool {
	_, ok := tpe.(*RelateType)
	return ok
}

func IsFormulaType(tpe Type) bool {
	_, ok := tpe.(*FormulaType)
	return ok
}

func IsArrayType(tpe Type) bool {
	_, ok := tpe.(*ArrayType)
	return ok
}

func IsAggregationType(tpe Type) bool {
	_, ok := tpe.(*AggregationType)
	return ok
}

func IsExpandType(tpe Type) bool {
	_, ok := tpe.(*ExpandType)
	return ok
}
