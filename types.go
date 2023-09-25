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

// COMMAND

type Command interface {
	aCommand()
}

type ObjectqlFindOneByIdCommand struct {
	Object string   `json:"object"`
	ID     string   `json:"id"`
	Fields []string `json:"fields"`
	Result string   `json:"result"`
	Direct bool     `json:"direct"`
}

type ObjectqlFindOneCommand struct {
	Object    string         `json:"object"`
	Condition map[string]any `json:"condition"`
	Sort      []string       `json:"sort"`
	Fields    []string       `json:"fields"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type ObjectqlFindListCommand struct {
	Condition map[string]any `json:"condition"`
	Top       int            `json:"top"`
	Skip      int            `json:"skip"`
	Sort      []string       `json:"sort"`
	Fields    []string       `json:"fields"`
	Object    string         `json:"object"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type ObjectqlCountCommand struct {
	Condition map[string]any `json:"condition"`
	Object    string         `json:"object"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type ObjectqlInsertCommand struct {
	Doc    map[string]any `json:"doc"`
	Fields []string       `json:"fields"`
	Object string         `json:"object"`
	Result string         `json:"result"`
	Direct bool           `json:"direct"`
}

type ObjectqlUpdateByIdCommand struct {
	ID     string         `json:"id"`
	Doc    map[string]any `json:"doc"`
	Fields []string       `json:"fields"`
	Object string         `json:"object"`
	Result string         `json:"result"`
	Direct bool           `json:"direct"`
}

type ObjectqlUpdateCommand struct {
	Condition map[string]any `json:"condition"`
	Doc       map[string]any `json:"doc"`
	Fields    []string       `json:"fields"`
	Object    string         `json:"object"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type ObjectqlDeleteByIdCommand struct {
	ID     string `json:"id"`
	Object string `json:"object"`
	Direct bool   `json:"direct"`
}

type ObjectqlDeleteCommand struct {
	Condition map[string]any `json:"condition"`
	Object    string         `json:"object"`
	Direct    bool           `json:"direct"`
}

func (c *ObjectqlFindOneByIdCommand) aCommand() {}
func (c *ObjectqlFindOneCommand) aCommand()     {}
func (c *ObjectqlFindListCommand) aCommand()    {}
func (c *ObjectqlCountCommand) aCommand()       {}
func (c *ObjectqlInsertCommand) aCommand()      {}
func (c *ObjectqlUpdateByIdCommand) aCommand()  {}
func (c *ObjectqlUpdateCommand) aCommand()      {}
func (c *ObjectqlDeleteByIdCommand) aCommand()  {}
func (c *ObjectqlDeleteCommand) aCommand()      {}
