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

type Command interface {
	aCommand()
}

type HandleCommand struct {
	Object  string         `json:"object"`
	Command string         `json:"command"`
	Fields  []string       `json:"fields"`
	Args    map[string]any `json:"args"`
	Result  string         `json:"result"`
}

type FindOneByIdCommand struct {
	Object string   `json:"object"`
	ID     string   `json:"id"`
	Fields []string `json:"fields"`
	Result string   `json:"result"`
	Direct bool     `json:"direct"`
}

type FindOneCommand struct {
	Object    string         `json:"object"`
	Condition map[string]any `json:"condition"`
	Sort      []string       `json:"sort"`
	Fields    []string       `json:"fields"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type FindListCommand struct {
	Condition map[string]any `json:"condition"`
	Top       int            `json:"top"`
	Skip      int            `json:"skip"`
	Sort      []string       `json:"sort"`
	Fields    []string       `json:"fields"`
	Object    string         `json:"object"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type CountCommand struct {
	Condition map[string]any `json:"condition"`
	Object    string         `json:"object"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type InsertCommand struct {
	Doc    map[string]any `json:"doc"`
	Fields []string       `json:"fields"`
	Object string         `json:"object"`
	Result string         `json:"result"`
	Direct bool           `json:"direct"`
}

type UpdateByIdCommand struct {
	ID     string         `json:"id"`
	Doc    map[string]any `json:"doc"`
	Fields []string       `json:"fields"`
	Object string         `json:"object"`
	Result string         `json:"result"`
	Direct bool           `json:"direct"`
}

type UpdateCommand struct {
	Condition map[string]any `json:"condition"`
	Doc       map[string]any `json:"doc"`
	Fields    []string       `json:"fields"`
	Object    string         `json:"object"`
	Result    string         `json:"result"`
	Direct    bool           `json:"direct"`
}

type DeleteByIdCommand struct {
	ID     string `json:"id"`
	Object string `json:"object"`
	Direct bool   `json:"direct"`
}

type DeleteCommand struct {
	Condition map[string]any `json:"condition"`
	Object    string         `json:"object"`
	Direct    bool           `json:"direct"`
}

func (c *HandleCommand) aCommand()      {}
func (c *FindOneByIdCommand) aCommand() {}
func (c *FindOneCommand) aCommand()     {}
func (c *FindListCommand) aCommand()    {}
func (c *CountCommand) aCommand()       {}
func (c *InsertCommand) aCommand()      {}
func (c *UpdateByIdCommand) aCommand()  {}
func (c *UpdateCommand) aCommand()      {}
func (c *DeleteByIdCommand) aCommand()  {}
func (c *DeleteCommand) aCommand()      {}

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
