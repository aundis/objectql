package objectql

import (
	"context"
	"reflect"

	"github.com/aundis/formula"
	"github.com/aundis/graphql"
	"github.com/aundis/graphql/language/ast"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	Parent                   *Object
	Require                  any
	RequireMsg               string
	Validate                 any
	ValidateMsg              string
	Type                     Type
	Name                     string
	Api                      string
	Comment                  string
	Default                  any
	Select                   []SelectOption
	SelectFrom               *SelectValueFrom
	SelectLabel              string
	valueApi                 string
	relations                []*relationFiledInfo
	requireSourceCode        *formula.SourceCode // 公式计算是否必填
	requireSourceCodeFields  []string            // 公式计算中需要的字段
	validateSourceCode       *formula.SourceCode // 数据验证公式
	validateSourceCodeFields []string            // 数据验证需要的字段
}

type SelectOption struct {
	Label string      `json:"label"`
	Value interface{} `json:"value"`
}

type SelectValueFrom struct {
	Object string `json:"object"`
	Label  string `json:"label"`
	Value  string `json:"value"`
}

type FieldReqireCheckHandle struct {
	Fields []string
	Handle func(ctx context.Context, cur *Var) error
}

type FieldValidateHandle struct {
	Fields []string
	Handle func(ctx context.Context, cur *Var) error
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
type DateType struct{}
type TimeType struct{}
type AnyType struct{}

func (t *ObjectIDType) aType() {}
func (t *IntType) aType()      {}
func (t *StringType) aType()   {}
func (t *BoolType) aType()     {}
func (t *FloatType) aType()    {}
func (t *DateTimeType) aType() {}
func (t *DateType) aType()     {}
func (t *TimeType) aType()     {}
func (t *AnyType) aType()      {}

var ObjectID = &ObjectIDType{}
var Int = &IntType{}
var String = &StringType{}
var Bool = &BoolType{}
var Float = &FloatType{}
var DateTime = &DateTimeType{}
var Date = &DateType{}
var Time = &TimeType{}
var Any = &AnyType{}

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
	Formula         string
	Type            Type
	sourceCode      *formula.SourceCode
	referenceFields []string // 公式引用到的字段
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
	Direct bool           `json:"direct"`
}

type FindListArgs struct {
	Filter map[string]any `json:"filter"`
	Top    int            `json:"top"`
	Skip   int            `json:"skip"`
	Sort   []string       `json:"sort"`
	Direct bool           `json:"direct"`
}

type AggregateArgs struct {
	Pipeline []map[string]any `json:"pipeline"`
	Direct   bool             `json:"direct"`
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

type ObjectMetaInfo struct {
	Name   string          `json:"name"`
	Api    string          `json:"api"`
	Fields []FieldMetaInfo `json:"fields"`
}

type FieldMetaInfo struct {
	Name        string           `json:"name"`
	Api         string           `json:"api"`
	Type        string           `json:"type"`
	Readonly    bool             `json:"readonly"`
	Select      []SelectOption   `json:"select"`
	SelectFrom  *SelectValueFrom `json:"selectFrom"`
	SelectLabel string           `json:"selectLabel"`
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
	Fields []string       `json:"fields"`
	Sort   []string       `json:"sort"`
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

type AggregateOptions struct {
	Pipeline []map[string]any `json:"pipeline"`
	Direct   bool             `json:"direct"`
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

var graphqlAny = graphql.NewScalar(graphql.ScalarConfig{
	Name:        "any",
	Description: "interface{}",
	Serialize: func(value interface{}) interface{} {
		if v, ok := value.(primitive.M); ok {
			return map[string]any(v)
		}
		return value
	},
	ParseValue: func(value interface{}) interface{} {
		return value
	},
	ParseLiteral: func(valueAST ast.Value) interface{} {
		return nil
	},
})

var Null = &struct{}{}
