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
	Type      FieldType
	Name      string
	Api       string
	Data      interface{}
	Comment   string
	relations []*RelationFiledInfo
}

type RelationFiledInfo struct {
	ThroughField *Field
	TargetField  *Field
}

type FieldType int

const (
	Int FieldType = iota
	String
	Bool
	Float
	DateTime
	Relate
	Formula
	Aggregation
)

type AggregationKind = int

const (
	Sum AggregationKind = iota
	Max
	Min
	Avg
	// Count
)

type RelateData struct {
	ObjectApi string
}

type FormulaData struct {
	Formula    string
	Type       FieldType
	sourceCode *formula.SourceCode
}

type AggregationData struct {
	Object    string
	Relate    string
	Field     string
	Type      FieldType
	Kind      AggregationKind
	Condition string
	resolved  *Field
}
