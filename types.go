package main

import "github.com/aundis/formula"

type Object struct {
	Name    string
	Api     string
	Fields  []*Field
	Comment string
}

type Field struct {
	Parent    *Object
	Name      string
	Api       string
	Kind      Kind
	Type      Type
	Data      interface{}
	Comment   string
	Relations []*RelationFiledInfo
}

type RelationFiledInfo struct {
	ThroughField *Field
	TargetField  *Field
}

type Kind int

const (
	Normal Kind = iota
	Relate
	Formula
	Aggregation
)

type Type int

const (
	Int Type = iota
	String
	Bool
	Float
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
	SourceCode *formula.SourceCode
}

type AggregationData struct {
	Object    string
	Relate    string
	Field     string
	Kind      AggregationKind
	Condition string
	Resolved  *Field
}
