package objectql

type InsertOptions struct {
	Doc    map[string]any
	Fields []string
}

type UpdateOptions struct {
	Condition map[string]any
	Doc       map[string]any
	Fields    []string
}

type UpdateByIdOptions struct {
	Doc    map[string]any
	Fields []string
}

type FindListOptions struct {
	Condition map[string]any
	Top       int
	Skip      int
	Sort      []string
	Fields    []string
}

type FindOneOptions struct {
	Condition map[string]any
	Top       int
	Skip      int
	Sort      []string
	Fields    []string
}
