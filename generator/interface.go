package generator

// Generator is only use to generate data for test purpose

type Generator interface {
	Table() string
	FieldCount() int
	GetCreate() []map[string]any
	GetUpdate() []map[string]any
}
