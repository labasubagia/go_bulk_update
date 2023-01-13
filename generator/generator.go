package generator

import "go_update_bulk/utils"

// Generator is only use to generate data for test purpose

type GenerateDump[T any] interface {
	Table() string
	Current() T
	DumpCreate(no int) T
	DumpUpdate(no int) T
}

type Generator interface {
	Table() string
	FieldCount() int
	GetCreate() []map[string]any
	GetUpdate() []map[string]any
	TotalData() int
}

type generator[T any] struct {
	tag        string
	typ        GenerateDump[T]
	size       int
	dataCreate []T
	dataUpdate []T
}

func NewGenerator[T any](start, size int, typ GenerateDump[T], tag string) Generator {
	end := start + size

	dataCreate := make([]T, 0, size)
	dataUpdate := make([]T, 0, size)
	for no := start; no < end; no++ {
		dataCreate = append(dataCreate, typ.DumpCreate(no))
		dataUpdate = append(dataUpdate, typ.DumpUpdate(no))
	}

	return &generator[T]{
		dataCreate: dataCreate,
		dataUpdate: dataUpdate,
		tag:        tag,
		typ:        typ,
		size:       size,
	}
}

func (g *generator[T]) Table() string {
	return g.typ.Table()
}

func (g *generator[T]) TotalData() int {
	return g.size
}

func (g *generator[T]) FieldCount() int {
	count, _ := utils.CountField(g.typ.Current())
	return count
}

func (g *generator[T]) GetCreate() []map[string]any {
	data, _ := utils.StructsToMaps(g.dataCreate, g.tag)
	return data
}

func (g *generator[T]) GetUpdate() []map[string]any {
	data, _ := utils.StructsToMaps(g.dataUpdate, g.tag)
	return data
}
