package generator

import (
	"fmt"
	"go_update_bulk/utils"
)

type User struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Address string `db:"address"`
}

type userGenerator struct {
	tag        string
	dataCreate []User
	dataUpdate []User
}

func NewUserGenerator(start, size int) Generator {
	end := start + size

	dataCreate := make([]User, 0, size)
	dataUpdate := make([]User, 0, size)
	for i := start; i < end; i++ {
		dataCreate = append(dataCreate, User{
			ID:      i,
			Age:     10,
			Name:    fmt.Sprintf("Name_%v", i),
			Address: fmt.Sprintf("Addr_%v", i),
		})
		dataUpdate = append(dataUpdate, User{
			ID:      i,
			Age:     i,
			Name:    fmt.Sprintf("Edited_Name_%v", i),
			Address: fmt.Sprintf("Edited_Addr_%v", i),
		})
	}

	return &userGenerator{
		dataCreate: dataCreate,
		dataUpdate: dataUpdate,
		tag:        "db",
	}
}

func (g *userGenerator) Table() string {
	return "user"
}

func (g *userGenerator) FieldCount() int {
	count, _ := utils.CountField(User{})
	return count
}

func (g *userGenerator) GetCreate() []map[string]any {
	data, _ := utils.StructsToMaps(g.dataCreate, g.tag)
	return data
}

func (g *userGenerator) GetUpdate() []map[string]any {
	data, _ := utils.StructsToMaps(g.dataCreate, g.tag)
	return data
}
