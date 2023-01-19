package db

import (
	"go_update_bulk/generator"
	"go_update_bulk/utils"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDbSQL(t *testing.T) {
	totalData := 100
	gen := generator.NewGenerator(1, totalData, generator.NewUserDump(), "db", true)
	table := gen.Table()
	createData := gen.GetCreate()
	updateData := gen.GetUpdate()
	fieldSize := gen.FieldCount()
	primaryKey := gen.Primary()

	db, err := NewSQL("root:root@(localhost:3307)/test_db", runtime.NumCPU(), 200)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.EmptyTable(table); err != nil {
		t.Fatal(err)
	}

	t.Run("test create", func(t *testing.T) {
		err := db.CreateBulk(table, createData, fieldSize)
		assert.Nil(t, err)
	})

	t.Run("test update", func(t *testing.T) {
		keyEdit := []string{primaryKey}

		updateFnCount := 3
		paged := utils.PagedData(updateData, int(math.Ceil(float64(totalData)/float64(updateFnCount))))
		if len(paged) != updateFnCount {
			t.Fatal("make sure paged data correct")
		}

		err := db.UpdateBulk(table, paged[0], keyEdit, fieldSize)
		assert.Nil(t, err)

		err = db.UpdateParallel(table, paged[1], keyEdit, fieldSize)
		assert.Nil(t, err)

		err = db.UpdateSequential(table, paged[2], keyEdit, fieldSize)
		assert.Nil(t, err)
	})

	t.Run("test delete", func(t *testing.T) {
		ids := []int{}
		for _, v := range createData {
			ids = append(ids, v[primaryKey].(int))
		}
		err := db.Delete(table, map[string]any{primaryKey: ids})
		assert.Nil(t, err)
	})
}
