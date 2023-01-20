package db

import (
	"encoding/json"
	"go_update_bulk/generator"
	"go_update_bulk/utils"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func toString(data any) string {
	bytes, _ := json.MarshalIndent(data, "", "\t")
	return string(bytes)
}

// Make sure data source name exists
const dataSourceName = "root:root@(localhost:3307)/test_db?parseTime=true"

func TestNewSQL(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		_, err := NewSQL("", 1, 1)
		assert.NotNil(t, err)

		_, err = NewSQL("non_exists:non_exists@(non_exists:404)/non_exists", 1, 1)
		assert.NotNil(t, err)

		_, err = NewSQL(dataSourceName, 0, 1)
		assert.NotNil(t, err)

		_, err = NewSQL(dataSourceName, 1, 0)
		assert.NotNil(t, err)
	})

	t.Run("success", func(t *testing.T) {
		_, err := NewSQL(dataSourceName, 1, 1)
		assert.Nil(t, err)
	})
}

func TestDbSQL(t *testing.T) {
	totalData := 10
	removeNil := true
	tag := "db"

	// types
	type Type = generator.User
	dump := generator.NewUserDump()

	// generator
	gen := generator.NewGenerator(1, totalData, dump, tag, removeNil)
	table := gen.Table()
	createData := gen.GetCreate()
	updateData := gen.GetUpdate()
	fieldSize := gen.FieldCount()
	primaryKey := gen.Primary()

	selectedFieldOnCreate := []string{"*"}

	keyEdit := []string{primaryKey}
	selectedFieldOnEdit := []string{"age", "name", "address", "updated_at"}

	// condition
	primaries := []any{}
	for _, v := range createData {
		primary, ok := v[primaryKey]
		require.True(t, ok)
		primaries = append(primaries, primary)
	}

	// Init db
	db, err := NewSQL(dataSourceName, runtime.NumCPU(), 200)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.EmptyTable(table); err != nil {
		t.Fatal(err)
	}

	t.Run("create", func(t *testing.T) {

		t.Run("failed", func(t *testing.T) {
			err := db.CreateBulk("", createData, fieldSize)
			assert.NotNil(t, err)

			err = db.CreateBulk(table, []map[string]any{}, fieldSize)
			assert.NotNil(t, err)

			err = db.CreateBulk(table, createData, 0)
			assert.NotNil(t, err)
		})

		t.Run("success", func(t *testing.T) {
			err := db.CreateBulk(table, createData, fieldSize)
			assert.Nil(t, err)

			data := []Type{}
			err = db.Select(&data, table, selectedFieldOnCreate, &map[string]any{primaryKey: primaries}, nil)
			mapped, _ := utils.StructsToMaps(data, tag, removeNil)
			assert.Nil(t, err)
			assert.Len(t, data, totalData)
			assert.Equal(t, toString(createData), toString(mapped))

		})
	})

	t.Run("select", func(t *testing.T) {
		data := []Type{}
		t.Run("failed", func(t *testing.T) {
			err := db.Select(data, "", selectedFieldOnCreate, nil, nil)
			assert.NotNil(t, err)

			err = db.Select(data, table, []string{}, nil, nil)
			assert.NotNil(t, err)
		})

		t.Run("success", func(t *testing.T) {
			err = db.Select(&data, table, selectedFieldOnCreate, &map[string]any{primaryKey: primaries}, nil)
			mapped, _ := utils.StructsToMaps(data, tag, removeNil)
			assert.Nil(t, err)
			assert.Len(t, data, totalData)
			assert.Equal(t, toString(createData), toString(mapped))
		})
	})

	t.Run("update", func(t *testing.T) {
		functions := []struct {
			name string
			fn   func(table string, data []map[string]any, keyEdits []string, fieldSize int) error
		}{
			{name: "bulk", fn: db.UpdateBulk},
			{name: "sequential", fn: db.UpdateSequential},
			{name: "parallel", fn: db.UpdateParallel},
		}

		updateFnCount := len(functions)

		pageSize := int(math.Ceil(float64(totalData) / float64(updateFnCount)))
		pagedData := utils.PagedData(updateData, pageSize)
		pagedPrimary := utils.PagedData(primaries, pageSize)

		assert.Len(t, pagedData, updateFnCount)
		assert.Equal(t, len(pagedData), len(pagedPrimary))

		for index, v := range functions {
			item := v
			data := pagedData[index]
			primaries := pagedPrimary[index]

			t.Run(item.name, func(t *testing.T) {
				t.Parallel()

				t.Run("failed", func(t *testing.T) {
					err := item.fn("", data, keyEdit, fieldSize)
					assert.NotNil(t, err)

					err = item.fn(table, []map[string]any{}, keyEdit, fieldSize)
					assert.NotNil(t, err)

					err = item.fn(table, data, []string{}, fieldSize)
					assert.NotNil(t, err)

					err = item.fn(table, data, []string{"non_exists"}, fieldSize)
					assert.NotNil(t, err)

					err = item.fn(table, data, keyEdit, 0)
					assert.NotNil(t, err)
				})

				t.Run("success", func(t *testing.T) {
					err := item.fn(table, data, keyEdit, fieldSize)
					assert.Nil(t, err)

					dest := []Type{}
					err = db.Select(&dest, table, selectedFieldOnEdit, &map[string]any{primaryKey: primaries}, nil)
					mapped, _ := utils.StructsToMaps(dest, tag, removeNil)
					require.Nil(t, err)
					assert.Len(t, dest, len(data))
					assert.Equal(t, toString(data), toString(mapped))
				})
			})
		}
	})

	t.Run("delete", func(t *testing.T) {

		t.Run("failed", func(t *testing.T) {
			err := db.Delete("", map[string]any{"id": 1})
			assert.NotNil(t, err)

			err = db.Delete(table, map[string]any{})
			assert.NotNil(t, err)

			err = db.Delete(table, map[string]any{"non_exists": []any{1, 2, 3}})
			assert.NotNil(t, err)

			err = db.Delete(table, map[string]any{primaryKey: []any{}})
			assert.NotNil(t, err)
		})

		t.Run("success", func(t *testing.T) {
			err := db.Delete(table, map[string]any{primaryKey: primaries})
			assert.Nil(t, err)
		})
	})
}
