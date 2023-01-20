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
	gen := generator.NewGenerator(1, totalData, generator.NewUserDump(), "db", true)
	table := gen.Table()
	createData := gen.GetCreate()
	updateData := gen.GetUpdate()
	fieldSize := gen.FieldCount()
	primaryKey := gen.Primary()

	// condition
	primaries := []any{}
	for _, v := range createData {
		primary, ok := v[primaryKey]
		require.True(t, ok)
		primaries = append(primaries, primary)
	}
	condition := map[string]any{primaryKey: primaries}

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

			data := []generator.User{}
			err = db.Select(&data, table, []string{"*"}, &map[string]any{primaryKey: primaries}, nil)
			mapped, _ := utils.StructsToMaps(data, "db", true)
			assert.Nil(t, err)
			assert.Len(t, data, totalData)
			assert.Equal(t, createData, mapped)

		})
	})

	t.Run("select", func(t *testing.T) {
		data := []generator.User{}
		t.Run("failed", func(t *testing.T) {
			err := db.Select(data, "", []string{"*"}, nil, nil)
			assert.NotNil(t, err)

			err = db.Select(data, table, []string{}, nil, nil)
			assert.NotNil(t, err)
		})

		t.Run("success", func(t *testing.T) {
			err = db.Select(&data, table, []string{"*"}, &map[string]any{primaryKey: primaries}, nil)
			mapped, _ := utils.StructsToMaps(data, "db", true)
			assert.Nil(t, err)
			assert.Len(t, data, totalData)
			assert.Equal(t, createData, mapped)
		})
	})

	t.Run("update", func(t *testing.T) {
		keyEdit := []string{primaryKey}

		updateFnCount := 3
		pageSize := int(math.Ceil(float64(totalData) / float64(updateFnCount)))
		pagedData := utils.PagedData(updateData, pageSize)
		pagedPrimary := utils.PagedData(primaries, pageSize)

		selectedFields := []string{"age", "name", "address", "updated_at"}

		assert.Len(t, pagedData, updateFnCount)
		assert.Equal(t, len(pagedData), len(pagedPrimary))

		t.Run("bulk", func(t *testing.T) {
			t.Parallel()
			data := pagedData[0]
			primaries := pagedPrimary[0]

			t.Run("failed", func(t *testing.T) {
				err := db.UpdateBulk("", data, keyEdit, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateBulk(table, []map[string]any{}, keyEdit, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateBulk(table, data, []string{}, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateBulk(table, data, []string{"non_exists"}, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateBulk(table, data, keyEdit, 0)
				assert.NotNil(t, err)
			})

			t.Run("success", func(t *testing.T) {
				err := db.UpdateBulk(table, data, keyEdit, fieldSize)
				assert.Nil(t, err)

				dest := []generator.User{}
				err = db.Select(&dest, table, selectedFields, &map[string]any{primaryKey: primaries}, nil)
				mapped, _ := utils.StructsToMaps(dest, "db", true)
				require.Nil(t, err)
				assert.Len(t, dest, len(data))
				assert.Equal(t, toString(data), toString(mapped))
			})
		})

		t.Run("parallel", func(t *testing.T) {
			t.Parallel()
			data := pagedData[1]
			primaries := pagedPrimary[1]

			t.Run("failed", func(t *testing.T) {
				err := db.UpdateParallel("", data, keyEdit, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateParallel(table, []map[string]any{}, keyEdit, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateParallel(table, data, []string{}, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateParallel(table, data, []string{"non_exists"}, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateParallel(table, data, keyEdit, 0)
				assert.NotNil(t, err)
			})

			t.Run("success", func(t *testing.T) {
				err = db.UpdateParallel(table, data, keyEdit, fieldSize)
				assert.Nil(t, err)

				dest := []generator.User{}
				err = db.Select(&dest, table, selectedFields, &map[string]any{primaryKey: primaries}, nil)
				mapped, _ := utils.StructsToMaps(dest, "db", true)
				require.Nil(t, err)
				assert.Len(t, dest, len(data))
				assert.Equal(t, toString(data), toString(mapped))
			})
		})

		t.Run("sequential", func(t *testing.T) {
			t.Parallel()
			data := pagedData[2]
			primaries := pagedPrimary[2]

			t.Run("failed", func(t *testing.T) {
				err := db.UpdateSequential("", data, keyEdit, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateSequential(table, []map[string]any{}, keyEdit, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateSequential(table, data, []string{}, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateSequential(table, data, []string{"non_exists"}, fieldSize)
				assert.NotNil(t, err)

				err = db.UpdateSequential(table, data, keyEdit, 0)
				assert.NotNil(t, err)
			})

			t.Run("success", func(t *testing.T) {
				err = db.UpdateSequential(table, data, keyEdit, fieldSize)
				assert.Nil(t, err)

				dest := []generator.User{}
				err = db.Select(&dest, table, selectedFields, &map[string]any{primaryKey: primaries}, nil)
				mapped, _ := utils.StructsToMaps(dest, "db", true)
				require.Nil(t, err)
				assert.Len(t, dest, len(data))
				assert.Equal(t, toString(data), toString(mapped))
			})
		})
	})

	t.Run("delete", func(t *testing.T) {

		t.Run("failed", func(t *testing.T) {
			err := db.Delete("", condition)
			assert.NotNil(t, err)

			err = db.Delete(table, map[string]any{})
			assert.NotNil(t, err)

			err = db.Delete(table, map[string]any{"non_exists": []any{1, 2, 3}})
			assert.NotNil(t, err)

			err = db.Delete(table, map[string]any{primaryKey: []any{}})
			assert.NotNil(t, err)
		})

		t.Run("success", func(t *testing.T) {

			err := db.Delete(table, condition)
			assert.Nil(t, err)
		})
	})
}
