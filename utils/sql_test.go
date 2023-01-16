package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBulkMaxDataSize(t *testing.T) {
	type testCase struct {
		dataSize, totalField, expected int
	}

	testCases := []testCase{
		{dataSize: 10, totalField: 4 * 10, expected: 16383},
		{dataSize: 20, totalField: 4 * 20, expected: 16383},
	}

	for index, testCase := range testCases {
		t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
			actual := BulkMaxDataSize(testCase.dataSize, testCase.totalField)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestBulkUpdateEstimateTotalField(t *testing.T) {
	type testCase struct {
		dataSize, fieldSize, conditionSize, expected int
	}

	testCases := []testCase{
		{dataSize: 10, fieldSize: 4, conditionSize: 1, expected: 70},
		{dataSize: 2, fieldSize: 4, conditionSize: 1, expected: 14},
	}

	for index, testCase := range testCases {
		t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
			actual := BulkUpdateEstimateTotalField(testCase.dataSize, testCase.fieldSize, testCase.conditionSize)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestBulkUpdateQuery(t *testing.T) {

	type testCase struct {
		table   string
		keyEdit []string
		data    []map[string]any
		query   string
		binds   map[string]any
	}

	t.Run("success", func(t *testing.T) {

		testCases := []testCase{
			{
				table:   "user",
				keyEdit: []string{"id"},
				data: []map[string]any{
					{"id": 1, "name": "Name0", "age": 1},
					{"id": 2, "name": "Name1", "age": 2, "address": "Addr1"},
				},
				query: `
					UPDATE
						user
					SET
						address = (
							CASE
								WHEN id = :id_1 THEN :address_1
								ELSE address
							END
						),
						age = (
							CASE
								WHEN id = :id_0 THEN :age_0
								WHEN id = :id_1 THEN :age_1
								ELSE age
							END
						),
						name = (
							CASE
								WHEN id = :id_0 THEN :name_0
								WHEN id = :id_1 THEN :name_1
								ELSE name
							END
						)
					WHERE
						id IN (:id_0, :id_1)
				`,
				binds: map[string]any{
					"id_0": 1, "age_0": 1, "name_0": "Name0",
					"id_1": 2, "age_1": 2, "name_1": "Name1", "address_1": "Addr1",
				},
			},
			{
				table:   "user",
				keyEdit: []string{"id", "name"},
				data:    []map[string]any{{"id": 1, "name": "Name0", "age": 1, "address": "Addr1"}},
				query: `
					UPDATE
						user
					SET
						address = (
							CASE
								WHEN id = :id_0
								AND name = :name_0 THEN :address_0
								ELSE address
							END
						),
						age = (
							CASE
								WHEN id = :id_0
								AND name = :name_0 THEN :age_0
								ELSE age
							END
						)
					WHERE
						id IN (:id_0)
						AND name IN (:name_0)
				`,
				binds: map[string]any{
					"id_0": 1, "age_0": 1, "name_0": "Name0", "address_0": "Addr1",
				},
			},
		}

		for index, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
				query, binds, err := BulkUpdateQuery(testCase.table, testCase.data, testCase.keyEdit)
				assert.Equal(t, UglifyQuery(testCase.query), UglifyQuery(query))
				assert.Equal(t, testCase.binds, binds)
				assert.Nil(t, err)
			})
		}
	})

	t.Run("failed", func(t *testing.T) {
		testCases := []testCase{
			{
				table:   "",
				keyEdit: []string{},
				data:    []map[string]any{},
			},
			{
				table:   "",
				keyEdit: []string{"id"},
				data:    []map[string]any{{"id": 1, "name": "Name0", "age": 1}},
			},
			{
				table:   "table",
				keyEdit: []string{},
				data:    []map[string]any{{"id": 1, "name": "Name0", "age": 1}},
			},
			{
				table:   "table",
				keyEdit: []string{"id"},
				data:    []map[string]any{},
			},
			{
				table:   "table",
				keyEdit: []string{"non_exists"},
				data:    []map[string]any{{"id": 1, "name": "Name0", "age": 1}},
			},
		}
		for index, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
				_, _, err := BulkUpdateQuery(testCase.table, testCase.data, testCase.keyEdit)
				assert.NotNil(t, err)
			})
		}
	})

}

func TestCreateQuery(t *testing.T) {
	type testCase struct {
		table string
		data  map[string]any
		query string
		bind  map[string]any
	}

	t.Run("success", func(t *testing.T) {

		testCases := []testCase{
			{
				table: "user",
				data:  map[string]any{"id": 1, "name": "John", "address": "Australia"},
				query: "INSERT INTO user (address, id, name) VALUES (:address, :id, :name)",
				bind:  map[string]any{"id": 1, "name": "John", "address": "Australia"},
			},
			{
				table: "product",
				data:  map[string]any{"id": 1, "name": "Mouse", "qty": 2},
				query: "INSERT INTO product (id, name, qty) VALUES (:id, :name, :qty)",
				bind:  map[string]any{"id": 1, "name": "Mouse", "qty": 2},
			},
		}

		for index, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
				query, bind, err := CreateQuery(testCase.table, testCase.data)
				assert.Nil(t, err)
				assert.Equal(t, UglifyQuery(testCase.query), UglifyQuery(query))
				assert.Equal(t, testCase.bind, bind)
			})
		}
	})

	t.Run("fail", func(t *testing.T) {
		_, _, err := CreateQuery("", map[string]any{"id": 1})
		assert.NotNil(t, err)

		_, _, err = CreateQuery("table", map[string]any{})
		assert.NotNil(t, err)
	})
}
