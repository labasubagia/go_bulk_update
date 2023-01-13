package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
