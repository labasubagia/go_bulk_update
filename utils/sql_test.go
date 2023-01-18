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

func TestUpdateQuery(t *testing.T) {
	type testCase struct {
		table     string
		field     map[string]any
		condition map[string]any
		query     string
		bind      map[string]any
	}

	t.Run("success", func(t *testing.T) {
		testCases := []testCase{
			{
				table:     "table",
				field:     map[string]any{"f1": "1", "f2": "2"},
				condition: map[string]any{"c1": 1, "c2": 2},
				query:     "UPDATE table SET f1 = :val_f1, f2 = :val_f2 WHERE c1 = :cond_c1 AND c2 = :cond_c2",
				bind:      map[string]any{"val_f1": "1", "val_f2": "2", "cond_c1": 1, "cond_c2": 2},
			},
			{
				table:     "table",
				field:     map[string]any{"f1": "1", "f2": "2"},
				condition: map[string]any{"f1": 1, "f2": 2},
				query:     "UPDATE table SET f1 = :val_f1, f2 = :val_f2 WHERE f1 = :cond_f1 AND f2 = :cond_f2",
				bind:      map[string]any{"val_f1": "1", "val_f2": "2", "cond_f1": 1, "cond_f2": 2},
			},
		}

		for index, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
				query, bind, err := UpdateQuery(testCase.table, testCase.field, testCase.condition)
				assert.Nil(t, err)
				assert.Equal(t, UglifyQuery(testCase.query), UglifyQuery(query))
				assert.Equal(t, testCase.bind, bind)
			})
		}
	})

	t.Run("failed", func(t *testing.T) {
		_, _, err := UpdateQuery("", map[string]any{"f1": 1}, map[string]any{"c1": 1})
		assert.NotNil(t, err)

		_, _, err = UpdateQuery("table", map[string]any{}, map[string]any{"c1": 1})
		assert.NotNil(t, err)

		_, _, err = UpdateQuery("table", map[string]any{"f1": 1}, map[string]any{})
		assert.NotNil(t, err)

		_, _, err = UpdateQuery("table", map[string]any{"f1": 1}, map[string]any{"c1": []int{}})
		assert.NotNil(t, err)
	})
}

func TestDeleteQuery(t *testing.T) {

	type testCase struct {
		table     string
		condition map[string]any
		query     string
		bind      map[string]any
	}

	t.Run("success", func(t *testing.T) {
		testCases := []testCase{
			{
				table:     "table",
				condition: map[string]any{"id": 1},
				query:     "DELETE FROM table WHERE id = :cond_id",
				bind:      map[string]any{"cond_id": 1},
			},
			{
				table:     "user",
				condition: map[string]any{"address": "Denpasar", "nationality": "Indonesia"},
				query:     "DELETE FROM user WHERE address = :cond_address AND nationality = :cond_nationality",
				bind:      map[string]any{"cond_address": "Denpasar", "cond_nationality": "Indonesia"},
			},
		}

		for index, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
				query, bind, err := DeleteQuery(testCase.table, testCase.condition)
				assert.Nil(t, err)
				assert.Equal(t, testCase.query, query)
				assert.Equal(t, testCase.bind, bind)
			})
		}
	})

	t.Run("failed", func(t *testing.T) {
		_, _, err := DeleteQuery("", map[string]any{"field": 1})
		assert.NotNil(t, err)

		_, _, err = DeleteQuery("table", map[string]any{})
		assert.NotNil(t, err)

		_, _, err = DeleteQuery("table", map[string]any{"ids": []int{}})
		assert.NotNil(t, err)
	})

}

func TestSelectQuery(t *testing.T) {

	type testCase struct {
		table     string
		fields    []string
		condition *map[string]any
		paginate  *Paginate
		query     string
		bind      map[string]any
	}

	t.Run("success", func(t *testing.T) {
		testCases := []testCase{
			{
				table:  "table",
				fields: []string{"field1", "field2"},
				query:  "SELECT field1, field2 FROM table",
				bind:   map[string]any{},
			},
			{
				table:     "table",
				fields:    []string{"field1", "field2"},
				condition: &map[string]any{"field3": 1},
				query:     "SELECT field1, field2 FROM table WHERE field3 = :cond_field3",
				bind:      map[string]any{"cond_field3": 1},
			},
			{
				table:     "table",
				fields:    []string{"field1", "field2"},
				condition: &map[string]any{"field3": 1},
				paginate:  &Paginate{Page: 1, Limit: 10},
				query: `
					SELECT
						field1,
						field2
					FROM
						table
					WHERE
						field3 = :cond_field3
					LIMIT
						:paginate_limit OFFSET :paginate_offset
				`,
				bind: map[string]any{"cond_field3": 1, "paginate_limit": 10, "paginate_offset": 0},
			},
		}

		for index, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
				query, bind, err := SelectQuery(testCase.table, testCase.fields, testCase.condition, testCase.paginate)
				assert.Nil(t, err)
				assert.Equal(t, UglifyQuery(testCase.query), UglifyQuery(query))
				assert.Equal(t, testCase.bind, bind)
			})
		}
	})

	t.Run("failed", func(t *testing.T) {
		_, _, err := SelectQuery("", []string{"f1", "f2"}, nil, nil)
		assert.NotNil(t, err)

		_, _, err = SelectQuery("table", []string{}, nil, nil)
		assert.NotNil(t, err)

		_, _, err = SelectQuery("table", []string{"f1", "f2"}, &map[string]any{}, nil)
		assert.NotNil(t, err)

		_, _, err = SelectQuery("table", []string{"f1", "f2"}, &map[string]any{"ids": []int{}}, nil)
		assert.NotNil(t, err)
	})
}

func TestConditionQuery(t *testing.T) {

	type testCase struct {
		condition map[string]any
		query     string
		bind      map[string]any
	}

	t.Run("success", func(t *testing.T) {
		testCases := []testCase{
			{
				condition: map[string]any{"c1": 1, "c2": 2},
				query:     "c1 = :cond_c1 AND c2 = :cond_c2",
				bind:      map[string]any{"cond_c1": 1, "cond_c2": 2},
			},
			{
				condition: map[string]any{"c1": 1, "c2": 2, "c3": []int{1, 2}, "c4": []int{}},
				query:     "c1 = :cond_c1 AND c2 = :cond_c2 AND c3 IN (:cond_c3)",
				bind:      map[string]any{"cond_c1": 1, "cond_c2": 2, "cond_c3": []int{1, 2}},
			},
		}

		for index, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
				query, bind, err := ConditionQuery(testCase.condition)
				assert.Nil(t, err)
				assert.Equal(t, UglifyQuery(testCase.query), UglifyQuery(query))
				assert.Equal(t, testCase.bind, bind)
			})
		}
	})

	t.Run("failed", func(t *testing.T) {
		_, _, err := ConditionQuery(map[string]any{})
		assert.NotNil(t, err)

		_, _, err = ConditionQuery(map[string]any{"ids": []int{}})
		assert.NotNil(t, err)
	})
}
