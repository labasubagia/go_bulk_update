package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructToMap(t *testing.T) {

	type testType struct {
		Field1 string  `json:"field1" db:"field1"`
		Field2 *string `json:"field2" db:"field2"`
		Field3 int
	}
	f2 := "f2"

	type testCase struct {
		input     any
		removeNil bool
		tag       string
		expected  map[string]any
	}

	t.Run("success", func(t *testing.T) {
		testCases := []testCase{
			{
				input:     testType{Field1: "f1", Field2: &f2, Field3: 1},
				tag:       "db",
				removeNil: true,
				expected:  map[string]any{"field1": "f1", "field2": "f2", "Field3": 1},
			},
			{
				input:     testType{Field1: "f1", Field3: 1},
				tag:       "json",
				removeNil: true,
				expected:  map[string]any{"field1": "f1", "Field3": 1},
			},
			{
				input:     testType{Field1: "f1", Field3: 1},
				tag:       "json",
				removeNil: false,
				expected:  map[string]any{"field1": "f1", "field2": nil, "Field3": 1},
			},
			{
				input:     testType{Field1: "f1", Field3: 1},
				tag:       "",
				removeNil: false,
				expected:  map[string]any{"Field1": "f1", "Field2": nil, "Field3": 1},
			},
		}

		for i, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", i), func(t *testing.T) {
				actual, err := StructToMap(testCase.input, testCase.tag, testCase.removeNil)
				assert.Nil(t, err)
				assert.Equal(t, fmt.Sprintf("%v", testCase.expected), fmt.Sprintf("%v", actual))
			})
		}
	})

	t.Run("failed", func(t *testing.T) {
		testCases := []testCase{
			{input: nil, expected: map[string]any{}},
		}
		for i, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", i), func(t *testing.T) {
				_, err := StructToMap(testCase.input, testCase.tag, testCase.removeNil)
				assert.NotNil(t, err)
			})
		}
	})
}

func TestStructsToMaps(t *testing.T) {
	type testType struct {
		Field1 string `json:"field1" db:"field1"`
		Field2 string `json:"field2" db:"field2"`
	}

	type testCase[T any | testType] struct {
		input     []T
		removeNil bool
		tag       string
		expected  []map[string]any
	}

	t.Run("success", func(t *testing.T) {

		testCases := []testCase[testType]{
			{
				input: []testType{
					{Field1: "1", Field2: "2"},
					{Field1: "1", Field2: "2"},
					{Field1: "1", Field2: "2"},
					{Field1: "1", Field2: "2"},
				},
				removeNil: true,
				tag:       "db",
				expected: []map[string]any{
					{"field1": "1", "field2": "2"},
					{"field1": "1", "field2": "2"},
					{"field1": "1", "field2": "2"},
					{"field1": "1", "field2": "2"},
				},
			},
		}

		for i, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", i), func(t *testing.T) {
				actual, err := StructsToMaps(testCase.input, testCase.tag, testCase.removeNil)
				assert.Nil(t, err)
				assert.Equal(t, testCase.expected, actual)
				assert.Equal(t, len(testCase.input), cap(actual))
			})
		}

	})

	t.Run("failed", func(t *testing.T) {
		testCases := []testCase[int]{
			{input: []int{1, 2}},
			{input: []int{}},
		}
		for i, testCase := range testCases {
			t.Run(fmt.Sprintf("TestCase %d", i), func(t *testing.T) {
				_, err := StructsToMaps(testCase.input, testCase.tag, testCase.removeNil)
				assert.NotNil(t, err)
			})
		}
	})
}

func TestCountField(t *testing.T) {
	type TestStruct struct {
		Field1 string
		Field2 string
		Field3 string
	}

	t.Run("success", func(t *testing.T) {
		actual, err := CountField(TestStruct{})
		assert.Nil(t, err)
		assert.Equal(t, 3, actual)
	})

	t.Run("failed", func(t *testing.T) {
		_, err := CountField("22")
		assert.NotNil(t, err)

		_, err = CountField(nil)
		assert.NotNil(t, err)
	})
}
