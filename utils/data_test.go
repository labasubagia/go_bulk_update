package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPagedData(t *testing.T) {

	testCases := []struct {
		Total, Size, Expected int
	}{
		{Total: 10, Size: 3, Expected: 4},
		{Total: 5, Size: 1, Expected: 5},
		{Total: 2, Size: 3, Expected: 1},
		{Total: 35, Size: 10, Expected: 4},
		{Total: 1, Size: 1, Expected: 1},
	}

	for i, testCase := range testCases {
		data := make([]int, testCase.Total)
		t.Run(fmt.Sprintf("TestCase %v", i+1), func(t *testing.T) {
			paged := PagedData(data, testCase.Size)
			assert.Equal(t, len(paged), testCase.Expected)
		})
	}
}

func TestOffset(t *testing.T) {
	type testCase struct {
		page, limit, offset int
	}
	testCases := []testCase{
		{page: 1, limit: 10, offset: 0},
		{page: 3, limit: 3, offset: 6},
		{page: 5, limit: 2, offset: 8},
	}
	for index, testCase := range testCases {
		t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
			paginate := Paginate{Page: testCase.page, Limit: testCase.limit}
			assert.Equal(t, paginate.Offset(), testCase.offset)
		})
	}
}
