package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaginateData(t *testing.T) {

	type testCase[T any] struct {
		data     []T
		total    int
		paginate *Paginate
		expected Pagination[T]
	}

	testCases := []testCase[int]{
		{
			data:     make([]int, 10),
			total:    50,
			paginate: &Paginate{Page: 2, Limit: 10},
			expected: Pagination[int]{
				Items:       make([]int, 10),
				Total:       50,
				CurrentPage: 2,
				Limit:       10,
				NextPage:    3,
				PrevPage:    1,
				TotalPage:   5,
			},
		},
		{
			data:     make([]int, 3),
			total:    10,
			paginate: &Paginate{Page: 3, Limit: 3},
			expected: Pagination[int]{
				Items:       make([]int, 3),
				Total:       10,
				CurrentPage: 3,
				Limit:       3,
				NextPage:    4,
				PrevPage:    2,
				TotalPage:   4,
			},
		},
		{
			data:  make([]int, 100),
			total: 100,
			expected: Pagination[int]{
				Items:       make([]int, 100),
				Total:       100,
				CurrentPage: 1,
				Limit:       100,
				NextPage:    1,
				PrevPage:    1,
				TotalPage:   1,
			},
		},
	}

	for index, testCase := range testCases {
		t.Run(fmt.Sprintf("TestCase %d", index+1), func(t *testing.T) {
			actual := PaginateData(testCase.data, testCase.total, testCase.paginate)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}
