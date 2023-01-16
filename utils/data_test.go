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
