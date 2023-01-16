package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortMapKeys(t *testing.T) {
	data := map[string]any{"c": 3, "b": 2, "a": 1}
	expected := []string{"a", "b", "c"}
	actual := SortMapKeys(data)
	assert.Equal(t, expected, actual)
}
