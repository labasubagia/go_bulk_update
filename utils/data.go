package utils

import "math"

func PagedData[T any](arr []T, pageSize int) [][]T {
	pagedData := [][]T{}
	totalPage := int(math.Ceil(float64(len(arr)) / float64(pageSize)))
	for page := 1; page <= totalPage; page++ {
		start := (page - 1) * pageSize
		stop := start + pageSize
		if stop > len(arr) {
			stop = len(arr)
		}
		pagedData = append(pagedData, arr[start:stop])
	}
	return pagedData
}
