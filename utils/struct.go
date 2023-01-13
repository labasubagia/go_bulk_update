package utils

import (
	"errors"
	"fmt"
	"reflect"
)

func StructToMap(payload any, tag string) (map[string]any, error) {
	result := map[string]any{}
	v := reflect.ValueOf(payload)
	if tag == "" {
		return result, errors.New("tag is required")
	}
	if v.Kind() != reflect.Struct {
		return result, errors.New("payload need to be struct")
	}
	for i := 0; i < v.NumField(); i++ {
		valueField := v.Field(i)
		typeField := v.Type().Field(i)

		fieldName := typeField.Tag.Get(tag)
		if fieldName == "" {
			continue
		}

		if valueField.Kind() == reflect.Ptr {
			if valueField.IsNil() {
				continue
			}
			valueField = valueField.Elem()
		}
		result[fieldName] = valueField.Interface()
	}
	return result, nil
}

func StructsToMaps[T any](data []T, tag string) ([]map[string]any, error) {
	empty := []map[string]any{}
	if len(data) == 0 {
		return empty, errors.New("data is empty")
	}
	result := make([]map[string]any, 0, len(data))
	for index, item := range data {
		m, err := StructToMap(item, tag)
		if err != nil {
			return empty, fmt.Errorf("failed convert data %d: %w", index, err)
		}
		result = append(result, m)
	}
	return result, nil
}

func CountField(input any) (int, error) {
	if reflect.TypeOf(input).Kind() != reflect.Struct {
		return 0, errors.New("input not struct")
	}
	return reflect.ValueOf(input).NumField(), nil
}
