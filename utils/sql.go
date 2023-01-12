package utils

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

const MaxPlaceholder = math.MaxUint16

func BulkUpdateEstimateTotalField(dataSize, fieldSize, conditionSize int) int {
	eachFieldInput := fieldSize - conditionSize
	fields := eachFieldInput * dataSize
	fieldCondition := conditionSize * dataSize * eachFieldInput
	whereCondition := conditionSize * dataSize
	return fields + fieldCondition + whereCondition
}

func BulkUpdateMaxDataSize(dataSize, totalField int) int {
	return int(float64(dataSize) * float64(MaxPlaceholder) / float64(totalField))
}

func BulkUpdateQuery(table string, data []map[string]any, keyEdits []string) (query string, binds map[string]any, err error) {
	emptyBinds := map[string]any{}
	if table == "" {
		return "", emptyBinds, errors.New("table is empty")
	}
	if len(data) == 0 {
		return "", emptyBinds, errors.New("data is empty")
	}
	if len(keyEdits) == 0 {
		return "", emptyBinds, errors.New("key edit is empty")
	}
	sort.Strings(keyEdits)

	binds = map[string]any{}
	columns := map[string]string{}
	conditions := map[string][]string{}
	for index, item := range data {
		condition := []string{}
		for _, key := range keyEdits {
			value, ok := item[key]
			if !ok {
				return "", emptyBinds, fmt.Errorf("key '%v' found in the data number %v: %v", key, index+1, item)
			}
			bindKey := fmt.Sprintf("%v_%v", key, index)
			condition = append(condition, fmt.Sprintf("%v = :%v", key, bindKey))
			conditions[key] = append(conditions[key], fmt.Sprintf(":%v", bindKey))
			binds[bindKey] = value
			delete(item, key)
		}

		for key, value := range item {
			bindKey := fmt.Sprintf("%v_%v", key, index)
			binds[bindKey] = value
			columns[key] = fmt.Sprintf("%s WHEN %s THEN %v", columns[key], strings.Join(condition, " AND "), fmt.Sprintf(":%v", bindKey))
		}
	}

	fieldQueries := []string{}
	for _, key := range SortMapKeys(columns) {
		fieldQueries = append(fieldQueries, fmt.Sprintf("%s = ( CASE %s ELSE %s END )", key, columns[key], key))
	}

	conditionQueries := []string{}
	for _, key := range SortMapKeys(conditions) {
		conditionQueries = append(conditionQueries, fmt.Sprintf("%s IN (%s)", key, strings.Join(conditions[key], ", ")))
	}

	query = fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		table,
		strings.Join(fieldQueries, ", "),
		strings.Join(conditionQueries, " AND "),
	)

	return query, binds, nil
}

func BulkCreateQuery(table string, data map[string]any) (query string, binds map[string]any, err error) {
	emptyBinds := map[string]any{}
	if table == "" {
		return "", emptyBinds, errors.New("table is empty")
	}
	if len(data) == 0 {
		return "", emptyBinds, errors.New("data is empty")
	}

	binds = map[string]any{}
	fields := []string{}
	placeholders := []string{}

	for _, key := range SortMapKeys(data) {
		val := data[key]
		fields = append(fields, key)
		placeholders = append(placeholders, fmt.Sprintf(":%s", key))
		binds[key] = val
	}
	query = fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)
	return query, binds, nil
}

func UpdateQuery(table string, payload, condition map[string]any) (query string, binds map[string]any, err error) {
	empty := map[string]any{}
	if table == "" {
		return "", empty, errors.New("table is empty")
	}
	if len(payload) == 0 {
		return "", empty, errors.New("payload is empty")
	}
	if len(condition) == 0 {
		return "", empty, errors.New("condition is empty")
	}

	// Field
	binds = map[string]any{}
	fields := []string{}
	if err != nil {
		return query, binds, fmt.Errorf("failed to build update query, make fields: %w", err)
	}
	for _, key := range SortMapKeys(payload) {
		keyBind := fmt.Sprintf("val_%s", key)
		val := payload[key]
		fields = append(fields, fmt.Sprintf("%s=:%s", key, keyBind))
		binds[keyBind] = val
	}

	// Condition
	conditionQuery, conditionBind, err := ConditionQuery(condition)
	if err != nil {
		return query, binds, fmt.Errorf("failed to build update query, make condition: %w", err)
	}
	if conditionQuery == "" {
		return query, binds, fmt.Errorf("make sure conditional not empty: %w", err)
	}
	for key, val := range conditionBind {
		binds[key] = val
	}

	// Query
	query = fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(fields, ", "), conditionQuery)
	return query, binds, nil
}

func ConditionQuery(condition map[string]any) (query string, binds map[string]any, err error) {
	if len(condition) == 0 {
		return "", map[string]any{}, errors.New("condition is empty")
	}

	binds = map[string]any{}
	cond := []string{}
	for _, key := range SortMapKeys(condition) {
		val := condition[key]
		kind := reflect.TypeOf(val).Kind()
		bindKey := fmt.Sprintf("cond_%s", key)
		str := ""
		if kind == reflect.Array || kind == reflect.Slice {
			if reflect.ValueOf(val).Len() == 0 {
				continue
			}
			str = fmt.Sprintf("%s IN (:%s)", key, bindKey)
		} else {
			str = fmt.Sprintf("%s=:%s", key, bindKey)
		}
		cond = append(cond, str)
		binds[bindKey] = val
	}
	return strings.Join(cond, " AND "), binds, err
}

// write query using this format
// https://extendsclass.com/sql-validator.html
func UglifyQuery(query string) string {
	matcher := regexp.MustCompile(`[\t|\n]+|\s\s+`)
	query = matcher.ReplaceAllString(query, " ")
	query = strings.TrimSpace(query)
	return query
}
