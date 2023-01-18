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

// MaxPlaceholder is the maximum limit SQL placeholder
//
// In this utils will set default to max uint16 (65535) for MySQL (database that I currently use)
const MaxPlaceholder = math.MaxUint16

// BulkMaxDataSize is maximum data can inserted in one SQL query based on length of data and maximum placeholder size
//
// dataSize is length of data
//
// for CreateBulk, totalField usually struct/map length * dataSize
//
// for UpdateBulk, totalField using BulkUpdateEstimateTotalField method
func BulkMaxDataSize(dataSize, totalField int) int {
	return int(float64(dataSize) * float64(MaxPlaceholder) / float64(totalField))
}

// BulkUpdateEstimateTotalField is to count estimated all of fields/placeholders that created in BulkUpdateQuery
//
// dataSize is length of data
//
// fieldSize is length of struct/map
//
// conditionSize is length of how many field that used as condition
func BulkUpdateEstimateTotalField(dataSize, fieldSize, conditionSize int) int {
	eachFieldInput := fieldSize - conditionSize
	fields := eachFieldInput * dataSize
	fieldCondition := conditionSize * dataSize * eachFieldInput
	whereCondition := conditionSize * dataSize
	return fields + fieldCondition + whereCondition
}

// BulkUpdateQuery to build bulk update data SQL in single query
//
// keyEdits is key that used as conditional e.g []string{"id"}
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
				return "", emptyBinds, fmt.Errorf("key '%s' found in the data number %d", key, index+1)
			}
			bindKey := fmt.Sprintf("%s_%d", key, index)
			condition = append(condition, fmt.Sprintf("%s = :%s", key, bindKey))
			conditions[key] = append(conditions[key], fmt.Sprintf(":%s", bindKey))
			binds[bindKey] = value
			delete(item, key)
		}

		for key, value := range item {
			bindKey := fmt.Sprintf("%s_%d", key, index)
			binds[bindKey] = value
			columns[key] = fmt.Sprintf("%s WHEN %s THEN %s", columns[key], strings.Join(condition, " AND "), fmt.Sprintf(":%s", bindKey))
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

// CreateQuery is used to build create query
func CreateQuery(table string, data map[string]any) (query string, binds map[string]any, err error) {
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

// UpdateQuery to build update data query
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
	for key, val := range conditionBind {
		binds[key] = val
	}

	// Query
	query = fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(fields, ", "), conditionQuery)
	return query, binds, nil
}

func DeleteQuery(table string, condition map[string]any) (query string, bind map[string]any, err error) {
	if table == "" {
		return "", map[string]any{}, fmt.Errorf("table is empty")
	}
	if len(condition) == 0 {
		return "", map[string]any{}, fmt.Errorf("condition is empty")
	}

	conditionQuery, conditionBind, err := ConditionQuery(condition)
	if err != nil {
		return "", map[string]any{}, fmt.Errorf("failed build condition: %w", err)
	}
	query = fmt.Sprintf("DELETE FROM %s WHERE %s", table, conditionQuery)
	return query, conditionBind, nil
}

// ConditionQuery is used to build conditional query in mysql
//
// e.g. WHERE id=:cond_id AND name=:cond_name
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

	query = strings.TrimSpace(strings.Join(cond, " AND "))
	if query == "" {
		return "", map[string]any{}, errors.New("query is empty, please make sure condition input is valid")
	}

	return query, binds, err
}

// UglifyQuery is used to remove all formatting from a query
//
// can be used in testing
//
// write query using this format https://extendsclass.com/sql-validator.html
func UglifyQuery(query string) string {
	matcher := regexp.MustCompile(`[\t|\n]+|\s\s+`)
	query = matcher.ReplaceAllString(query, " ")
	query = strings.TrimSpace(query)
	return query
}
