package main

import (
	"database/sql"
	"fmt"
	"reflect"
)

func parseRow(scans []any, fieldIdxMap []int, refValue reflect.Value, fieldTypeMap []typeRef, columnTypeMap []typeRef, columns []*sql.ColumnType) error {
	for i := range scans {
		idx := fieldIdxMap[i]
		if idx == -1 {
			continue
		}

		field := refValue.Elem().Field(idx)
		fieldType := fieldTypeMap[idx]
		err := setValue(field, fieldType, scans[i], columnTypeMap[i])
		if err != nil {
			return fmt.Errorf("%s (col:%s)", err.Error(), columns[i].Name())
		}
	}
	return nil
}

func fetchData[T any](onlyOneRow bool, conn *sql.DB, query string, params ...any) ([]T, error) {
	rows, err := conn.Query(query, params...)
	if err != nil {
		return nil, err
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	numOfColumns := len(columns)

	scans := make([]any, numOfColumns)
	scansPtr := make([]any, numOfColumns)
	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	var tempData T
	refValue := reflect.ValueOf(&tempData)
	refType := reflect.TypeOf(tempData)

	if refType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type t should be struct")
	}

	fieldIdxMap := getFieldIdxMap(columns, refType)
	fieldTypeMap := getFieldTypeMap(refValue)
	columnTypeMap := getColumnTypeMap(columns)

	if onlyOneRow {
		if !rows.Next() {
			return nil, nil
		}
		err := rows.Scan(scansPtr...)
		if err != nil {
			return nil, err
		}
		err = parseRow(scans, fieldIdxMap, refValue, fieldTypeMap, columnTypeMap, columns)
		if err != nil {
			return nil, err
		}
		return []T{tempData}, nil
	}

	var finalValues []T = make([]T, 0)
	for rows.Next() {
		err := rows.Scan(scansPtr...)
		if err != nil {
			return nil, err
		}

		err = parseRow(scans, fieldIdxMap, refValue, fieldTypeMap, columnTypeMap, columns)
		if err != nil {
			return nil, err
		}
		finalValues = append(finalValues, tempData)
	}

	return finalValues, nil
}

func Fetch[T any](conn *sql.DB, query string, params ...any) ([]T, error) {
	return fetchData[T](false, conn, query, params...)
}

func FetchOne[T any](conn *sql.DB, query string, params ...any) (*T, error) {
	result, err := fetchData[T](true, conn, query, params...)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}

	return &result[0], nil
}
