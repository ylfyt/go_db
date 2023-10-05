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

func fetchSlice(out any, onlyOneRow bool, conn *sql.DB, query string, params ...any) error {
	outType := reflect.TypeOf(out)

	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("out should be pointer to slice")
	}

	if outType.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("out should be slice")
	}

	sliceType := outType.Elem()
	isPointer := false
	var elemType reflect.Type
	if sliceType.Elem().Kind() == reflect.Pointer {
		if sliceType.Elem().Elem().Kind() != reflect.Struct {
			return fmt.Errorf("element must be struct")
		}
		isPointer = true
		elemType = sliceType.Elem().Elem()
	} else if sliceType.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("element must be struct")
	} else {
		elemType = sliceType.Elem()
	}

	rows, err := conn.Query(query, params...)
	if err != nil {
		return err
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	numOfColumns := len(columns)

	scans := make([]any, numOfColumns)
	scansPtr := make([]any, numOfColumns)
	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	fieldIdxMap := getFieldIdxMap(columns, elemType)
	fieldTypeMap := getFieldTypeMap(elemType)
	columnTypeMap := getColumnTypeMap(columns)

	outValue := reflect.ValueOf(out).Elem()

	for rows.Next() {
		err := rows.Scan(scansPtr...)
		if err != nil {
			return err
		}

		refVal := reflect.New(elemType)
		err = parseRow(scans, fieldIdxMap, refVal, fieldTypeMap, columnTypeMap, columns)
		if err != nil {
			return err
		}

		if isPointer {
			newOut := reflect.Append(outValue, refVal)
			outValue.Set(newOut)
		} else {
			newOut := reflect.Append(outValue, refVal.Elem())
			outValue.Set(newOut)
		}

	}

	return nil
}

func Fetch(out any, conn *sql.DB, query string, params ...any) error {
	return fetchSlice(out, false, conn, query, params...)
}
