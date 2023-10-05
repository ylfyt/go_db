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

func fetchSlice(out any, conn *sql.DB, query string, params ...any) error {
	outType := reflect.TypeOf(out)

	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("out should be pointer")
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
	} else {
		if sliceType.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("element must be struct")
		}
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

	scans := make([]any, len(columns))
	scansPtr := make([]any, len(columns))
	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	fieldIdxMap := getFieldIdxMap(columns, elemType)
	fieldTypeMap := getFieldTypeMap(elemType)
	columnTypeMap := getColumnTypeMap(columns)

	outValue := reflect.ValueOf(out).Elem()
	for rows.Next() {
		refVal, err := rowScan(rows, scansPtr, scans, elemType, fieldIdxMap, fieldTypeMap, columnTypeMap, columns)
		if err != nil {
			return err
		}

		if isPointer {
			newOut := reflect.Append(outValue, *refVal)
			outValue.Set(newOut)
		} else {
			newOut := reflect.Append(outValue, refVal.Elem())
			outValue.Set(newOut)
		}
	}

	return nil
}

func rowScan(rows *sql.Rows, scansPtr []any, scans []any, elemType reflect.Type, fieldIdxMap []int, fieldTypeMap []typeRef, columnTypeMap []typeRef, columns []*sql.ColumnType) (*reflect.Value, error) {
	err := rows.Scan(scansPtr...)
	if err != nil {
		return nil, err
	}

	refVal := reflect.New(elemType)
	err = parseRow(scans, fieldIdxMap, refVal, fieldTypeMap, columnTypeMap, columns)
	if err != nil {
		return nil, err
	}
	return &refVal, nil
}

func fetchStruct(out any, conn *sql.DB, query string, params ...any) error {
	outType := reflect.TypeOf(out)
	isPointer := false
	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("out must be pointer")
	}
	var elemType reflect.Type
	if outType.Elem().Kind() == reflect.Pointer {
		if outType.Elem().Elem().Kind() != reflect.Struct {
			return fmt.Errorf("out must be struct")
		}
		elemType = outType.Elem().Elem()
		isPointer = true
	} else {
		if outType.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("out must be struct")
		}
		if reflect.ValueOf(out).IsNil() {
			return fmt.Errorf("out cannot be nil")
		}
		elemType = outType.Elem()
	}

	rows, err := conn.Query(query, params...)
	if err != nil {
		return err
	}
	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	scans := make([]any, len(columns))
	scansPtr := make([]any, len(columns))
	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	fieldIdxMap := getFieldIdxMap(columns, elemType)
	fieldTypeMap := getFieldTypeMap(elemType)
	columnTypeMap := getColumnTypeMap(columns)

	outValue := reflect.ValueOf(out).Elem()
	if rows.Next() {
		refVal, err := rowScan(rows, scansPtr, scans, elemType, fieldIdxMap, fieldTypeMap, columnTypeMap, columns)
		if err != nil {
			return err
		}
		if isPointer {
			outValue.Set(*refVal)
		} else {
			outValue.Set(refVal.Elem())
		}
	}
	return nil
}

func Fetch(out any, conn *sql.DB, query string, params ...any) error {
	return fetchSlice(out, conn, query, params...)
}
