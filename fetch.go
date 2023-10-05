package main

import (
	"fmt"
	"reflect"
)

func (me *DB) Fetch(out any, query string, params ...any) error {
	outRef := reflect.TypeOf(out)
	if outRef.Kind() != reflect.Pointer {
		return fmt.Errorf("output must be pointer")
	}
	isSlice := outRef.Elem().Kind() == reflect.Slice

	if isSlice {
		if outRef.Elem().Elem().Kind() == reflect.Pointer {
			if isPrimitiveType(outRef.Elem().Elem().Elem()) {
				return me.fetchColumn(!isSlice, out, query, params...)
			}
			return me.fetch(!isSlice, out, query, params...)
		}
		if isPrimitiveType(outRef.Elem().Elem()) {
			return me.fetchColumn(!isSlice, out, query, params...)
		}
		return me.fetch(!isSlice, out, query, params...)
	}

	if outRef.Elem().Kind() == reflect.Pointer {
		if isPrimitiveType(outRef.Elem().Elem()) {
			return me.fetchColumn(!isSlice, out, query, params...)
		}
		return me.fetch(!isSlice, out, query, params...)
	}
	if isPrimitiveType(outRef.Elem()) {
		return me.fetchColumn(!isSlice, out, query, params...)
	}
	return me.fetch(!isSlice, out, query, params...)
}

func (me *DB) fetch(onlyOne bool, out any, query string, params ...any) error {
	if onlyOne {
		return me.fetchStruct(out, query, params...)
	}
	return me.fetchSlice(out, query, params...)
}

func (me *DB) fetchColumn(onlyOne bool, out any, query string, params ...any) error {
	if onlyOne {
		return me.fetchColumnOne(out, query, params...)
	}
	return me.fetchColumns(out, query, params...)
}

func (me *DB) fetchSlice(out any, query string, params ...any) error {
	outType := reflect.TypeOf(out)
	isPointer := false
	sliceType := outType.Elem()
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

	rows, err := me.conn.Query(query, params...)
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

func (me *DB) fetchStruct(out any, query string, params ...any) error {
	outType := reflect.TypeOf(out)

	isPointer := false
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

	rows, err := me.conn.Query(query, params...)
	if err != nil {
		return err
	}
	if !rows.Next() {
		return nil
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
	refVal, err := rowScan(rows, scansPtr, scans, elemType, fieldIdxMap, fieldTypeMap, columnTypeMap, columns)
	if err != nil {
		return err
	}

	outValue := reflect.ValueOf(out).Elem()
	if isPointer {
		outValue.Set(*refVal)
	} else {
		outValue.Set(refVal.Elem())
	}

	return nil
}

func (me *DB) fetchColumns(out any, query string, params ...any) error {
	outType := reflect.TypeOf(out)
	isPointer := false
	sliceType := outType.Elem()
	var elemType reflect.Type
	if sliceType.Elem().Kind() == reflect.Pointer {
		if !isPrimitiveType(sliceType.Elem().Elem()) {
			return fmt.Errorf("element must be primitive type")
		}
		isPointer = true
		elemType = sliceType.Elem().Elem()
	} else {
		if !isPrimitiveType(sliceType.Elem()) {
			return fmt.Errorf("element must be primitive type")
		}
		elemType = sliceType.Elem()
	}

	rows, err := me.conn.Query(query, params...)
	if err != nil {
		return err
	}
	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return fmt.Errorf("there is no column in query")
	}

	temp := reflect.New(elemType)
	tempTypeRef := getTypeRef(temp.Elem().Interface())
	columnTypeMap := getColumnTypeMap(columns)

	var scan any
	outValue := reflect.ValueOf(out).Elem()
	for rows.Next() {
		err = rows.Scan(&scan)
		if err != nil {
			return err
		}
		err = setValue(temp.Elem(), tempTypeRef, scan, columnTypeMap[0])
		if err != nil {
			return err
		}
		if isPointer {
			newOut := reflect.Append(outValue, temp)
			outValue.Set(newOut)
		} else {
			newOut := reflect.Append(outValue, temp.Elem())
			outValue.Set(newOut)
		}
	}

	return nil
}

func (me *DB) fetchColumnOne(out any, query string, params ...any) error {
	outType := reflect.TypeOf(out)

	isPointer := false
	var elemType reflect.Type
	if outType.Elem().Kind() == reflect.Pointer {
		if !isPrimitiveType(outType.Elem().Elem()) {
			return fmt.Errorf("element must be primitive type")
		}
		elemType = outType.Elem().Elem()
		isPointer = true
	} else {
		if !isPrimitiveType(outType.Elem()) {
			return fmt.Errorf("element must be primitive type")
		}
		if reflect.ValueOf(out).IsNil() {
			return fmt.Errorf("out cannot be nil")
		}
		elemType = outType.Elem()
	}

	rows, err := me.conn.Query(query, params...)
	if err != nil {
		return err
	}
	if !rows.Next() {
		return nil
	}
	var scan any
	err = rows.Scan(&scan)
	if err != nil {
		return err
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	columnTypeMap := getColumnTypeMap(columns)

	temp := reflect.New(elemType)

	err = setValue(temp.Elem(), getTypeRef(temp.Elem().Interface()), scan, columnTypeMap[0])
	if err != nil {
		return err
	}

	outValue := reflect.ValueOf(out).Elem()
	if isPointer {
		outValue.Set(temp)
	} else {
		outValue.Set(temp.Elem())
	}

	return nil
}
