package go_db

import (
	"database/sql"
	"fmt"
	"reflect"
)

type queryable interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
}

func fetchSlice(conn queryable, out any, query string, args ...any) error {
	outType := reflect.TypeOf(out)
	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("output must be pointer")
	}

	if outType.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("output must slice")
	}

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

	rows, err := conn.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	scans := make([]any, len(columns))
	scansPtr := make([]any, len(columns))
	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	fieldTypeMap := getFieldTypeMap(elemType)
	columnTypeMap := getColumnTypeMap(columns)
	fieldIdxMap, fieldJoinMap, parentKeys := getFieldIdxMap(columns, elemType)

	for i := range fieldJoinMap {
		el := fieldJoinMap[i]
		el.FieldTypeMap = getFieldTypeMap(fieldJoinMap[i].Type)
		fieldJoinMap[i] = el
	}

	newIdx := 0
	nestedKeyToIdx := make(map[string]bool)
	var parentKeyToIdx map[string]int = make(map[string]int)
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
		shouldInsert := true
		if len(parentKeys) > 0 {
			parentKey := ""
			for _, colIdx := range parentKeys {
				parentKey += "_" + fmt.Sprint(scans[colIdx])
			}
			parentIdx, parentExist := parentKeyToIdx[parentKey]
			shouldInsert = !parentExist

			for parentFieldIdx, join := range fieldJoinMap {
				nestedNew := reflect.New(join.Type)
				for fieldIdx, colIdx := range join.FieldIdxToColIdx {
					if colIdx == -1 {
						continue
					}
					field := nestedNew.Elem().Field(fieldIdx)
					err := setValue(field, join.FieldTypeMap[fieldIdx], scans[colIdx], columnTypeMap[colIdx])
					if err != nil {
						return fmt.Errorf("%s (col:%s)", err.Error(), columns[colIdx].Name())
					}
				}

				if parentExist {
					if !join.IsSlice {
						continue
					}
					key := fmt.Sprint(parentIdx) + "_" + fmt.Sprint(parentFieldIdx) + "_"
					for _, colIdx := range join.KeyColIdxs {
						key += fmt.Sprint(scans[colIdx]) + "_"
					}
					if nestedKeyToIdx[key] {
						continue
					}
					nestedKeyToIdx[key] = true
					el := outValue.Index(parentIdx)
					newEl := reflect.Append(el.Field(parentFieldIdx), nestedNew.Elem())
					el.Field(parentFieldIdx).Set(newEl)
					continue
				}

				if join.IsSlice {
					key := fmt.Sprint(newIdx) + "_" + fmt.Sprint(parentFieldIdx) + "_"
					for _, colIdx := range join.KeyColIdxs {
						key += fmt.Sprint(scans[colIdx]) + "_"
					}
					nestedKeyToIdx[key] = true
					newOut := reflect.Append(refVal.Elem().Field(parentFieldIdx), nestedNew.Elem())
					refVal.Elem().Field(parentFieldIdx).Set(newOut)
				} else if join.IsPointer {
					refVal.Elem().Field(parentFieldIdx).Set(nestedNew)
				} else {
					refVal.Elem().Field(parentFieldIdx).Set(nestedNew.Elem())
				}
			}
			if shouldInsert {
				parentKeyToIdx[parentKey] = newIdx
			}
		}
		if !shouldInsert {
			continue
		}
		newIdx++
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

func fetchStruct(conn queryable, out any, query string, args ...any) error {
	outType := reflect.TypeOf(out)
	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("output must be pointer")
	}

	isPointer := false
	var elemType reflect.Type
	if outType.Elem().Kind() == reflect.Pointer {
		if outType.Elem().Elem().Kind() != reflect.Struct {
			return fmt.Errorf("output must be struct")
		}
		elemType = outType.Elem().Elem()
		isPointer = true
	} else {
		if outType.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("output must be struct")
		}
		if reflect.ValueOf(out).IsNil() {
			return fmt.Errorf("output cannot be nil")
		}
		elemType = outType.Elem()
	}

	rows, err := conn.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	scans := make([]any, len(columns))
	scansPtr := make([]any, len(columns))
	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	fieldIdxMap, fieldJoinMap, _ := getFieldIdxMap(columns, elemType)
	fieldTypeMap := getFieldTypeMap(elemType)
	columnTypeMap := getColumnTypeMap(columns)

	hasSlice := false
	for i := range fieldJoinMap {
		hasSlice = hasSlice || fieldJoinMap[i].IsSlice
		el := fieldJoinMap[i]
		el.FieldTypeMap = getFieldTypeMap(fieldJoinMap[i].Type)
		fieldJoinMap[i] = el
	}

	isSet := false
	joinSet := false
	refVal := reflect.New(elemType)
	parentFieldKeyMap := make(map[int]string)
	for rows.Next() {
		err = rows.Scan(scansPtr...)
		if err != nil {
			return err
		}
		if !isSet {
			err = parseRow(scans, fieldIdxMap, refVal, fieldTypeMap, columnTypeMap, columns)
			if err != nil {
				return err
			}
			isSet = true
		}
		if !hasSlice && joinSet {
			continue
		}
		for parentFieldIdx, join := range fieldJoinMap {
			nestedNew := reflect.New(join.Type)
			for fieldIdx, colIdx := range join.FieldIdxToColIdx {
				if colIdx == -1 {
					continue
				}
				field := nestedNew.Elem().Field(fieldIdx)
				err := setValue(field, join.FieldTypeMap[fieldIdx], scans[colIdx], columnTypeMap[colIdx])
				if err != nil {
					return fmt.Errorf("%s (col:%s)", err.Error(), columns[colIdx].Name())
				}
			}

			key := ""
			for _, colIdx := range join.KeyColIdxs {
				key += "_" + fmt.Sprint(scans[colIdx])
			}
			oldKey, exist := parentFieldKeyMap[parentFieldIdx]
			if exist && key != oldKey {
				continue
			}

			if exist {
				if !join.IsSlice {
					continue
				}
				newOut := reflect.Append(refVal.Elem().Field(parentFieldIdx), nestedNew.Elem())
				refVal.Elem().Field(parentFieldIdx).Set(newOut)
				continue
			}
			parentFieldKeyMap[parentFieldIdx] = key
			if join.IsSlice {
				newOut := reflect.Append(refVal.Elem().Field(parentFieldIdx), nestedNew.Elem())
				refVal.Elem().Field(parentFieldIdx).Set(newOut)
			} else if join.IsPointer {
				refVal.Elem().Field(parentFieldIdx).Set(nestedNew)
			} else {
				refVal.Elem().Field(parentFieldIdx).Set(nestedNew.Elem())
			}
		}
		joinSet = true
	}

	if !isSet {
		return nil
	}
	outValue := reflect.ValueOf(out).Elem()
	if isPointer {
		outValue.Set(refVal)
	} else {
		outValue.Set(refVal.Elem())
	}

	return nil
}

func fetchColumns(conn queryable, out any, query string, args ...any) error {
	outType := reflect.TypeOf(out)
	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("output must be pointer")
	}

	if outType.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("output must be slice")
	}

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

	rows, err := conn.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	if len(columns) != 1 {
		return fmt.Errorf("selected column must be one")
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

func fetchColumnOne(conn queryable, out any, query string, args ...any) error {
	outType := reflect.TypeOf(out)
	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("output must be pointer")
	}

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

	rows, err := conn.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	if len(columns) != 1 {
		return fmt.Errorf("selected column must be one")
	}
	columnTypeMap := getColumnTypeMap(columns)

	if !rows.Next() {
		return nil
	}
	var scan any
	err = rows.Scan(&scan)
	if err != nil {
		return err
	}

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

func fetchAsMap(conn queryable, query string, args ...any) ([]map[string]any, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	scans := make([]any, len(columns))
	scansPtr := make([]any, len(columns))
	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	columnTypeMap := getColumnTypeMap(columns)

	var result []map[string]any
	for rows.Next() {
		err := rows.Scan(scansPtr...)
		if err != nil {
			return nil, err
		}
		var newValue map[string]any = make(map[string]any)
		for i := range columns {
			val := parseDBValue(scans[i], columnTypeMap[i])
			newValue[columns[i].Name()] = val
		}
		result = append(result, newValue)
	}

	return result, nil
}
