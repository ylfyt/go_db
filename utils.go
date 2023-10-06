package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"
)

func camelToSnake(camel string) string {
	var buf bytes.Buffer
	for i, char := range camel {
		if unicode.IsUpper(char) {
			if i > 0 {
				buf.WriteRune('_')
			}
			buf.WriteRune(unicode.ToLower(char))
		} else {
			buf.WriteRune(char)
		}
	}

	return buf.String()
}

func getTypeRef(val any) typeRef {
	switch val.(type) {
	case int:
		return type_INT
	case int32:
		return type_INT32
	case int64:
		return type_INT64
	case uint32:
		return type_UINT32
	case uint64:
		return type_UINT64
	case []byte:
		return type_BYTES
	case string:
		return type_STRING
	case bool:
		return type_BOOL
	case time.Time:
		return type_TIME
	case float32:
		return type_FLOAT32
	case float64:
		return type_FLOAT64
	case map[string]any:
		return type_MAP_STRING_ANY
	case map[string]int:
		return type_MAP_STRING_INT
	case map[string]string:
		return type_MAP_STRING_STRING
	case []int:
		return type_ARRAY_INT
	case []int32:
		return type_ARRAY_INT32
	case []int64:
		return type_ARRAY_INT64
	case []map[string]any:
		return type_ARRAY_MAP_STRING_ANY
	}
	return type_UNKNOWN
}

func getFieldTypeMap(refType reflect.Type) []typeRef {
	ref := reflect.New(refType).Elem()
	refTypes := make([]typeRef, ref.NumField())
	for i := 0; i < ref.NumField(); i++ {
		if !ref.Field(i).CanInterface() {
			continue
		}
		refTypes[i] = getTypeRef(ref.Field(i).Interface())
	}

	return refTypes
}

func getColumnTypeMap(columns []*sql.ColumnType) []typeRef {
	refTypes := make([]typeRef, len(columns))
	for i, v := range columns {
		var x typeRef
		switch v.DatabaseTypeName() {
		case "_INT4":
			x = type_ARRAY_INT
		case "_INT8":
			x = type_ARRAY_INT64
		case "CHAR", "VARCHAR", "NVARCHAR", "TEXT":
			x = type_STRING
		case "BOOL":
			x = type_BOOL
		case "INT4", "INT8":
			x = type_INT64
		case "FLOAT4":
			x = type_FLOAT32
		case "FLOAT8":
			x = type_FLOAT64
		case "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ":
			x = type_TIME
		case "JSON", "JSONB":
			x = type_JSON
		default:
			x = type_UNKNOWN
		}
		refTypes[i] = x
	}

	return refTypes
}

func getFieldIdxMap(columns []*sql.ColumnType, ref reflect.Type) []int {
	fields := map[string]int{}
	for i := 0; i < ref.NumField(); i++ {
		field := ref.Field(i)
		if !field.IsExported() {
			continue
		}
		columnName := field.Tag.Get("col")
		if columnName == "" {
			columnName = strings.Split(field.Tag.Get("json"), ",")[0]
		}
		if columnName == "" {
			columnName = camelToSnake(field.Name)
		}
		if columnName == "" {
			continue
		}
		fields[columnName] = i
	}

	fieldMap := make([]int, len(columns))
	for i, col := range columns {
		idx, exist := fields[col.Name()]
		if !exist {
			fieldMap[i] = -1
			continue
		}
		fieldMap[i] = idx
	}

	return fieldMap
}

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

func isPrimitiveType(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	default:
		return false
	}
}
