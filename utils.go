package main

import (
	"database/sql"
	"reflect"
	"strings"
	"time"
)

func getTypeRef(val any) typeRef {
	switch val.(type) {
	case int:
		return type_INT32
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
	case []int:
		return type_ARRAY_INT32
	case []int32:
		return type_ARRAY_INT32
	case []int64:
		return type_ARRAY_INT64
	case []map[string]any:
		return type_ARRAY_MAP_STRING_ANY
	}
	return type_UNKNOWN
}

func getFieldTypeMap(ref reflect.Value) []typeRef {
	ref = ref.Elem()
	refTypes := make([]typeRef, ref.NumField())
	for i := 0; i < ref.NumField(); i++ {
		refTypes[i] = getTypeRef(ref.Field(i).Interface())
	}

	return refTypes
}

func getColumnTypeMap(columns []*sql.ColumnType) []typeRef {
	refTypes := make([]typeRef, len(columns))
	for i, v := range columns {
		var x typeRef
		switch v.DatabaseTypeName() {
		case "_INT4", "_INT8":
			x = type_ARRAY_INT
		case "CHAR", "VARCHAR", "NVARCHAR", "TEXT":
			x = type_STRING
		case "BOOL":
			x = type_BOOL
		case "INT4":
			x = type_INT64
		case "INT8":
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
		columnName := field.Tag.Get("col")
		if columnName == "" {
			columnName = strings.Split(field.Tag.Get("json"), ",")[0]
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
