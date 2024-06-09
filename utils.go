package go_db

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
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
	case uuid.UUID:
		return type_UUID
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
		// fmt.Println("DB TYPE:", v.DatabaseTypeName())
		switch v.DatabaseTypeName() {
		case "_INT4":
			x = type_ARRAY_INT
		case "_INT8":
			x = type_ARRAY_INT64
		case "CHAR", "VARCHAR", "NVARCHAR", "TEXT":
			x = type_STRING
		case "BOOL":
			x = type_BOOL
		case
			"INT",
			"INT4",
			"INT8",
			"BIGINT",
			"UNSIGNED INT",
			"UNSIGNED INT8",
			"UNSIGNED BIGINT",
			"UNSIGNED BIG INT",
			"UNSIGNED BIG INT8":
			x = type_INT64
		case "FLOAT4":
			x = type_FLOAT32
		case "FLOAT8", "NUMERIC":
			x = type_FLOAT64
		case "DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ":
			x = type_TIME
		case "JSON", "JSONB":
			x = type_JSON
		case "UUID":
			x = type_UUID
		default:
			x = type_UNKNOWN
		}
		refTypes[i] = x
	}

	return refTypes
}

func parseFieldName(f reflect.StructField) string {
	name := f.Tag.Get("col")
	if name == "-" {
		return ""
	}
	if name == "" {
		name = strings.Split(f.Tag.Get("json"), ",")[0]
	}
	if name == "" || name == "-" {
		name = camelToSnake(f.Name)
	}
	return name
}

func getFieldIdx(ref reflect.Type) map[string]int {
	fields := map[string]int{}
	for i := 0; i < ref.NumField(); i++ {
		field := ref.Field(i)
		if !field.IsExported() {
			continue
		}
		columnName := parseFieldName(field)
		fields[columnName] = i
	}
	return fields
}

type JoinInfo struct {
	Keys             []int
	FieldIdxToColIdx []int
	IsSlice          bool
	IsPointer        bool
	Type             reflect.Type
	FieldTypeMap     []typeRef
}

func getFieldIdxMap(columns []*sql.ColumnType, ref reflect.Type) ([]int, map[int]JoinInfo) {
	// fieldName -> fieldIdx
	fields := getFieldIdx(ref)

	// columnIdx -> fieldIdx
	fieldMap := make([]int, len(columns))
	for i, col := range columns {
		idx, exist := fields[col.Name()]
		if !exist {
			fieldMap[i] = -1
			continue
		}
		fieldMap[i] = idx
	}

	nestedMap := make(map[int]JoinInfo)
	for i := 0; i < ref.NumField(); i++ {
		field := ref.Field(i)
		if !field.IsExported() {
			continue
		}
		keyData := field.Tag.Get("key")
		if keyData == "" {
			continue
		}
		fieldColTag := parseFieldName(field)
		keys := strings.Split(keyData, ",")
		var idxs []int
		valid := true
		for _, key := range keys {
			columnIdx := findIdx(columns, func(i int) bool { return columns[i].Name() == key })
			if columnIdx == -1 {
				valid = false
				break
			}
			idxs = append(idxs, columnIdx)
		}
		if !valid {
			continue
		}
		isSlice := field.Type.Kind() == reflect.Slice
		isPointer := field.Type.Kind() == reflect.Pointer
		var ref reflect.Type
		if isSlice || isPointer {
			ref = field.Type.Elem()
		} else {
			ref = field.Type
		}
		if ref.Kind() != reflect.Struct {
			fmt.Printf("warning: join key only can be applied to type struct, *struct, or []struct (field:%s)\n", field.Name)
			continue
		}
		fieldIdxs := getFieldIdx(ref)
		fieldIdxToColIdx := make([]int, len(fieldIdxs))
		count := 0
		for fieldTag, fieldIdx := range fieldIdxs {
			columnIdx := findIdx(columns, func(i int) bool {
				return columns[i].Name() == fieldColTag+"_"+fieldTag
			})
			fieldIdxToColIdx[fieldIdx] = columnIdx
			if columnIdx != -1 {
				count++
			}
		}
		if count == 0 {
			continue
		}
		nestedMap[i] = JoinInfo{
			Keys:             idxs,
			FieldIdxToColIdx: fieldIdxToColIdx,
			IsSlice:          isSlice,
			Type:             ref,
			IsPointer:        isPointer,
		}
	}
	return fieldMap, nestedMap
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

// func rowScan(rows *sql.Rows, scansPtr []any, scans []any, elemType reflect.Type, fieldIdxMap []int, fieldTypeMap []typeRef, columnTypeMap []typeRef, columns []*sql.ColumnType) (*reflect.Value, error) {
// 	err := rows.Scan(scansPtr...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	refVal := reflect.New(elemType)
// 	err = parseRow(scans, fieldIdxMap, refVal, fieldTypeMap, columnTypeMap, columns)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &refVal, nil
// }

func isPrimitiveType(typ reflect.Type) bool {
	if typ.String() == "time.Time" {
		return true
	}
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String, reflect.Bool:
		return true
	case reflect.Slice:
		return isPrimitiveType(typ.Elem())
	default:
		return false
	}
}

func parseDbValue(val any, columnType typeRef) (any, error) {
	if columnType == type_UUID {
		if strVal, ok := val.(string); ok && len(strVal) > 0 {
			val, _ = uuid.Parse(strVal)
		} else if byteVal, ok := val.([]byte); ok && len(byteVal) > 0 {
			val, _ = uuid.ParseBytes(byteVal)
		} else {
			return nil, fmt.Errorf("cannot parse value for uuid column type (string or []byte only)")
		}
	}
	if columnType == type_ARRAY_INT {
		val = parseArray[int](val, func(s string) int { newEl, _ := strconv.Atoi(s); return newEl })
	}
	if columnType == type_ARRAY_INT64 {
		val = parseArray[int64](val, func(s string) int64 { newEl, _ := strconv.ParseInt(s, 10, 64); return newEl })
	}
	if columnType == type_FLOAT64 {
		if floatVal, ok := val.([]byte); ok {
			newVal, _ := strconv.ParseFloat(string(floatVal), 64)
			val = newVal
		}
	}
	if columnType == type_BOOL {
		if boolVal, ok := val.(bool); ok {
			return boolVal, nil
		}
		if intVal, ok := val.(int64); ok {
			return intVal > 0, nil
		}
		if intVal, ok := val.(int); ok {
			return intVal > 0, nil
		}
		if intVal, ok := val.(int32); ok {
			return intVal > 0, nil
		}
		return nil, fmt.Errorf("unsupported value for db type bool")
	}
	return val, nil
}

func findIdx[T any](data []T, selector func(i int) bool) int {
	for i := range data {
		if selector(i) {
			return i
		}
	}

	return -1
}
