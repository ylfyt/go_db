package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func setValue(field reflect.Value, fieldType typeRef, val any, columnType typeRef) error {
	if fieldType == type_UNKNOWN {
		return fmt.Errorf("unknown type '%s'", field.Type())
	}
	if columnType == type_UNKNOWN {
		return fmt.Errorf("unknown db type")
	}
	if val == nil {
		field.Set(reflect.New(field.Type()).Elem())
		return nil
	}
	if fieldType == columnType {
		field.Set(reflect.ValueOf(val))
		return nil
	}

	fieldTypeName := field.Type().Name()
	if columnType == type_JSON {
		if fieldType == type_MAP_STRING_ANY {
			var tmpVal map[string]any
			err := json.Unmarshal(val.([]byte), &tmpVal)
			if err != nil {
				return err
			}
			field.Set(reflect.ValueOf(tmpVal))
			return nil
		}
		if fieldType == type_ARRAY_MAP_STRING_ANY {
			var tmpVal []map[string]any
			err := json.Unmarshal(val.([]byte), &tmpVal)
			if err != nil {
				return err
			}
			field.Set(reflect.ValueOf(tmpVal))
			return nil
		}
		if fieldType == type_STRING {
			tmpVal := string(val.([]byte))
			field.SetString(tmpVal)
			return nil
		}
		return fmt.Errorf("cannot convert db type 'json' to '%s'", field.Type())
	}
	if columnType == type_ARRAY_INT {
		data := strings.Split(strings.Trim(string(val.([]byte)), "{}"), ",")
		if fieldType == type_ARRAY_INT32 {
			values := make([]int32, len(data))
			for i, val := range data {
				val, _ := strconv.Atoi(val)
				values[i] = int32(val)
			}
			field.Set(reflect.ValueOf(values))
		} else {
			values := make([]int64, len(data))
			for i, val := range data {
				val, _ := strconv.ParseInt(val, 10, 64)
				values[i] = val
			}
			field.Set(reflect.ValueOf(values))
		}
		return nil
	}
	if columnType == type_INT64 {
		if fieldType == type_INT32 {
			field.SetInt(val.(int64))
			return nil
		}
		return fmt.Errorf("cannot convert db type 'int64' to '%s'", fieldTypeName)
	}
	if columnType == type_STRING {
		return fmt.Errorf("cannot convert db type 'string' to '%s'", fieldTypeName)
	}
	if columnType == type_BOOL {
		return fmt.Errorf("cannot convert db type 'bool' to '%s'", fieldTypeName)
	}
	if columnType == type_FLOAT32 {
		return fmt.Errorf("cannot convert db type 'float32' to '%s'", fieldTypeName)
	}
	if columnType == type_FLOAT64 {
		return fmt.Errorf("cannot convert db type 'float64' to '%s'", fieldTypeName)
	}
	if columnType == type_TIME {
		return fmt.Errorf("cannot convert db type 'time' to '%s'", fieldTypeName)
	}

	return nil
}
