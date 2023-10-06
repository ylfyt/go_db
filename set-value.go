package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type mapParseType interface {
	map[string]int |
		map[string]int64 |
		map[string]any |
		map[string]string |
		[]map[string]int |
		[]map[string]int64 |
		[]map[string]any |
		[]map[string]string
}

func parseArray[T any](arrStr any, newValueCreator func(string) T) []T {
	data := strings.Split(strings.Trim(string(arrStr.([]byte)), "{}"), ",")
	newArr := []T{}
	for _, el := range data {
		if el == "" {
			continue
		}
		newEl := newValueCreator(el)
		newArr = append(newArr, newEl)
	}
	return newArr
}

func setJsonToMap[T mapParseType](val any, field reflect.Value) error {
	var tmpVal T
	err := json.Unmarshal(val.([]byte), &tmpVal)
	if err != nil {
		return err
	}
	field.Set(reflect.ValueOf(tmpVal))
	return nil
}

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
	if columnType == type_ARRAY_INT {
		val = parseArray[int](val, func(s string) int { newEl, _ := strconv.Atoi(s); return newEl })
	}
	if columnType == type_ARRAY_INT64 {
		val = parseArray[int64](val, func(s string) int64 { newEl, _ := strconv.ParseInt(s, 10, 64); return newEl })
	}
	if fieldType == columnType {
		field.Set(reflect.ValueOf(val))
		return nil
	}

	fieldTypeName := field.Type().Name()
	if columnType == type_JSON {
		if fieldType == type_MAP_STRING_ANY {
			return setJsonToMap[map[string]any](val, field)
		}
		if fieldType == type_MAP_STRING_INT {
			return setJsonToMap[map[string]int](val, field)
		}
		if fieldType == type_MAP_STRING_STRING {
			return setJsonToMap[map[string]string](val, field)
		}
		if fieldType == type_ARRAY_MAP_STRING_ANY {
			return setJsonToMap[[]map[string]string](val, field)
		}
		if fieldType == type_STRING {
			tmpVal := string(val.([]byte))
			field.SetString(tmpVal)
			return nil
		}
		return fmt.Errorf("cannot convert db type 'json' to '%s'", field.Type())
	}
	if columnType == type_INT64 {
		if fieldType == type_INT {
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
