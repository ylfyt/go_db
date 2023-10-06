package main

type typeRef int

const (
	type_UNKNOWN typeRef = iota + 0
	type_INT
	type_INT32
	type_INT64
	type_UINT
	type_UINT32
	type_UINT64
	type_BYTES
	type_TIME // time.Time
	type_JSON
	type_BOOL
	type_STRING
	type_MAP_STRING_ANY
	type_MAP_STRING_INT
	type_MAP_STRING_STRING
	type_FLOAT32
	type_FLOAT64
	type_ARRAY_INT
	type_ARRAY_INT32
	type_ARRAY_INT64
	type_ARRAY_UINT
	type_ARRAY_UINT32
	type_ARRAY_UINT64
	type_ARRAY_STRING
	type_ARRAY_MAP_STRING_ANY
)
