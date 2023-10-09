package main

import (
	"testing"
)

func BenchmarkFunctionOld(b *testing.B) {
	connStr := "postgresql://postgres:postgres@localhost/db_product?sslmode=disable"
	conn, err := Connect(connStr)
	conn.SetMaxOpenConns(100)
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		oldMethod(conn)
	}
}

func BenchmarkFunctionNew(b *testing.B) {
	connStr := "postgresql://postgres:postgres@localhost/db_product?sslmode=disable"
	conn, err := New(connStr, Option{
		MaxOpenConn: 100,
	})
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		newMethod(conn)
	}
}
