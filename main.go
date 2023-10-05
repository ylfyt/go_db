package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Product struct {
	Id     int    `col:"id"`
	Name   string `col:"name"`
	Desc   string `col:"description"`
	UserId int    `col:"user_id"`
}

var conn *sql.DB

func testSlice() {
	var products []Product
	err := fetchSlice(&products, conn, "SELECT * FROM products LIMIT 2")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data: %+v\n", products)
}

func testStruct() {
	var products *Product
	err := fetchStruct(&products, conn, "SELECT * FROM products LIMIT 1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data: %+v\n", products)
}

func main() {
	connStr := "postgresql://postgres:postgres@localhost/coba?sslmode=disable"
	connTmp, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	conn = connTmp

	// testSlice()
	testStruct()
}
