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

func main() {
	connStr := "postgresql://postgres:postgres@localhost/coba?sslmode=disable"
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	var products []Product
	err = fetchSlice(&products, false, conn, "SELECT * FROM products LIMIT 2")
	if err != nil {
		panic(err)
	}

	// err = db.Fetch(&products)
	// if err != nil {
	// 	panic(err)
	// }
	fmt.Printf("Data: %+v\n", products)
}
