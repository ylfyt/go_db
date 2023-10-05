package main

import (
	"fmt"
)

type Product struct {
	Id     int    `col:"id"`
	Name   string `col:"name"`
	Desc   string `col:"description"`
	UserId int    `col:"user_id"`
}

func main() {
	connStr := "postgresql://postgres:postgres@localhost/coba?sslmode=disable"
	db, err := New(connStr, nil)
	if err != nil {
		panic(err)
	}

	var product *int64
	err = db.Fetch(&product, `SELECT id FROM products WHERE id = 10`)
	if err != nil {
		panic(err)
	}

	fmt.Println("Data:", product)
}
