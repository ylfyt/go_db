package main

import (
	"fmt"
)

type Product struct {
	Id          int
	Name        string
	Description string
	UserId      int
}

func main() {
	connStr := "postgresql://postgres:postgres@localhost/coba?sslmode=disable"
	db, err := New(connStr, nil)
	if err != nil {
		panic(err)
	}

	var product []Product
	err = db.Fetch(&product, `SELECT * FROM products`)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data: %+v\n", product)
}
