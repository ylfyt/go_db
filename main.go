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
	db, err := New(connStr, Option{
		MaxOpenConn:     10,
		MaxIdleConn:     1,
		MaxConnLifeTime: 5,
	})
	if err != nil {
		panic(err)
	}

	var product Product
	err = db.Fetch(&product, `SELECT * FROM products`)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data: %+v\n", product)

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Done()

	affected, err := tx.Write(`INSERT INTO products(name, description, user_id) VALUES('Budi', 'test budi', 3)`)
	if err != nil {
		panic(err)
	}

	fmt.Println("affected", affected)
	// tx.Commit()
}
