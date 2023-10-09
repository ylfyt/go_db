package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func oldMethod(conn *sql.DB) {
	type Product struct {
		Id          string `json:"id"`
		StoreId     string `json:"store_id"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Price       string `json:"price"`
		CreatedAt   string `json:"created_at"`
		Detail      string `json:"detail"`
	}

	res, err := GetQuery(conn, `SELECT * FROM product`)
	if err != nil {
		fmt.Println("Err", err)
	}

	var products []Product
	err = json.Unmarshal(res, &products)
	if err != nil {
		fmt.Println("Err", err)
	}
	// fmt.Println("Data:", products)
}

func newMethod(conn *DB) {
	type Product struct {
		Id          int64     `json:"id"`
		StoreId     int64     `json:"store_id"`
		Name        string    `json:"name"`
		Description string    `json:"description"`
		Price       int64     `json:"price"`
		CreatedAt   time.Time `json:"created_at"`
		Detail      string    `json:"detail"`
	}

	var products []Product
	err := conn.Fetch(&products, `SELECT * FROM product`)
	if err != nil {
		fmt.Println("Err", err)
	}
	// fmt.Println("Data:", products)
}

func main() {
	connStr := "postgresql://postgres:postgres@localhost/db_product?sslmode=disable"
	conn, err := New(connStr, Option{
		MaxOpenConn: 100,
	})
	if err != nil {
		panic(err)
	}
	// oldMethod(conn)
	newMethod(conn)
}
