package main

import "github.com/ylfyt/go_db/go_db"

func main() {
	connStr := "postgresql://postgres:postgres@localhost/db_product?sslmode=disable"
	conn, err := go_db.New(connStr, go_db.Option{
		MaxOpenConn: 100,
	})
	if err != nil {
		panic(err)
	}
	_ = conn
}
