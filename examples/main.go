package main

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/ylfyt/go_db"
)

func main() {
	connStr := "postgresql://postgres:postgres@localhost/go_db?sslmode=disable"
	db, err := go_db.New(connStr, go_db.Option{
		MaxOpenConn: 100,
	})
	if err != nil {
		panic(err)
	}

	type User struct {
		Id uuid.UUID
	}
	var user *User
	err = db.GetFirst(&user, `SELECT * FROM users LIMIT 1`)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data: %+v\n", user)
}
