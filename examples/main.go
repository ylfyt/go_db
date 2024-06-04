package go_db_test

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/ylfyt/go_db"
	// _ "github.com/lib/pq"
	// _ "modernc.org/sqlite"
)

func Test1() {
	connStr := "dist/example.db"
	db, err := go_db.New(connStr, go_db.Option{
		MaxOpenConn: 100,
		Driver:      "sqlite",
	})
	if err != nil {
		panic(err)
	}

	// _, err = db.Write(`
	// 	CREATE TABLE users(
	// 		id UUID
	// 	)
	// `)
	// if err != nil {
	// 	panic(err)
	// }

	// _, err = db.Write(`
	// 	INSERT INTO users VALUES(uuid())
	// `)
	// if err != nil {
	// 	panic(err)
	// }

	type User struct {
		Id uuid.UUID
	}
	var user []User
	err = db.Get(&user, `SELECT * FROM users LIMIT 1`)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Data: %+v\n", user)
}
