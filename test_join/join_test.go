package test_join

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/ylfyt/go_db"
	_ "modernc.org/sqlite"
)

var db *go_db.DB

type User struct {
	Id       int
	Name     string
	Products []Product `key:"id"`
}

type Product struct {
	Id     int
	Name   string
	UserId int
	User   *User `key:"id,user_id"`
}

func createConn() error {
	// err := os.Remove("example.db")
	// if err != nil && !os.IsNotExist(err) {
	// 	return err
	// }
	connStr := "example.db"
	godb, err := go_db.New(connStr, go_db.Option{
		Driver: "sqlite",
	})
	if err != nil {
		return err
	}
	db = godb

	// db.Write(`CREATE TABLE users(id int, name varchar)`)
	// db.Write(`CREATE TABLE products(id int, name varchar, user_id int)`)
	return nil
}

func TestMain(m *testing.M) {
	err := createConn()
	if err != nil {
		panic(err)
	}

	// Run all tests
	exitCode := m.Run()

	// Perform teardown actions after running tests, if needed
	// ...

	// Exit with the appropriate exit code
	os.Exit(exitCode)
}

func TestJOIN(t *testing.T) {
	var users []User
	var products []Product

	// TODO
	// key -> data idx
	// change columnIdx -> fieldIdx to fieldIdx -> columnIdx for nestedMap

	// err := db.Get(&products, `
	// 	SELECT
	// 		p.id,
	// 		p.name,
	// 		p.user_id,
	// 		u.id AS user_id,
	// 		u.name AS user_name
	// 	FROM
	// 		products p
	// 		JOIN users u
	// 			ON u.id == p.user_id
	// `)
	err := db.Get(&users, `
		SELECT 
			u.id, 
			u.name,
			p.id AS products_id, 
			p.name AS products_name,
			u.id AS products_user_id
		FROM 
			users u JOIN products p 
				ON u.id = p.user_id			
	`)
	if err != nil {
		t.Error(err)
		return
	}

	j, _ := json.Marshal(users)
	j2, _ := json.Marshal(products)
	fmt.Printf("Data: %+v\n", string(j))
	fmt.Printf("Data: %+v\n", string(j2))
	t.Error("test")
}
