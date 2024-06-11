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
	Id       int `key:"1"`
	Name     string
	Products []Product `join:"true"`
	Store    *Store    `join:"true"`
	Posts    []Post    `join:"true"`
}

type Product struct {
	Id     int `key:"1"`
	Name   string
	UserId int
	User   *User
}

type Store struct {
	Id     int `key:"1"`
	UserId int
	Name   string
}

type Post struct {
	Id       int `key:"1"`
	Title    string
	AuthorId int
	Author   *User
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

	err := db.Get(&products, `
		SELECT
			p.id,
			p.name,
			p.user_id,
			u.id AS user_id,
			u.name AS user_name
		FROM
			products p
			JOIN users u
				ON u.id == p.user_id
	`)
	// err := db.Get(&users, `
	// 	SELECT
	// 		u.id,
	// 		u.name,
	// 		p.id AS products_id,
	// 		p.name AS products_name,
	// 		p.user_id AS products_user_id,
	// 		s.id AS store_id,
	// 		s.name AS store_name,
	// 		s.user_id AS store_user_id,
	// 		po.id AS posts_id,
	// 		po.title AS posts_title,
	// 		po.author_id AS posts_author_id
	// 	FROM
	// 		users u
	// 		JOIN products p ON u.id = p.user_id
	// 		JOIN store s ON u.id = s.user_id
	// 		JOIN posts po ON u.id = po.author_id
	// 	WHERE u.id = 2
	// `)
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
