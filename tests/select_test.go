package tests

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/ylfyt/go_db"
	_ "modernc.org/sqlite"
)

var db *go_db.DB

func TestMain(m *testing.M) {
	err := os.Remove("example.db")
	if err != nil {
		panic(err)
	}
	connStr := "example.db"
	godb, err := go_db.New(connStr, go_db.Option{
		MaxOpenConn: 100,
		Driver:      "sqlite",
	})
	if err != nil {
		panic(err)
	}
	db = godb

	// Run all tests
	exitCode := m.Run()

	// Perform teardown actions after running tests, if needed
	// ...

	// Exit with the appropriate exit code
	os.Exit(exitCode)
}

func TestSelect(t *testing.T) {
	_, err := db.Write(`
		CREATE TABLE users(
			id UUID
		)
	`)
	if err != nil {
		t.Error(err)
	}

	_, err = db.Write(`
		INSERT INTO users VALUES('0e29d730-1d37-4c59-bb57-e999231123ad')
	`)
	if err != nil {
		t.Error(err)
	}

	type User struct {
		Id uuid.UUID
	}
	var user []User
	err = db.Get(&user, `SELECT * FROM users LIMIT 1`)
	if err != nil {
		t.Error()
	}
	fmt.Printf("Data: %+v\n", user)
}
