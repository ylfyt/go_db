package pq_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/ylfyt/go_db"
	_ "modernc.org/sqlite"
)

var db *go_db.DB

func createConn() error {
	connStr := "postgresql://postgres:postgres@localhost?sslmode=disable"
	goDb, err := go_db.New(connStr, go_db.Option{
		Driver: "postgres",
	})
	if err != nil {
		return err
	}
	_, err = goDb.Write("DROP DATABASE IF EXISTS go_db_test")
	if err != nil {
		return err
	}
	_, err = goDb.Write("CREATE DATABASE go_db_test")
	if err != nil {
		return err
	}

	connStrTest := "postgresql://postgres:postgres@localhost/go_db_test?sslmode=disable"
	goDb2, err := go_db.New(connStrTest, go_db.Option{
		Driver: "postgres",
	})
	if err != nil {
		return err
	}
	db = goDb2
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

func TestUUID(t *testing.T) {
	_, err := db.Write(`
		CREATE TABLE uuid_t(
			id UUID
		)
	`)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = db.Write(`
		INSERT INTO uuid_t VALUES('0e29d730-1d37-4c59-bb57-e999231123ad')
	`)
	if err != nil {
		t.Error(err)
		return
	}

	type User struct {
		Id uuid.UUID
	}
	var user *User
	err = db.GetFirst(&user, `SELECT * FROM uuid_t LIMIT 1`)
	if err != nil {
		t.Error(err)
		return
	}
	if user == nil || user.Id.String() != "0e29d730-1d37-4c59-bb57-e999231123ad" {
		t.Error("not valid", user)
		return
	}
	fmt.Printf("Data: %+v\n", user)
}

func TestVARCHAR(t *testing.T) {
	_, err := db.Write(`
		CREATE TABLE text_t(
			c1 VARCHAR,
			c2 TEXT
		)
	`)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = db.Write(`
		INSERT INTO text_t VALUES('budi', 'andi')
	`)
	if err != nil {
		t.Error(err)
		return
	}

	type User struct {
		C1  string
		C_2 string `col:"c2"`
	}
	var user *User
	err = db.GetFirst(&user, `SELECT * FROM text_t LIMIT 1`)
	if err != nil {
		t.Error(err)
		return
	}
	if user == nil || user.C1 != "budi" || user.C_2 != "andi" {
		t.Error("not valid", user)
		return
	}
	fmt.Printf("Data: %+v\n", user)
}

func TestINT(t *testing.T) {
	_, err := db.Write(`
		CREATE TABLE int_t(
			c1 INT,
			c2 INT8,
			c3 BIGINT
		)
	`)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = db.Write(`
		INSERT INTO int_t VALUES($1, $2, $3)
	`, 10, 20, 30)
	if err != nil {
		t.Error(err)
		return
	}

	type User struct {
		C1 int
		C2 int64
		C3 int
	}
	var user *User
	err = db.GetFirst(&user, `SELECT * FROM int_t LIMIT 1`)
	if err != nil {
		t.Error(err)
		return
	}
	if user == nil || user.C1 != 10 || user.C2 != 20 || user.C3 != 30 {
		t.Error("not valid", user)
		return
	}
	fmt.Printf("Data: %+v\n", user)
}

func TestDate(t *testing.T) {
	_, err := db.Write(`
		CREATE TABLE date_t(
			c1 DATE,
			c2 TIMESTAMP WITH TIME ZONE
		)
	`)
	if err != nil {
		t.Error(err)
		return
	}

	t1 := time.Now()
	t2 := time.Now()
	_, err = db.Write(`
		INSERT INTO date_t VALUES($1, $2)
	`, t1, t2)
	if err != nil {
		t.Error(err)
		return
	}

	type Res struct {
		C1 time.Time
		C2 time.Time
	}
	var res *Res
	err = db.GetFirst(&res, `SELECT * FROM date_t LIMIT 1`)
	if err != nil {
		t.Error(err)
		return
	}
	c1 := time.Date(t1.Year(), t1.Month(), t1.Day(), 0, 0, 0, 0, time.UTC)
	if res == nil || res.C1.Unix() != c1.Unix() || res.C2.Unix() != t2.Unix() {
		t.Error("not valid", res, c1.Unix())
		return
	}
	fmt.Printf("Data: %+v\n", res)
}

func TestBool(t *testing.T) {
	_, err := db.Write(`
		CREATE TABLE bool_t(
			c1 BOOL,
			c2 BOOL
		)
	`)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = db.Write(`
		INSERT INTO bool_t VALUES($1, $2)
	`, true, false)
	if err != nil {
		t.Error(err)
		return
	}

	type Res struct {
		C1 bool
		C2 bool
	}
	var res *Res
	err = db.GetFirst(&res, `SELECT * FROM bool_t LIMIT 1`)
	if err != nil {
		t.Error(err)
		return
	}
	if res == nil || res.C1 != true || res.C2 != false {
		t.Errorf("not valid %+v", res)
		return
	}
	fmt.Printf("Data: %+v\n", res)
}
