package go_db

import "database/sql"

type DB struct {
	conn *sql.DB
}

type Option struct {
	MaxOpenConn     int
	MaxIdleConn     int
	MaxIdleLifeTime int // in seconds
	Driver          string
}

const (
	DEFAULT_MAX_OPEN_CONN      = 100
	DEFAULT_MAX_IDLE_CONN      = 10
	DEFAULT_MAX_CONN_LIFE_TIME = 300
	DEFAULT_DRIVER             = "postgres"
)

// Fetch data with multiple rows. The output must be slice
//
//	var users []User
//	err := db.Get(&users, query, args...)
//
// go_db using 3 naming strategies:
//
// 1. 'col' tag (first priority)
//
//	FirstName string `json:"firstName" col:"first_name"`
//
// 2. 'json' tag (second priority)
//
//	FirstName string `json:"first_name"`
//
// 3. field name with convertion from camelCase to snake_case (last priority)
//
//	FirstName string // will be coverted to first_name
func (me *DB) Get(out any, query string, args ...any) error {
	return fetchSlice(me.conn, out, query, args...)
}

// Fetch data with multiple rows and transform it into slice of map[string]any
//
//	users, err := db.GetAsMap(&users, query, args...)
func (me *DB) GetAsMap(query string, args ...any) ([]map[string]any, error) {
	return fetchAsMap(me.conn, query, args...)
}

// Fetch data with single rows. The output must be struct. The output will be nil if there is no row
//
//	var user *User
//	err := db.GetFirst(&user, query, args...)
func (me *DB) GetFirst(out any, query string, args ...any) error {
	return fetchStruct(me.conn, out, query, args...)
}

// Fetch only the fist column of the query. The output must be slice of primitive data types such as int, string, bool or float
//
//	var names []string
//	err := db.Col(&names, query, args...)
func (me *DB) Col(out any, query string, args ...any) error {
	return fetchColumns(me.conn, out, query, args...)
}

// Fetch only the fist column and row of the query. The output must be a primitive data type such as int, string, bool or float
//
//	var count int
//	err := db.ColFirst(&count, `SELECT count(1) FROM users`)
//
//	var name *string
//	err := db.ColFirst(&name, `SELECT name FROM users WHERE id = 1`)
func (me *DB) ColFirst(out any, query string, args ...any) error {
	return fetchColumnOne(me.conn, out, query, args...)
}

// Execute write operation to the database
//
//	affectedRows, err := db.Write(query, args...)
func (me *DB) Write(query string, args ...any) (int, error) {
	return write(me.conn, query, args...)
}

// Begin transaction
//
//	tx, err := db.Begin()
//	affectedRows, err := tx.Write(query, args...)
//	tx.Commit()
func (me *DB) Begin() (*TX, error) {
	tx, err := me.conn.Begin()
	if err != nil {
		return nil, err
	}

	return &TX{
		conn:     tx,
		hasError: false,
	}, nil
}
