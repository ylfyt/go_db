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

func (me *DB) Get(out any, query string, args ...any) error {
	return fetchSlice(me.conn, out, query, args...)
}

func (me *DB) FetchAsMap(query string, args ...any) ([]map[string]any, error) {
	return fetchAsMap(me.conn, query, args...)
}

func (me *DB) GetFirst(out any, query string, args ...any) error {
	return fetchStruct(me.conn, out, query, args...)
}

func (me *DB) Col(out any, query string, args ...any) error {
	return fetchColumns(me.conn, out, query, args...)
}

func (me *DB) ColFirst(out any, query string, args ...any) error {
	return fetchColumnOne(me.conn, out, query, args...)
}

func (me *DB) Write(query string, args ...any) (int, error) {
	return write(me.conn, query, args...)
}
