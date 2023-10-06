package main

import "database/sql"

type DB struct {
	conn *sql.DB
}

type Option struct {
	MaxOpenConn     int
	MaxIdleConn     int
	MaxIdleLifeTime int // in seconds
}

const (
	DEFAULT_MAX_OPEN_CONN      = 100
	DEFAULT_MAX_IDLE_CONN      = 10
	DEFAULT_MAX_CONN_LIFE_TIME = 300
)

func (me *DB) Fetch(out any, query string, args ...any) error {
	return fetch(me.conn, out, query, args...)
}

func (me *DB) Write(query string, args ...any) (int, error) {
	return write(me.conn, query, args...)
}
