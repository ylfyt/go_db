package main

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
}

type Option struct {
	MaxOpenConn int
}


func New(connStr string, opt *Option) (*DB, error) {
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if opt == nil {
		opt = &Option{}
	}
	if opt.MaxOpenConn > 0 {
		conn.SetMaxOpenConns(opt.MaxOpenConn)
	}

	return &DB{
		conn: conn,
	}, nil
}
