package main

import (
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

func New(connStr string, opts ...Option) (*DB, error) {
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if len(opts) == 0 {
		opts = append(opts, Option{
			MaxOpenConn:     DEFAULT_MAX_OPEN_CONN,
			MaxIdleConn:     DEFAULT_MAX_IDLE_CONN,
			MaxIdleLifeTime: DEFAULT_MAX_CONN_LIFE_TIME,
		})
	}

	opt := opts[0]
	if opt.MaxOpenConn > 0 {
		conn.SetMaxOpenConns(opt.MaxOpenConn)
	}
	if opt.MaxIdleConn > 0 {
		conn.SetMaxIdleConns(opt.MaxIdleConn)
	}
	if opt.MaxIdleLifeTime > 0 {
		conn.SetConnMaxIdleTime(time.Duration(opt.MaxIdleLifeTime * int(time.Second)))
	}

	return &DB{
		conn: conn,
	}, nil
}
