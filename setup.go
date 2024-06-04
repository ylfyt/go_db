package go_db

import (
	"database/sql"
	"time"
)

func New(connStr string, opts ...Option) (*DB, error) {
	if len(opts) == 0 {
		opts = append(opts, Option{
			MaxOpenConn:     DEFAULT_MAX_OPEN_CONN,
			MaxIdleConn:     DEFAULT_MAX_IDLE_CONN,
			MaxIdleLifeTime: DEFAULT_MAX_CONN_LIFE_TIME,
			Driver:          DEFAULT_DRIVER,
		})
	}
	opt := opts[0]

	conn, err := sql.Open(opt.Driver, connStr)
	if err != nil {
		return nil, err
	}

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
