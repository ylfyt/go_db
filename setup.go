package main

import (
	"database/sql"
	"fmt"
	"reflect"

	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
}

type Option struct {
	MaxOpenConn int
}

func (me *DB) Test() error {
	return me.conn.Ping()
}

func (me *DB) Fetch(out any) error {
	outType := reflect.TypeOf(out)
	if outType.Kind() != reflect.Pointer {
		return fmt.Errorf("output must be pointer")
	}

	if outType.Elem().Kind() == reflect.Slice {
		sliceElemType := outType.Elem().Elem()

		if sliceElemType.Kind() == reflect.Pointer {
			fmt.Printf("Data: %+v\n", sliceElemType)
			return nil
		}
		fmt.Printf("s: %+v\n", sliceElemType)
		return nil
	}

	return nil
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
