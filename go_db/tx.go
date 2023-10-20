package go_db

import (
	"database/sql"
	"errors"
	"fmt"
)

type TX struct {
	conn      *sql.Tx
	hasError  bool
	isCommied bool
}

func (me *TX) Get(out any, query string, args ...any) error {
	if me.isCommied {
		return fmt.Errorf("transaction already commited")
	}
	if me.hasError {
		return fmt.Errorf("there is some error in transaction")
	}
	err := fetchSlice(me.conn, out, query, args...)
	if err != nil {
		me.hasError = true
	}
	return err
}

func (me *TX) FetchAsMap(query string, args ...any) ([]map[string]any, error) {
	if me.isCommied {
		return nil, fmt.Errorf("transaction already commited")
	}
	if me.hasError {
		return nil, fmt.Errorf("there is some error in transaction")
	}
	res, err := fetchAsMap(me.conn, query, args...)
	if err != nil {
		me.hasError = true
	}
	return res, err
}

func (me *TX) GetFirst(out any, query string, args ...any) error {
	if me.isCommied {
		return fmt.Errorf("transaction already commited")
	}
	if me.hasError {
		return fmt.Errorf("there is some error in transaction")
	}
	err := fetchStruct(me.conn, out, query, args...)
	if err != nil {
		me.hasError = true
	}
	return err
}

func (me *TX) Col(out any, query string, args ...any) error {
	if me.isCommied {
		return fmt.Errorf("transaction already commited")
	}
	if me.hasError {
		return fmt.Errorf("there is some error in transaction")
	}
	err := fetchColumns(me.conn, out, query, args...)
	if err != nil {
		me.hasError = true
	}
	return err
}

func (me *TX) ColFirst(out any, query string, args ...any) error {
	if me.isCommied {
		return fmt.Errorf("transaction already commited")
	}
	if me.hasError {
		return fmt.Errorf("there is some error in transaction")
	}
	err := fetchColumnOne(me.conn, out, query, args...)
	if err != nil {
		me.hasError = true
	}
	return err
}

func (me *TX) Write(query string, args ...any) (int, error) {
	if me.isCommied {
		return 0, fmt.Errorf("transaction already commited")
	}
	if me.hasError {
		return 0, fmt.Errorf("there is some error in transaction")
	}
	n, err := write(me.conn, query, args...)
	if err != nil {
		me.hasError = true
	}
	return n, err
}

// Commit the transaction.
// Just call commit with defer, commit will not be called if there are some error
func (me *TX) Commit() error {
	if me.isCommied {
		return fmt.Errorf("transaction already commited")
	}

	if me.hasError {
		return errors.New("there is some error in transaction")
	}

	err := me.conn.Commit()
	if err != nil {
		me.hasError = true
		return err
	}
	me.isCommied = true
	return nil
}

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
