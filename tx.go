package main

import (
	"database/sql"
	"errors"
)

type TX struct {
	conn       *sql.Tx
	isCommited bool
}

func (me *TX) Fetch(out any, query string, args ...any) error {
	return fetch(me.conn, out, query, args...)
}

func (me *TX) Write(query string, args ...any) (int, error) {
	return write(me.conn, query, args...)
}

// 'Done' function is recommended to call using defer after 'Begin' the trx.
// It will call 'Rollback' automaticlly if trx is not commited
func (me *TX) Done() error {
	if !me.isCommited {
		return me.conn.Rollback()
	}
	return nil
}

// Commit the transaction.
// Done function will call 'Rollback' automaticlly if this function is not called
func (me *TX) Commit() error {
	if me.isCommited {
		return errors.New("already commited")
	}

	err := me.conn.Commit()
	if err != nil {
		return err
	}
	me.isCommited = true
	return nil
}

func (me *DB) Begin() (*TX, error) {
	tx, err := me.conn.Begin()
	if err != nil {
		return nil, err
	}

	return &TX{
		conn:       tx,
		isCommited: false,
	}, nil
}
