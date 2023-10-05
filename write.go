package main

func (me *DB) Write(query string, args ...any) (int, error) {
	res, err := me.conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affectedRows), nil
}
