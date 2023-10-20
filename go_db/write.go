package go_db

func write(conn queryable, query string, args ...any) (int, error) {
	res, err := conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affectedRows), nil
}
