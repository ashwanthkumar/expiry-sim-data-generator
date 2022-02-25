package main

import (
	"database/sql"
	"strconv"
	"time"
)

func NullableInt32(input int32) sql.NullInt32 {
	return sql.NullInt32{Int32: input, Valid: true}
}

func NullableInt32FromString(input string, base int) (sql.NullInt32, error) {
	i, err := strconv.ParseInt(input, 10, 32)
	if err != nil {
		return sql.NullInt32{}, err
	}

	return sql.NullInt32{Int32: int32(i), Valid: true}, nil
}

func NullableTime(input time.Time) sql.NullTime {
	return sql.NullTime{Time: input, Valid: true}
}
