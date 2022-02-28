package utils

import (
	"log"
	"time"
)

func HandleError(err error) {
	if err != nil {
		log.Fatalf("%v\n", err)
	}
}

const dateFormat = "2006-01-02"

func UnixToDateFormat(input int64) string {
	return time.Unix(input, 0).Format(dateFormat)
}

func UnixTimeToDateFormat(input time.Time) string {
	return input.Format(dateFormat)
}
