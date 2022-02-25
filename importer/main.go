package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	// Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details
)

func main() {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		panic(err.Error())
	}
	time.Local = loc

	// db, err := gorm.Open(sqlite.Open("optionskaro-backtest.db"), &gorm.Config{})

	input := "/Users/ashwanth.kumar/Downloads/raw-options-data/monthly/24022022.csv"
	f, err := os.Open(input)
	if err != nil {
		log.Fatal(err)
	}
	records := CSVToMap(f)
	for _, r := range records {
		ticker := r["Ticker"]
		dateTime, err := parseTime(r["Date/Time"])
		handleError(err)
		fmt.Printf("%v\n", dateTime)

		oi, err := strconv.ParseFloat(r["Open Interest"], 64)
		handleError(err)

		volume, err := strconv.ParseFloat(r["Volume"], 64)
		handleError(err)

		if isSpot(ticker, oi, volume) {
			fmt.Printf("%v\n", r)
		}
	}
}

func parseTime(input string) (time.Time, error) {
	return dateparse.ParseIn(strings.ReplaceAll(input, "-", "/"), time.Local, dateparse.PreferMonthFirst(false))
}

func handleError(err error) {
	if err != nil {
		log.Fatalf("%v\n", err)
	}
}

func isOption(ticker string) bool {
	return strings.HasSuffix(ticker, "CE") || strings.HasSuffix(ticker, "PE")
}

func isFuture(ticker string) bool {
	return strings.HasSuffix(ticker, "FUT")
}

func isDerivative(ticker string) bool {
	return isOption(ticker) || isFuture(ticker)
}

func isSpot(ticker string, oi float64, volume float64) bool {
	return !isDerivative(ticker) && oi == 0.0 && volume == 0.0
}

func CSVToMap(reader io.Reader) []map[string]string {
	r := csv.NewReader(reader)
	rows := []map[string]string{}
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return rows
}
