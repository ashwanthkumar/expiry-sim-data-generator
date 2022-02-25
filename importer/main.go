package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	// Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details
)

func main() {
	// Force Setup time.Local to IST so we can deploy it to anywhere and not worry about it
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		panic(err.Error())
	}
	time.Local = loc

	// db, err := gorm.Open(sqlite.Open("optionskaro-backtest.db"), &gorm.Config{})

	input := "/Users/ashwanth.kumar/Downloads/raw-options-data/monthly/24022022.csv"
	isMonthlyExpiry := strings.Contains(input, "/monthly/")
	isWeeklyExpiry := strings.Contains(input, "/weekly/")
	expiryDate, err := parseDateFromFileName(fileNameWithoutExt(input))
	handleError(err)

	f, err := os.Open(input)
	if err != nil {
		log.Fatal(err)
	}
	records := CSVToMap(f)
	for _, r := range records {
		ticker := r["Ticker"]
		dateTime, err := parseTime(r["Date/Time"])
		handleError(err)

		oi, err := strconv.ParseFloat(r["Open Interest"], 64)
		handleError(err)

		volume, err := strconv.ParseFloat(r["Volume"], 64)
		handleError(err)

		is_spot := isSpot(ticker, oi, volume)

	}
}

func fileNameWithoutExt(input string) string {
	filename := filepath.Base(input)
	fileExtIfAny := filepath.Ext(input)
	return strings.ReplaceAll(filename, fileExtIfAny, "")
}

func parseTime(input string) (time.Time, error) {
	return dateparse.ParseIn(strings.ReplaceAll(input, "-", "/"), time.Local, dateparse.PreferMonthFirst(false))
}

func parseDateFromFileName(fileNameWithoutExt string) (time.Time, error) {
	// 28082020 -- first 2 is date, next 2 is month and next 4 is year
	date, err := strconv.ParseInt(fileNameWithoutExt[0:2], 10, 16)
	handleError(err)
	month, err := strconv.ParseInt(fileNameWithoutExt[2:4], 10, 16)
	handleError(err)
	year, err := strconv.ParseInt(fileNameWithoutExt[4:], 10, 16)
	handleError(err)
	return dateparse.ParseLocal(fmt.Sprintf("%d-%d-%d", year, month, date))
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
