package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	// Pure go SQLite driver, checkout https://github.com/glebarez/sqlite for details
)

func main() {
	// Force Setup time.Local to IST so we can deploy it to anywhere and not worry about it
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		panic(err.Error())
	}
	time.Local = loc

	dbLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             250 * time.Millisecond, // Slow SQL threshold
			LogLevel:                  logger.Info,            // Log level
			IgnoreRecordNotFoundError: false,                  // Ignore ErrRecordNotFound error for logger
			Colorful:                  true,                   // Disable color
		},
	)

	log.Println("Opening database for importing")
	db, err := gorm.Open(sqlite.Open("optionskaro-backtest.db"), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger:                 dbLogger,
	})
	log.Println("Running Migrations for our models")
	db.AutoMigrate(&Instrument{}, &TickData{})

	input := "/Users/ashwanth.kumar/Downloads/raw-options-data/monthly/24022022.csv"
	log.Printf("Starting to read the input file: %s\n", input)

	isMonthlyExpiry := strings.Contains(input, "/monthly/")
	isWeeklyExpiry := strings.Contains(input, "/weekly/")
	expiryDate, err := parseDateFromFileName(fileNameWithoutExt(input))
	handleError(err)

	tickerToInstrumentCache := make(map[string]Instrument)

	f, err := os.Open(input)
	if err != nil {
		log.Fatal(err)
	}
	records := CSVToMap(f)
	lastTicker := ""
	var recordsToInsert []TickData
	const bulkInsertBatchSize = 1000
	for _, r := range records {
		ticker := r["Ticker"]
		if strings.HasPrefix(ticker, "USDINR") {
			continue
		}
		if !strings.EqualFold(lastTicker, ticker) && len(recordsToInsert) > 0 {
			// batch insert the recordsToInsert
			result := db.CreateInBatches(recordsToInsert, bulkInsertBatchSize)
			if result.Error != nil {
				handleError(result.Error)
			}

			recordsToInsert = make([]TickData, bulkInsertBatchSize+1)
		}
		lastTicker = ticker

		oi, err := strconv.ParseFloat(r["Open Interest"], 64)
		handleError(err)
		volume, err := strconv.ParseFloat(r["Volume"], 64)
		handleError(err)

		instrument := Instrument{}

		if existingInstrument, ok := tickerToInstrumentCache[ticker]; ok {
			instrument = existingInstrument
		} else {
			instrument.Expiry = NullableTime(expiryDate)
			instrument.IsWeeklyExpiry = isWeeklyExpiry
			instrument.IsMonthlyExpiry = isMonthlyExpiry
			instrument.Symbol = ticker

			if isOption(ticker) {
				parts, err := optionStrikeFromTicker(ticker)
				handleError(err)
				instrument.Symbol = parts[0]

				strike, err := NullableInt32FromString(parts[1], 10)
				handleError(err)
				instrument.Strike = strike

				instrumentType, err := NewInstrumentType(parts[2])
				handleError(err)
				instrument.InstrumentType = instrumentType
				instrument.LotSize = NullableInt32(50)
			}

			if isFuture(ticker) {
				instrument.Symbol = symbolFromFut(ticker)
				instrument.InstrumentType = "FUT"
				instrument.LotSize = NullableInt32(50)
			}

			// NOTE: DONOT MOVE THIS ABOVE THE IF BLOCKS
			instrument.Underlying = findUnderlyingFromSymbol(instrument.Symbol)

			instrument.IsSpot = isSpot(ticker, oi, volume)

			result := db.Where(&instrument).FirstOrCreate(&instrument)
			if result.Error != nil {
				handleError(result.Error)
			}
			tickerToInstrumentCache[ticker] = instrument
		}

		// building the tick data
		dateTime, err := parseTime(r["Date/Time"])
		handleError(err)
		open, err := strconv.ParseFloat(r["Open"], 64)
		handleError(err)
		high, err := strconv.ParseFloat(r["High"], 64)
		handleError(err)
		low, err := strconv.ParseFloat(r["Low"], 64)
		handleError(err)
		close, err := strconv.ParseFloat(r["Close"], 64)
		handleError(err)

		// queryTickData := TickData{
		// 	InstrumentId: instrument.ID,
		// 	Timestamp:    dateTime,
		// }

		tickData := TickData{
			InstrumentId: instrument.ID,
			Timestamp:    dateTime,
			OI:           uint32(oi),
			Volume:       uint32(volume),
			Open:         open,
			High:         high,
			Low:          low,
			Close:        close,
		}

		recordsToInsert = append(recordsToInsert, tickData)

		if len(recordsToInsert) == bulkInsertBatchSize {
			result := db.Create(recordsToInsert)
			if result.Error != nil {
				handleError(result.Error)
			}

			recordsToInsert = make([]TickData, bulkInsertBatchSize+1)
		}
	}
	log.Println("Saved all the records into DB")
}

func findUnderlyingFromSymbol(symbol string) string {
	switch {
	case strings.EqualFold(symbol, "BANKNIFTY"):
		return "BANKNIFTY"
	case strings.EqualFold(symbol, "NIFTY"):
		return "NIFTY"
	default:
		return symbol
	}
}

var futureRegex = regexp.MustCompile(`^([A-Z]+)-FUT$`)

func symbolFromFut(input string) string {
	matches := futureRegex.FindStringSubmatch(input)
	fmt.Printf("%v\n", matches)
	return matches[1]
}

var optionsRegex = regexp.MustCompile(`^([A-Z]+)([0-9]+)(CE|PE)$`)

func optionStrikeFromTicker(input string) ([]string, error) {
	if !isOption(input) {
		return []string{}, errors.New(fmt.Sprintf("%s doesn't seem like a valid option symbol", input))
	}
	matches := optionsRegex.FindStringSubmatch(input)
	if len(matches) < 1 {
		fmt.Println("Input failed for: " + input)
		return []string{}, errors.New(fmt.Sprintf("%s doesn't seem like a valid option symbol", input))
	}
	// fmt.Printf("%v\n", matches)
	// [0] -> original string
	// [1] -> symbol
	// [2] -> strike
	// [3] -> CE/PE - Instrument Type
	return matches[1:], nil
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
	return optionsRegex.MatchString(ticker)
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
