package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
)

type TickData struct {
	Open   float64
	High   float64
	Low    float64
	Close  float64
	OI     int64
	Volume int64
}

func NewTickDataFromSlice(input []string) TickData {
	if len(input) != 6 {
		err := errors.New("input slice is not of size 6. It should be of the format: [Open, High, Low, Close, Open Interest, Volume]")
		handleError(err)
	}
	open, err := strconv.ParseFloat(input[0], 64)
	handleError(err)
	high, err := strconv.ParseFloat(input[1], 64)
	handleError(err)
	low, err := strconv.ParseFloat(input[2], 64)
	handleError(err)
	close, err := strconv.ParseFloat(input[3], 64)
	handleError(err)
	oi, err := strconv.ParseInt(input[4], 10, 64)
	handleError(err)
	volume, err := strconv.ParseInt(input[5], 10, 64)
	handleError(err)

	return TickData{
		Open:   open,
		High:   high,
		Low:    low,
		Close:  close,
		OI:     oi,
		Volume: volume,
	}
}
func (t TickData) ValueToUseForCompute() float64 {
	return t.Close
}
func (t TickData) ToSlice(timerTick int64) []float64 {
	return []float64{
		float64(timerTick), // time in seconds since epoch
		t.Open,
		t.High,
		t.Low,
		t.Close,
		float64(t.OI),
		float64(t.Volume),
	}
}

func main() {
	input := "/Users/ashwanth.kumar/Downloads/raw-options-data/weekly/22072021.csv"
	f, err := os.Open(input)
	handleError(err)
	defer f.Close()
	records := CSVToMap(f)

	// this is a nested map which has the following structure
	// outer map's key is of type "time.Time" that denotes
	// the time of the tick.
	// The inner map's key denotes the ticker name (NIFTY, INDIAVIX, etc.)
	// we would ideally want to query all the columns for a given time tick,
	// hence this format of choice.
	columnarData := make(map[int64]map[string]TickData)

	columnNames := readData(records, columnarData)
	log.Printf("built in-memory collection of nifty records from: %v\n", columnNames)

	allTimeTicksForTheCurrentExpiry := buildTimeTicksFromColumns(columnarData)
	// given each file contains all the data to expiry the last time in a sorted slice will be the expiry time that we need
	expiryDate := truncateToDay(time.Unix(allTimeTicksForTheCurrentExpiry[len(allTimeTicksForTheCurrentExpiry)-1], 0))

	unitTime := 5 * time.Minute
	columnToPick := "NIFTY15700CE"

	ohlcDataByUnitTime, ohlcDataTicks := ohlcDataGroupedByFor(allTimeTicksForTheCurrentExpiry, columnarData, columnToPick, unitTime)

	ticks := [][]float64{}
	for _, tick := range ohlcDataTicks {
		tickData := ohlcDataByUnitTime[tick]
		ticks = append(ticks, tickData.ToSlice(tick))
	}
	output := make(map[string]interface{})
	output["ticker"] = columnToPick
	output["expiryDate"] = expiryDate.Format("2006-01-02")
	output["tf_minutes"] = unitTime.Minutes()
	output["data"] = ticks
	file, err := json.Marshal(output)
	handleError(err)

	fileName := fmt.Sprintf("%d%s.json", expiryDate.Year(), columnToPick)
	err = ioutil.WriteFile(fileName, file, 0644)
	handleError(err)

	// const minStrikeDistance float64 = 50
	// for _, tick := range allTimeTicksForTheCurrentExpiry {
	// 	columns := columnarData[tick]
	// 	tickTime := time.Unix(tick, 0)
	// 	vix, atmStraddle := vixAndAtmStraddle(columns, minStrikeDistance)
	// 	fmt.Printf("%s - %f - %f\n", tickTime, vix, atmStraddle)
	// }
}

func truncateToDay(t time.Time) time.Time {
	nt, err := time.Parse("2006-01-02", t.Format("2006-01-02"))
	handleError(err)
	return nt
}

// this should help us build ohlc data by any unit of time (5 minutes, 15 minutes, 1 hour, etc.)
func ohlcDataGroupedByFor(allTimeTicksForTheCurrentExpiry []int64, columnarData map[int64]map[string]TickData, symbolToPick string, groupedTimeUnit time.Duration) (map[int64]TickData, []int64) {
	ohlcDataByUnitTime := make(map[int64]TickData)
	ohlcDataTicks := []int64{}

	for _, tick := range allTimeTicksForTheCurrentExpiry {
		columns := columnarData[tick]
		newTick, present := columns[symbolToPick]
		if !present {
			// we want to skip updating our ohlc when we don't have a corresponding record in our source
			continue
		}

		tickTime := time.Unix(tick, 0)
		roundedTickTime := tickTime.Truncate(groupedTimeUnit).Unix()
		tickData, present := ohlcDataByUnitTime[roundedTickTime]
		if !present {
			tickData = TickData{
				Open:   0,
				High:   0,
				Low:    math.MaxFloat64,
				Close:  0,
				OI:     0,
				Volume: 0,
			}
			tickData.Open = newTick.Open
		}

		if newTick.High > tickData.High {
			tickData.High = newTick.High
		}
		if newTick.Low < tickData.Low {
			tickData.Low = newTick.Low
		}
		tickData.Close = newTick.Close
		tickData.OI = newTick.OI
		tickData.Volume += newTick.Volume
		ohlcDataByUnitTime[roundedTickTime] = tickData
	}

	for tick := range ohlcDataByUnitTime {
		ohlcDataTicks = append(ohlcDataTicks, tick)
	}
	sort.Slice(ohlcDataTicks, func(i, j int) bool { return ohlcDataTicks[i] < ohlcDataTicks[j] })

	return ohlcDataByUnitTime, ohlcDataTicks
}

// func vixAndAtmStraddle(columns map[string]TickData, nearestStrike float64) (float64, float64) {
// 	vix := columns["INDIAVIX"].ValueToUseForCompute()
// 	fut := columns["NIFTY-FUT"].ValueToUseForCompute()

// 	atmStrike := int(math.Round(fut/nearestStrike) * nearestStrike)

// 	ceStrike := fmt.Sprintf("NIFTYWK%dCE", atmStrike)
// 	ce := columns[ceStrike].ValueToUseForCompute()

// 	peStrike := fmt.Sprintf("NIFTYWK%dPE", atmStrike)
// 	pe := columns[peStrike].ValueToUseForCompute()

// 	atmStraddle := ce + pe
// 	return vix, atmStraddle
// }

func buildTimeTicksFromColumns(columnarData map[int64]map[string]TickData) []int64 {
	sliceToSortAndReturn := []int64{}
	for tick := range columnarData {
		sliceToSortAndReturn = append(sliceToSortAndReturn, tick)
	}
	sort.Slice(sliceToSortAndReturn, func(i, j int) bool { return sliceToSortAndReturn[i] < sliceToSortAndReturn[j] })
	return sliceToSortAndReturn
}

func readData(records []map[string]string, columnarData map[int64]map[string]TickData) []string {
	tickerNames := make(map[string]struct{})
	for _, r := range records {
		rawTicker := r["Ticker"]
		if isNiftyTicker(rawTicker) {
			ticker := cleanTicker(rawTicker)
			tickerNames[ticker] = struct{}{}
			tickTime, err := parseTime(r["Date/Time"])
			handleError(err)
			tick := tickTime.Unix()
			columns, present := columnarData[tick]
			if !present {
				columns = make(map[string]TickData)
			}
			tickData := NewTickDataFromSlice([]string{
				r["Open"],
				r["High"],
				r["Low"],
				r["Close"],
				r["Open Interest"],
				r["Volume"],
			})

			columns[ticker] = tickData
			columnarData[tick] = columns
		}
	}

	allColumns := []string{}
	for ticker := range tickerNames {
		allColumns = append(allColumns, ticker)
	}
	sort.Strings(allColumns)
	return allColumns
}

func cleanTicker(ticker string) string {
	// 1. remove the WK prefix as part of NIFTY weekly expiry to keep it on par with monthly expiry symbols
	return strings.ReplaceAll(ticker, "NIFTYWK", "NIFTY")
}

func SliceContains(slice []int64, elem int64) bool {
	for _, item := range slice {
		if elem == item {
			return true
		}
	}
	return false
}

func isNiftyTicker(ticker string) bool {
	isNiftyTicker := (ticker == "NIFTY" || ticker == "NIFTY-FUT" || ticker == "INDIAVIX" || strings.HasPrefix(ticker, "NIFTYWK"))
	return isNiftyTicker
}

func parseTime(input string) (time.Time, error) {
	return dateparse.ParseIn(strings.ReplaceAll(input, "-", "/"), time.Local, dateparse.PreferMonthFirst(false))
}

func handleError(err error) {
	if err != nil {
		log.Fatalf("%v\n", err)
	}
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
