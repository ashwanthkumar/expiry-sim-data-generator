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
	"path"
	"path/filepath"
	"regexp"
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
	baseOutput := "data/"
	baseOutputForOptions := path.Join(baseOutput, "options")
	baseOutputForFut := path.Join(baseOutput, "futures")
	baseOutputForSpot := path.Join(baseOutput, "spot")
	baseLocations := []string{"monthly/", "weekly/"}
	for _, baseLocation := range baseLocations {

		files, err := ioutil.ReadDir(baseLocation)
		handleError(err)

		for _, f := range files {
			input := path.Join(baseLocation, f.Name())
			f, err := os.Open(input)
			handleError(err)
			defer f.Close()
			log.Printf("Processing file: %s", input)
			records := CSVToMap(f)

			// this is a nested map which has the following structure
			// outer map's key is of type "time.Time" that denotes
			// the time of the tick.
			// The inner map's key denotes the ticker name (NIFTY, INDIAVIX, etc.)
			// we would ideally want to query all the columns for a given time tick,
			// hence this format of choice.
			columnarData := make(map[int64]map[string]TickData)

			columnNames := readData(records, columnarData, isNiftyOptionsTicker)
			// log.Printf("built in-memory collection of nifty records from: %v\n", columnNames)

			allTimeTicksForTheCurrentExpiry := buildTimeTicksFromColumns(columnarData)
			// given each file contains all the data to expiry the last time in a sorted slice will be the expiry time that we need
			expiryDate := truncateToDay(time.Unix(allTimeTicksForTheCurrentExpiry[len(allTimeTicksForTheCurrentExpiry)-1], 0))
			expiryDateFormat := expiryDate.Format("2006-01-02")

			for _, columnToPick := range columnNames {
				underlying := underlyingFromTicker(columnToPick)
				timeUnits := []time.Duration{1 * time.Minute, 3 * time.Minute, 5 * time.Minute}
				for _, unitTime := range timeUnits {
					ohlcDataByUnitTime, ohlcDataTicks := ohlcDataGroupedByFor(allTimeTicksForTheCurrentExpiry, columnarData, columnToPick, unitTime)
					ticks := [][]float64{}
					for _, tick := range ohlcDataTicks {
						tickData := ohlcDataByUnitTime[tick]
						ticks = append(ticks, tickData.ToSlice(tick))
					}

					baseOutputDir := buildBaseOutputDir(columnToPick, baseOutputForFut, baseOutputForOptions, baseOutputForSpot)
					writeTickDataToFs(baseOutputDir, columnToPick, expiryDateFormat, unitTime, ticks, underlying)
				}
			}
			underlyingToColumns := GroupBy(columnNames, underlyingFromTicker)

			for underlying, columnsToWrite := range underlyingToColumns {
				baseOutputToColumns := GroupBy(columnsToWrite, func(column string) string {
					return buildBaseOutputDir(column, baseOutputForFut, baseOutputForOptions, baseOutputForSpot)
				})

				for baseOutputDir, columns := range baseOutputToColumns {
					writeTickersFromThisExpiry(baseOutputDir, underlying, columns, expiryDateFormat)
				}
			}
		}
	}

	// index the list of expiries
	locationsToIndexexpiries := []string{baseOutputForFut, baseOutputForOptions}
	for _, base := range locationsToIndexexpiries {
		filesInBase, err := ioutil.ReadDir(base)
		handleError(err)
		for _, underlying := range filesInBase {
			dirWithListOfexpiries := path.Join(base, underlying.Name())
			expiries, err := ioutil.ReadDir(dirWithListOfexpiries)
			handleError(err)
			expiriesToWrite := []string{}
			for _, e := range expiries {
				expiriesToWrite = append(expiriesToWrite, e.Name())
			}

			writeExpiriesForUnderlying(expiriesToWrite, dirWithListOfexpiries)
		}
	}

	// now that we've dumped all the files across various timeframes to disk, let's create easy to process index content
	// inside each expiry folder, we might want to dump all the strikes / symbols in a static file so we can query it from
	// the UI. Similarly, we also need a expiries.json which has all the required expiry dates in a file.

	// const minStrikeDistance float64 = 50
	// for _, tick := range allTimeTicksForTheCurrentExpiry {
	// 	columns := columnarData[tick]
	// 	tickTime := time.Unix(tick, 0)
	// 	vix, atmStraddle := vixAndAtmStraddle(columns, minStrikeDistance)
	// 	fmt.Printf("%s - %f - %f\n", tickTime, vix, atmStraddle)
	// }
}

func GroupBy(slice []string, groupFn func(string) string) map[string][]string {
	groups := make(map[string][]string)
	for _, elem := range slice {
		elementKey := groupFn(elem)
		existing, present := groups[elementKey]
		if !present {
			existing = []string{}
		}
		existing = append(existing, elem)
		groups[elementKey] = existing
	}
	return groups
}

func buildBaseOutputDir(ticker, basePathForFut, basePathForOptions, basePathForSpot string) string {
	if isFuture(ticker) {
		return basePathForFut
	} else if isOption(ticker) {
		return basePathForOptions
	} else {
		// if spot
		return basePathForSpot
	}
}

func writeExpiriesForUnderlying(expiriesToWrite []string, dirWithListOfexpiries string) {
	file, err := json.Marshal(expiriesToWrite)
	handleError(err)
	fileName := fmt.Sprintf("%s/expiries.json", dirWithListOfexpiries)
	err = os.MkdirAll(filepath.Dir(fileName), os.ModePerm)
	handleError(err)
	err = ioutil.WriteFile(fileName, file, 0644)
	handleError(err)
	log.Printf("Wrote %s that has all the expiries\n", fileName)
}

func writeTickersFromThisExpiry(baseOutput, underlying string, columnNames []string, expiryDateFormat string) {
	file, err := json.Marshal(columnNames)
	handleError(err)
	fileName := fmt.Sprintf("%s/%s/%s/symbols.json", baseOutput, underlying, expiryDateFormat)
	err = os.MkdirAll(filepath.Dir(fileName), os.ModePerm)
	handleError(err)
	err = ioutil.WriteFile(fileName, file, 0644)
	handleError(err)
	log.Printf("Wrote %s that has all the strikes\n", fileName)
}

func writeTickDataToFs(baseOutput, columnToPick, expiryDateFormat string, unitTime time.Duration, ticks [][]float64, underlying string) {
	output := make(map[string]interface{})
	output["ticker"] = columnToPick
	output["expiryDate"] = expiryDateFormat
	tfMinutes := unitTime.Minutes()
	output["tf_minutes"] = tfMinutes
	output["data"] = ticks

	file, err := json.Marshal(output)
	handleError(err)

	fileName := fmt.Sprintf("%s/%s/%s/%s/%dmin.json", baseOutput, underlying, expiryDateFormat, columnToPick, int(tfMinutes))
	err = os.MkdirAll(filepath.Dir(fileName), os.ModePerm)
	handleError(err)
	err = ioutil.WriteFile(fileName, file, 0644)
	handleError(err)
	log.Printf("Wrote %s against %dmin timeframe\n", fileName, int(tfMinutes))
}

func underlyingFromTicker(ticker string) string {
	if isFuture(ticker) {
		return symbolFromFut(ticker)
	} else if isOption(ticker) {
		symbol, err := symbolFromOptions(ticker)
		handleError(err)
		return symbol
	} else {
		// if spot
		return ticker
	}
}

var futureRegex = regexp.MustCompile(`^([A-Z]+)-FUT$`)

func symbolFromFut(input string) string {
	matches := futureRegex.FindStringSubmatch(input)
	// fmt.Printf("%v\n", matches)
	return matches[1]
}

var optionsRegex = regexp.MustCompile(`^([A-Z]+)([0-9]+)(CE|PE)$`)

func isOption(ticker string) bool {
	return optionsRegex.MatchString(ticker)
}
func isFuture(ticker string) bool {
	return futureRegex.MatchString(ticker)
}

func symbolFromOptions(input string) (string, error) {
	if !isOption(input) {
		return "", fmt.Errorf("%s doesn't seem like a valid option symbol", input)
	}
	matches := optionsRegex.FindStringSubmatch(input)
	if len(matches) < 1 {
		fmt.Println("Input failed for: " + input)
		return "", fmt.Errorf("%s doesn't seem like a valid option symbol", input)
	}
	// fmt.Printf("%v\n", matches)
	// [0] -> original string
	// [1] -> symbol
	// [2] -> strike
	// [3] -> CE/PE - Instrument Type
	return cleanTicker(matches[1]), nil
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

func readData(records []map[string]string, columnarData map[int64]map[string]TickData, recordSelector func(string) bool) []string {
	tickerNames := make(map[string]struct{})
	for _, r := range records {
		rawTicker := r["Ticker"]
		if recordSelector(rawTicker) {
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

func isNiftyOptionsTicker(ticker string) bool {
	underlying := underlyingFromTicker(ticker)
	isNiftyOptionsTicker := (isOption(ticker) || isFuture(ticker)) && strings.EqualFold(underlying, "NIFTY")
	return isNiftyOptionsTicker
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
