package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/ashwanthkumar/optionskaro/experiments/tickers"
	"github.com/ashwanthkumar/optionskaro/experiments/utils"
	"github.com/ashwanthkumar/optionskaro/models"
)

func main() {
	baseOutput := "data"
	baseLocations := []string{"monthly/", "weekly/"}

	// NIFTY -> {2022-02-26 -> "MONTHLY/WEEKLY", ....} time is stored using time.Unix()
	underlyingToExpiry := make(map[string]map[int64]string)
	for _, baseLocation := range baseLocations {
		files, err := ioutil.ReadDir(baseLocation)
		utils.HandleError(err)
		for _, f := range files[:1] {
			input := path.Join(baseLocation, f.Name())
			f, err := os.Open(input)
			utils.HandleError(err)
			defer f.Close()
			log.Printf("Processing file: %s", input)
			records := CSVToMap(f)

			// this is a nested map which has the following structure
			// outer map's key is of type "time.Time" that denotes
			// the time of the tick.
			// The inner map's key denotes the ticker name (NIFTY, INDIAVIX, etc.)
			// we would ideally want to query all the columns for a given time tick,
			// hence this format of choice.
			columnarData := make(map[int64]map[string]models.TickData)

			columnNames := readData2(records, columnarData, func(ticker string) bool {
				return isNiftyOptionsTicker2(ticker)
			})

			allTimeTicksForTheCurrentExpiry := buildTimeTicksFromColumns(columnarData)
			tickPerDay := GroupByInt64(allTimeTicksForTheCurrentExpiry, func(tick int64) int64 {
				return truncateToDay(time.Unix(tick, 0)).Unix()
			})

			// NB: Making an assumption in here. We might need to check at some point if we had a monthly expiry which lasted less than 5 days
			// weekly expires have atmost 5 days, if we have more then it's mostly likely monthly expiry
			isMonthlyExpiry := len(tickPerDay) > 5

			// given each file contains all the data till expiry the last time in a sorted slice will be the expiry date-time
			expiryDate := truncateToDay(time.Unix(allTimeTicksForTheCurrentExpiry[len(allTimeTicksForTheCurrentExpiry)-1], 0))
			expiryDateFormat := utils.UnixTimeToDateFormat(expiryDate)

			for _, tickerInput := range columnNames {
				ticker, err := tickers.ParseTicker(tickerInput)
				utils.HandleError(err)

				updateUnderlyingToExpiryMapping(underlyingToExpiry, ticker, expiryDate, isMonthlyExpiry)

				// these are the roll-ups we do on the data and persist them for easy fetch from the UI
				timeUnits := []time.Duration{1 * time.Minute, 3 * time.Minute, 5 * time.Minute}
				for _, unitTime := range timeUnits {
					ohlcDataByUnitTime, ohlcDataByUnitTimeKeys := ohlcDataGroupedByFor(allTimeTicksForTheCurrentExpiry, columnarData, ticker.RawTicker, unitTime)

					ticks := [][]float64{}
					for _, tick := range ohlcDataByUnitTimeKeys {
						tickData := ohlcDataByUnitTime[tick]
						ticks = append(ticks, tickData.ToSlice(tick))
					}

					// write the output to file
					tfMinutes := int(unitTime.Minutes())
					fileName := fmt.Sprintf("%s/%s/%s_%dmin.json", baseOutput, expiryDateFormat, ticker.RawTicker, tfMinutes)
					output := make(map[string]interface{})
					output["ticker"] = ticker.RawTicker
					output["is_option"] = ticker.IsOption
					output["is_future"] = ticker.IsFuture
					output["is_spot"] = ticker.IsSpot
					output["instrument_type"] = ticker.InstrumentType
					output["strike"] = ticker.Strike
					output["underlying"] = ticker.Underlying
					output["expiry_date"] = expiryDateFormat
					// output["date"] = date
					output["tf_minutes"] = tfMinutes
					output["data"] = ticks

					writeDataAsJsonToFile(fileName, output)
					log.Printf("Wrote %s against %dmin timeframe\n", fileName, tfMinutes)
				} // end for per timeUnit
			} // end for per column (ticker)

			// we should also find a way to dump all the symbols as part of a given expiry so we can choose to show it in any format we want in the UI
			fileName := fmt.Sprintf("%s/%s/symbols.json", baseOutput, expiryDateFormat)
			output := make(map[string]interface{})
			output["expiry_date"] = expiryDateFormat
			output["is_monthly_expiry"] = isMonthlyExpiry
			output["is_weekly_expiry"] = !isMonthlyExpiry
			output["symbols"] = columnNames
			writeDataAsJsonToFile(fileName, output)
		} // end for per file that we read

	} // end for per directory that we read

	// ignorelist of underlying that we don't need
	ignoreList := []string{"INDIAVIX"}
	for key, _ := range underlyingToExpiry {
		if utils.StringContainsIgnoreCase(ignoreList, key) {
			delete(underlyingToExpiry, key)
		}
	}

	// should write the underlying to expiry maping at the root of baseOutput so we can query for it directly
	fileName := fmt.Sprintf("%s/expiries.json", baseOutput)
	writeDataAsJsonToFile(fileName, underlyingToExpiry)
}

func writeDataAsJsonToFile(fileName string, outputToWriteAsJson interface{}) {
	file, err := json.Marshal(outputToWriteAsJson)
	utils.HandleError(err)

	err = os.MkdirAll(filepath.Dir(fileName), os.ModePerm)
	utils.HandleError(err)
	err = ioutil.WriteFile(fileName, file, 0644)
	utils.HandleError(err)
}

func updateUnderlyingToExpiryMapping(underlyingToExpiry map[string]map[int64]string, ticker tickers.Ticker, expiryDate time.Time, isMonthly bool) {
	expirySoFar, present := underlyingToExpiry[ticker.Underlying]
	if !present {
		expirySoFar = make(map[int64]string)
	}
	value := "WEEKLY"
	if isMonthly {
		value = "MONTHLY"
	}
	expirySoFar[expiryDate.Unix()] = value
	underlyingToExpiry[ticker.Underlying] = expirySoFar
}

func isNiftyOptionsTicker2(rawTicker string) bool {
	ticker, err := tickers.ParseTicker(rawTicker)
	utils.HandleError(err)

	if ticker.IsSpot {
		listOfAllowedRawTickers := []string{"INDIAVIX", "NIFTY", "NIFTY-FUT", "BANKNIFTY", "BANKNIFTY-FUT"}
		return utils.StringContainsIgnoreCase(listOfAllowedRawTickers, ticker.RawTicker)
	} else {
		listOfAllowedRawTickers := []string{"NIFTY"}
		return utils.StringContainsIgnoreCase(listOfAllowedRawTickers, ticker.Underlying)
	}
}

func readData2(records []map[string]string, columnarData map[int64]map[string]models.TickData, recordSelector func(string) bool) []string {
	tickerNames := make(map[string]struct{})
	for _, r := range records {
		rawTicker := r["Ticker"]
		if recordSelector(rawTicker) {
			tickerNames[rawTicker] = struct{}{}
			tickTime, err := parseTime(r["Date/Time"])
			utils.HandleError(err)
			tick := tickTime.Unix()
			columns, present := columnarData[tick]
			if !present {
				columns = make(map[string]models.TickData)
			}
			tickData := models.NewTickDataFromSlice([]string{
				r["Open"],
				r["High"],
				r["Low"],
				r["Close"],
				r["Open Interest"],
				r["Volume"],
			})

			columns[rawTicker] = tickData
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

func buildTimeTicksFromColumns(columnarData map[int64]map[string]models.TickData) []int64 {
	sliceToSortAndReturn := []int64{}
	for tick := range columnarData {
		sliceToSortAndReturn = append(sliceToSortAndReturn, tick)
	}
	sort.Slice(sliceToSortAndReturn, func(i, j int) bool { return sliceToSortAndReturn[i] < sliceToSortAndReturn[j] })
	return sliceToSortAndReturn
}

func parseTime(input string) (time.Time, error) {
	return dateparse.ParseIn(strings.ReplaceAll(input, "-", "/"), time.Local, dateparse.PreferMonthFirst(false))
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

func truncateToDay(t time.Time) time.Time {
	nt, err := time.Parse("2006-01-02", t.Format("2006-01-02"))
	utils.HandleError(err)
	return nt
}

// this should help us build ohlc data by any unit of time (5 minutes, 15 minutes, 1 hour, etc.)
func ohlcDataGroupedByFor(allTimeTicksForTheCurrentExpiry []int64, columnarData map[int64]map[string]models.TickData, symbolToPick string, groupedTimeUnit time.Duration) (map[int64]models.TickData, []int64) {
	ohlcDataByUnitTime := make(map[int64]models.TickData)
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
			tickData = models.TickData{
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

func GroupByInt64(slice []int64, groupFn func(int64) int64) map[int64][]int64 {
	groups := make(map[int64][]int64)
	for _, elem := range slice {
		elementKey := groupFn(elem)
		existing := groups[elementKey]
		existing = append(existing, elem)
		groups[elementKey] = existing
	}
	return groups
}
