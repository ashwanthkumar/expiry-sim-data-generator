package tickers

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ashwanthkumar/expiry-sim-data-generator/utils"
)

func UnderlyingFromTicker(ticker string) string {
	if IsFuture(ticker) {
		return symbolFromFut(ticker)
	} else if IsOption(ticker) {
		symbol, err := symbolFromOptions(ticker)
		utils.HandleError(err)
		return symbol
	} else {
		// if spot
		return ticker
	}
}

type Ticker struct {
	RawTicker  string
	Underlying string
	IsFuture   bool
	IsOption   bool
	IsSpot     bool

	Strike         int    // 0 if spot and future, non-zero otherwise
	InstrumentType string // CE,PE,FUT
}

func ParseTicker(input string) (Ticker, error) {
	var err error = nil
	ticker := Ticker{
		RawTicker:      input,
		Underlying:     UnderlyingFromTicker(input),
		IsFuture:       false,
		IsOption:       false,
		IsSpot:         false,
		Strike:         0,
		InstrumentType: "",
	}

	ticker.IsFuture = IsFuture(input)
	ticker.IsOption = IsOption(input)
	ticker.IsSpot = !ticker.IsFuture && !ticker.IsOption

	if ticker.IsOption {
		parts, err := parseOptions(cleanTicker(input))
		if err != nil {
			return Ticker{}, err
		}
		strike, err := strconv.ParseInt(parts[2], 10, 32)
		if err != nil {
			return Ticker{}, err
		}
		ticker.Strike = int(strike)
		ticker.InstrumentType = parts[3]
	} else if ticker.IsFuture {
		ticker.InstrumentType = "FUT"
	}
	return ticker, err
}

var futureRegex = regexp.MustCompile(`^([A-Z]+)-FUT$`)

func symbolFromFut(input string) string {
	matches := futureRegex.FindStringSubmatch(input)
	// fmt.Printf("%v\n", matches)
	return matches[1]
}

var optionsRegex = regexp.MustCompile(`^([A-Z]+)([0-9]+)(CE|PE)$`)

func IsOption(ticker string) bool {
	return optionsRegex.MatchString(ticker)
}
func IsFuture(ticker string) bool {
	return futureRegex.MatchString(ticker)
}

func parseOptions(input string) ([]string, error) {
	if !IsOption(input) {
		return []string{}, fmt.Errorf("%s doesn't seem like a valid option symbol", input)
	}
	matches := optionsRegex.FindStringSubmatch(input)
	if len(matches) < 1 {
		fmt.Println("Input failed for: " + input)
		return []string{}, fmt.Errorf("%s doesn't seem like a valid option symbol", input)
	}
	// fmt.Printf("%v\n", matches)
	// [0] -> original string
	// [1] -> symbol
	// [2] -> strike
	// [3] -> CE/PE - Instrument Type
	return matches, nil
}

func cleanTicker(ticker string) string {
	// 1. remove the WK prefix as part of NIFTY weekly expiry to keep it on par with monthly expiry symbols
	return strings.ReplaceAll(ticker, "NIFTYWK", "NIFTY")
}

func symbolFromOptions(input string) (string, error) {
	if !IsOption(input) {
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
