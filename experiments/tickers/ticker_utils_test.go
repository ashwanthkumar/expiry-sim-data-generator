package tickers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTickerForSpot(t *testing.T) {
	ticker, err := ParseTicker("NIFTY")
	assert.Nil(t, err)
	assert.Equal(t, ticker.InstrumentType, "")
	assert.Equal(t, ticker.Strike, 0)
	assert.Equal(t, ticker.IsFuture, false)
	assert.Equal(t, ticker.IsOption, false)
	assert.Equal(t, ticker.IsSpot, true)
	assert.Equal(t, ticker.RawTicker, "NIFTY")
	assert.Equal(t, ticker.Underlying, "NIFTY")
}
func TestParseTickerForFuture(t *testing.T) {
	ticker, err := ParseTicker("NIFTY-FUT")
	assert.Nil(t, err)
	assert.Equal(t, ticker.InstrumentType, "FUT")
	assert.Equal(t, ticker.Strike, 0)
	assert.Equal(t, ticker.IsFuture, true)
	assert.Equal(t, ticker.IsOption, false)
	assert.Equal(t, ticker.IsSpot, false)
	assert.Equal(t, ticker.RawTicker, "NIFTY-FUT")
	assert.Equal(t, ticker.Underlying, "NIFTY")
}

func TestParseTickerForWeeklyOptions(t *testing.T) {
	ticker, err := ParseTicker("NIFTYWK13500PE")
	assert.Nil(t, err)
	assert.Equal(t, ticker.InstrumentType, "PE")
	assert.Equal(t, ticker.Strike, 13500)
	assert.Equal(t, ticker.IsFuture, false)
	assert.Equal(t, ticker.IsOption, true)
	assert.Equal(t, ticker.IsSpot, false)
	assert.Equal(t, ticker.RawTicker, "NIFTYWK13500PE")
	assert.Equal(t, ticker.Underlying, "NIFTY")
}
