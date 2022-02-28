package models

import (
	"errors"
	"strconv"

	"github.com/ashwanthkumar/optionskaro/utils"
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
		utils.HandleError(err)
	}
	open, err := strconv.ParseFloat(input[0], 64)
	utils.HandleError(err)
	high, err := strconv.ParseFloat(input[1], 64)
	utils.HandleError(err)
	low, err := strconv.ParseFloat(input[2], 64)
	utils.HandleError(err)
	close, err := strconv.ParseFloat(input[3], 64)
	utils.HandleError(err)
	oi, err := strconv.ParseInt(input[4], 10, 64)
	utils.HandleError(err)
	volume, err := strconv.ParseInt(input[5], 10, 64)
	utils.HandleError(err)

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
