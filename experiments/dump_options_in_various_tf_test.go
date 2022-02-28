package main

import "testing"
import "github.com/stretchr/testify/assert"

func TestSliceContains(t *testing.T) {
	input := []int64{1, 2, 3, 4, 5}
	expected := true
	assert.Equal(t, expected, SliceContains(input, 5))
}

func TestIsOptionOnWeekly(t *testing.T) {
	assert.True(t, isNiftyOptionsTicker("NIFTYWK15500PE", false))
	assert.False(t, isNiftyOptionsTicker("NIFTY-FUT", false))
}

func TestIsOptionOnMonthly(t *testing.T) {
	assert.True(t, isNiftyOptionsTicker("NIFTYWK15500PE", false))
	assert.True(t, isNiftyOptionsTicker("NIFTY-FUT", true))
}

func TestIsNotOptionOnWeekly(t *testing.T) {
	assert.False(t, isNiftyOptionsTicker("NIFTY", false))
	assert.False(t, isNiftyOptionsTicker("BANKNIFTY-FUT", false))
	assert.False(t, isNiftyOptionsTicker("BANKNIFTY-FUT", false))
}

func TestIsNotOptionOnMonthly(t *testing.T) {
	assert.False(t, isNiftyOptionsTicker("NIFTY", false))
	assert.False(t, isNiftyOptionsTicker("BANKNIFTY-FUT", false))
	assert.False(t, isNiftyOptionsTicker("BANKNIFTY-FUT", false))
}
