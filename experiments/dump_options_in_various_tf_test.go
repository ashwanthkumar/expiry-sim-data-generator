package main

import "testing"
import "github.com/stretchr/testify/assert"

func TestSliceContains(t *testing.T) {
	input := []int64{1, 2, 3, 4, 5}
	expected := true
	assert.Equal(t, expected, SliceContains(input, 5))
}

func TestIsOption(t *testing.T) {
	assert.True(t, isNiftyOptionsTicker("NIFTYWK15500PE"))
	assert.True(t, isNiftyOptionsTicker("NIFTY-FUT"))
}

func TestIsNotOption(t *testing.T) {
	assert.False(t, isNiftyOptionsTicker("NIFTY"))
	assert.False(t, isNiftyOptionsTicker("BANKNIFTY-FUT"))
	assert.False(t, isNiftyOptionsTicker("BANKNIFTY-FUT"))
}
