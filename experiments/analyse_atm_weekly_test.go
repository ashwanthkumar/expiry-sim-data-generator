package main

import "testing"
import "github.com/stretchr/testify/assert"

func TestSliceContains(t *testing.T) {
	input := []int64{1, 2, 3, 4, 5}
	expected := true
	assert.Equal(t, expected, SliceContains(input, 5))
}
