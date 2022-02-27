package main

// IntSlice attaches the methods of Interface to []int, sorting in increasing order.
type Int64Slice []int64

func (x Int64Slice) Len() int             { return len(x) }
func (x Int64Slice) Less(i, j int64) bool { return x[i] < x[j] }
func (x Int64Slice) Swap(i, j int64)      { x[i], x[j] = x[j], x[i] }
