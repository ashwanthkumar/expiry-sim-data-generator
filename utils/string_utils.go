package utils

import "strings"

func StringContainsIgnoreCase(slice []string, contains string) bool {
	for _, allowed := range slice {
		if strings.EqualFold(allowed, contains) {
			return true
		}
	}
	return false
}
