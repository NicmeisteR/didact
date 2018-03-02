package core

import "errors"

var ErrForbidden = errors.New("Forbidden")
var ErrNotFound = errors.New("Not found")
var ErrRateLimit = errors.New("Hit the rate limit")
var ErrNoSuccess = errors.New("No success")
var ErrMetadataIncomplete = errors.New("Metadata incomplete")
var ErrEventsInvalid = errors.New("Events invalid")


func max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func sqlDuration(value string) string {
	if len(value) == 0 {
		return "PT0H"
	}
	return value
}
