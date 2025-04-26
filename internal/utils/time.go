package utils

import "time"

func StartOfMonth(month time.Month, year int) time.Time {
	return time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
}

func AddMonth(t time.Time, month int) time.Time {
	return t.AddDate(0, month, 0)
}

func ToDateStr(t time.Time) string {
	return t.Format(time.DateOnly)
}
