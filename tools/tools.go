package tools

func Max64(first int64, args ...int64) int64 {
	for _, v := range args {
		if first < v {
			first = v
		}
	}
	return first
}

func StringPtr(s string) *string {
	return &s
}
