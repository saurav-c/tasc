package common

import (
	"strconv"
	"strings"
)

// Compare two key versions which are encoded as
// commitTs-NodeId
func CompareKeyVersion(v1 string, v2 string) int {
	split1 := strings.Split(v1, VersionDelimeter)
	split2 := strings.Split(v2, VersionDelimeter)

	ts1 := Int64FromString(split1[0])
	ts2 := Int64FromString(split2[0])

	if ts1 > ts2 {
		return 1
	} else if ts1 < ts2 {
		return -1
	} else {
		nId1 := split1[1]
		nId2 := split2[1]
		if nId1 > nId2 {
			return 1
		} else {
			return -1
		}
	}
}

func Int64ToString(n int64) string {
	return strconv.FormatInt(n, 10)
}

func Int64FromString(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}