package units

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

var byteUnits = []struct {
	suffix string
	factor int64
}{
	{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	{"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}, {"B", 1},
}

func ParseBytes(value string) (int64, error) {
	value = strings.TrimSpace(value)
	for _, unit := range byteUnits {
		if !strings.HasSuffix(value, unit.suffix) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(value, unit.suffix)), 10, 64)
		if err != nil || n <= 0 || n > math.MaxInt64/unit.factor {
			return 0, fmt.Errorf("invalid size %q", value)
		}
		return n * unit.factor, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	return n, nil
}
