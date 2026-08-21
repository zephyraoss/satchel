//go:build !cgo

package store

import (
	"fmt"

	_ "modernc.org/sqlite"
)

const driverName = "sqlite"

const DriverDescription = "modernc.org/sqlite (pure Go)"

func driverDSN(path string) string {
	return fmt.Sprintf("file:%s?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(10000)&_pragma=foreign_keys(ON)", path)
}

const rawBusyTimeoutParam = "_pragma=busy_timeout(5000)"
