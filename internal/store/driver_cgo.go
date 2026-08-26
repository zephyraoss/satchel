//go:build cgo

package store

import (
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

const driverName = "sqlite3"

const DriverDescription = "mattn/go-sqlite3 (cgo)"

func driverDSN(path string) string {
	return fmt.Sprintf("file:%s?_txlock=immediate&_journal_mode=WAL&_synchronous=OFF&_busy_timeout=10000&_foreign_keys=on", path)
}

const rawBusyTimeoutParam = "_busy_timeout=5000"
