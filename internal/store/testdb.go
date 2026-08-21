package store

import "database/sql"

func OpenRaw(path string) (*sql.DB, error) {
	return sql.Open(driverName, "file:"+path+"?"+rawBusyTimeoutParam)
}
