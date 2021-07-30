package db

import (
	"fmt"
)

const (
	DBName       = "historicalData"
	DBCollection = "dataPoints"
	dbHost       = "127.0.0.1"
	dbPort       = 27017
)

var DBURI = fmt.Sprintf("mongodb://%s:%d", dbHost, dbPort)
