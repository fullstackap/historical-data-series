package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	apihttp "historical-data-series/api/http"
	servicedb "historical-data-series/api/service/db"
	serverdb "historical-data-series/server/db"
	"historical-data-series/server/routing"
)

// TODO: add env variables handling to avoid using certain consts
// TODO: add centralized error handling
// TODO: add centralized unit test init db management
// TODO: add transactions to write test data in unit tests
// TODO: add api version control, enforcing both uri and headers requirements
// TODO: add mutual exclusive locks for concurrent handler usage
// TODO: add rate limit to allow up X simultaneous clients

const (
	serverHost             = "127.0.0.1"
	serverPort             = "8000"
	serverWriteTimeoutSecs = 15
	serverReadTimeoutSecs  = 15
)

var serverAddr = fmt.Sprintf("%s:%s", serverHost, serverPort)

func main() {
	// get db context, db context cancellation func, and db client
	var dbi = serverdb.DBInstance{}
	dbCtx, dbClient, dbClientDisconnect, err := dbi.GetDB()
	if err != nil {
		log.Fatal(err)
		return
	}

	// disconnect db client upon exit
	defer dbClientDisconnect()

	// setup env
	var (
		db  = dbClient.Database(serverdb.DBName)
		env = &apihttp.Env{
			DataPoints: servicedb.NewDataPointService(db, dbCtx),
		}
	)

	// TODO: remove after testing
	db.Collection("dataPoints").DeleteMany(dbCtx, bson.D{})

	// setup server
	server := &http.Server{
		Handler:      routing.NewRouter(env),
		Addr:         serverAddr,
		WriteTimeout: serverWriteTimeoutSecs * time.Second,
		ReadTimeout:  serverReadTimeoutSecs * time.Second,
	}

	// start server
	log.Fatal(server.ListenAndServe())
}
