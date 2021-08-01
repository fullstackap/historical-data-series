package routing

import (
	apihttp "historical-data-series/api/http"
	"historical-data-series/server/middleware"

	"github.com/gorilla/mux"
)

func AddRoutesV1(r *mux.Router, env *apihttp.Env) {
	r.HandleFunc("/persist", middleware.LimitNumOfConcurrentClients(env.PersistHandler, maxClients)).Methods("POST")
	r.HandleFunc("/retrieve", middleware.LimitNumOfConcurrentClients(env.RetrieveHandler, maxClients)).Methods("GET")
}
