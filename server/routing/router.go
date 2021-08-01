package routing

import (
	"net/http"

	"github.com/gorilla/mux"

	apihttp "historical-data-series/api/http"
	"historical-data-series/server/middleware"
)

const maxClients = 1

func NewRouter(env *apihttp.Env) *mux.Router {
	var router = mux.NewRouter()

	// define handlers for endpoints
	router.HandleFunc("/api/persist", middleware.LimitNumOfConcurrentClients(env.PersistHandler, maxClients)).Methods("POST")
	router.HandleFunc("/api/retrieve", middleware.LimitNumOfConcurrentClients(env.RetrieveHandler, maxClients)).Methods("GET")

	// define handler for unhandled paths
	var api = router.PathPrefix("/api").Subrouter()
	api.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	// define router entry point
	http.Handle("/", router)

	// apply any middleware
	router.Use(middleware.LoggingMiddleware)

	return router
}
