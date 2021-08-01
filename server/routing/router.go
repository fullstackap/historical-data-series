package routing

import (
	"net/http"

	"github.com/gorilla/mux"

	apihttp "historical-data-series/api/http"
	"historical-data-series/server/middleware"
)

func NewRouter(env *apihttp.Env) *mux.Router {
	var router = mux.NewRouter()

	var (
		api         = router.PathPrefix("/api").Subrouter()
		subrouterV1 = api.PathPrefix("/v1").Subrouter()
	)

	// define handler for unhandled paths
	api.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	// v1
	AddRoutesV1(subrouterV1, env)

	// define router entry point
	http.Handle("/", router)

	// apply any middleware
	router.Use(middleware.LoggingMiddleware)

	return router
}
