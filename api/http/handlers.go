package http

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/schema"
	"github.com/pkg/errors"

	"historical-data-series/api/service/apimodel"
)

var decoder = schema.NewDecoder()

type Env struct {
	DataPoints interface {
		Persist(dp apimodel.DataPoint) error
		Retrieve(filters apimodel.Filters) ([]apimodel.DataPoint, error)
	}
}

func (e *Env) PersistHandler(w http.ResponseWriter, r *http.Request) {
	var dp apimodel.DataPoint
	if err := json.NewDecoder(r.Body).Decode(&dp); err != nil {
		e.respondWithError(w, http.StatusBadRequest, err)
		return
	}

	if err := e.DataPoints.Persist(dp); err != nil {
		e.respondWithError(w, http.StatusInternalServerError, err)
		return
	}
}

func (e *Env) RetrieveHandler(w http.ResponseWriter, r *http.Request) {
	var (
		params  = r.URL.Query()
		filters apimodel.Filters
	)

	if err := decoder.Decode(&filters, params); err != nil {
		e.respondWithError(w, http.StatusInternalServerError, errors.Wrap(err, "failed at decoder.Decode"))
		return
	}

	dps, err := e.DataPoints.Retrieve(filters)
	if err != nil {
		e.respondWithError(w, http.StatusInternalServerError, err)
		return
	}

	json.NewEncoder(w).Encode(dps)
}

func (e *Env) respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func (e *Env) respondWithError(w http.ResponseWriter, code int, err error) {
	e.respondWithJSON(w, code, map[string]string{"error": err.Error()})
}
