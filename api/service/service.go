package service

import (
	"historical-data-series/api/service/apimodel"
)

type DataPointService interface {
	Persist(dp apimodel.DataPoint) error
	Retrieve(filters apimodel.Filters) (dps []apimodel.DataPoint, err error)
}
