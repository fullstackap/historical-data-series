package db_test

import (
	"historical-data-series/api/service/apimodel"

	"go.mongodb.org/mongo-driver/bson"
)

var (
	testSamples1 = apimodel.Samples{
		{
			Timestamp: 1,
			Value:     1,
		},
	}

	testSamples2 = apimodel.Samples{
		{
			Timestamp: 2,
			Value:     2,
		},
		{
			Timestamp: 3,
			Value:     3,
		},
	}

	testSamples3 = apimodel.Samples{
		{
			Timestamp: 4,
			Value:     4,
		},
		{
			Timestamp: 4,
			Value:     4,
		},
	}
)

type persistTestScenario struct {
	dp        apimodel.DataPoint
	expectErr bool
}

func newPersistTestScenario(
	expectErr bool,
	entity, period string,
	start, end uint64,
	samples ...apimodel.Sample) persistTestScenario {
	var s = persistTestScenario{
		dp: apimodel.DataPoint{
			Entity:          entity,
			StaticPeriod:    period,
			SampleStartTime: start,
			SampleEndTime:   end,
		},
		expectErr: expectErr,
	}
	if len(samples) > 0 {
		s.dp.Samples = samples
	}
	return s
}

func givenDataPoint(
	entity, period string,
	start, end uint64,
	samples ...apimodel.Sample,
) apimodel.DataPoint {
	dp := apimodel.DataPoint{
		Entity:          entity,
		StaticPeriod:    period,
		SampleStartTime: start,
		SampleEndTime:   end,
	}

	if len(samples) > 0 {
		dp.Samples = append(dp.Samples, samples...)
	}

	return dp
}

func givenBSON(dp apimodel.DataPoint) bson.D {
	var samples = bson.A{}
	for _, s := range dp.Samples {
		samples = append(samples, bson.D{
			{"timestamp", s.Timestamp},
			{"value", s.Value},
		})
	}
	return bson.D{
		{"entity", dp.Entity},
		{"staticPeriod", dp.StaticPeriod},
		{"sampleStartTime", dp.SampleStartTime},
		{"sampleEndTime", dp.SampleEndTime},
		{"samples", samples},
	}
}

func givenDPToBSON(
	entity, period string,
	start, end uint64,
	samples ...apimodel.Sample,
) bson.D {
	return givenBSON(givenDataPoint(entity, period, start, end, samples...))
}
