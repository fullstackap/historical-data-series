package db_test

import (
	"historical-data-series/api/service/apimodel"
	servicedb "historical-data-series/api/service/db"
	serverdb "historical-data-series/server/db"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetrieve(t *testing.T) {
	var dbi = serverdb.TestDBInstance{}
	testCtx, testCtxCancel, testDBClient, testDBClientDisconnect, err := dbi.GetTestDB()
	require.NoError(t, err)

	defer testCtxCancel()
	defer testDBClientDisconnect()

	var (
		testDB = testDBClient.Database("test_db")
		svc    = servicedb.NewDataPointService(testDB, testCtx)
	)

	collection := testDB.Collection(serverdb.DBCollection)
	require.NoError(t, collection.Drop(testCtx))

	// storing a datapoint with no samples
	require.NoError(t, svc.Persist(givenDataPoint("entity", "period", 0, 0)))

	// search should result with the datapoint saved previously
	var req = apimodel.Filters{}
	res, err := svc.Retrieve(req)
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))

	// search should result with the datapoint saved previously again
	req = apimodel.Filters{Entity: "entity"}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))

	// search should result with the datapoint saved previously again
	req = apimodel.Filters{Entity: "entity", Period: "period"}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))

	// search should result with no result
	req = apimodel.Filters{Entity: "xxx", Period: "period"}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	assert.Equal(t, 0, len(res))

	// insert entity and period data point but with samples
	require.NoError(t, svc.Persist(givenDataPoint("entity", "period", 0, 0, testSamples1...)))

	// a plain search should result to one item again, with the corresponding sample as an update took place this time
	req = apimodel.Filters{}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	assert.Equal(t, 1, len(res[0].Samples))
	assert.Equal(t, int64(1), res[0].Samples[0].Timestamp)
	assert.Equal(t, int32(1), res[0].Samples[0].Value)

	// insert same entity and period data point with diff samples so we end up with update
	require.NoError(t, svc.Persist(givenDataPoint("entity", "period", 0, 0, testSamples2...)))

	// the object returned should be the same containing 3 items
	req = apimodel.Filters{}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	assert.Equal(t, 3, len(res[0].Samples))

	// the object returned should be the same containing all 3 items
	req = apimodel.Filters{From: 1}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	assert.Equal(t, 3, len(res[0].Samples))

	// the object returned should be the same containing 2 items
	req = apimodel.Filters{From: 2}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	assert.Equal(t, 2, len(res[0].Samples))

	// the object returned should be the same containing 1 items
	req = apimodel.Filters{From: 3}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	assert.Equal(t, 1, len(res[0].Samples))

	// the object returned should be the same containing 0 items
	req = apimodel.Filters{From: 4}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	assert.Equal(t, 0, len(res[0].Samples))
}
