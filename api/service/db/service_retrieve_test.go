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

	_, err = collection.InsertOne(testCtx, givenDPToBSON("entity", "period", 0, 0))
	require.NoError(t, err)

	var req = apimodel.Filters{}
	res, err := svc.Retrieve(req)
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))

	req = apimodel.Filters{Entity: "entity"}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))

	req = apimodel.Filters{Entity: "entity", Period: "period"}
	res, err = svc.Retrieve(req)
	require.NoError(t, err)
	assert.Equal(t, 1, len(res))
}
