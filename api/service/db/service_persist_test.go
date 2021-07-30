package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	servicedb "historical-data-series/api/service/db"
	serverdb "historical-data-series/server/db"
)

func TestPersistInputValidations(t *testing.T) {
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

	for name, s := range map[string]persistTestScenario{
		"empty model":           newPersistTestScenario(true, "", "", 0, 0),
		"missing entity":        newPersistTestScenario(true, "", "static_period", 0, 0),
		"missing static period": newPersistTestScenario(true, "entity", "", 0, 0),
	} {
		t.Run(name, func(t *testing.T) {
			t.Helper()

			var (
				err    = svc.Persist(s.dp)
				errMsg = fmt.Sprintf("failed at scenario '%s'", name)
			)
			switch {
			case s.expectErr:
				assert.Error(t, err, errMsg)
			default:
				assert.NoError(t, err, errMsg)
			}
		})
	}
}

func TestPersistDBValidations(t *testing.T) {
	t.Skip("WIP")

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

	for name, s := range map[string]persistTestScenario{
		"valid model":                                newPersistTestScenario(false, "entity", "static_period", 0, 0),
		"model already exists":                       newPersistTestScenario(true, "entity", "static_period", 0, 0),
		"another valid model":                        newPersistTestScenario(false, "entity", "static_period_2", 0, 0),
		"valid model with sample":                    newPersistTestScenario(false, "entity", "static_period", 0, 0, testSamples1...),
		"invalid model with existing sample":         newPersistTestScenario(true, "entity", "static_period", 0, 0, testSamples1...),
		"valid model with samples":                   newPersistTestScenario(false, "entity", "static_period", 0, 0, testSamples2...),
		"invalid model with existing samples":        newPersistTestScenario(true, "entity", "static_period", 0, 0, testSamples2...),
		"invalid model with duplicate input samples": newPersistTestScenario(true, "entity", "static_period", 0, 0, testSamples3...),
	} {
		var (
			err    = svc.Persist(s.dp)
			errMsg = fmt.Sprintf("failed at scenario '%s'", name)
		)
		switch {
		case s.expectErr:
			assert.Error(t, err, errMsg)
		default:
			assert.NoError(t, err, errMsg)
		}
	}
}
