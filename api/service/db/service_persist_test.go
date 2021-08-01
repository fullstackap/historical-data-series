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
		"empty model":           newPersistTestScenario("", true, "", "", 0, 0),
		"missing entity":        newPersistTestScenario("", true, "", "static_period", 0, 0),
		"missing static period": newPersistTestScenario("", true, "entity", "", 0, 0),
		"valid model with invalid timestamp in sample": newPersistTestScenario("", true, "entity", "static_period", 0, 0, invalidTestSamples...),
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

	// execute in sequence
	for i, c := range []struct {
		scenario persistTestScenario
	}{
		{scenario: newPersistTestScenario("valid model", false, "entity", "static_period", 0, 0)},
		{scenario: newPersistTestScenario("model already exists", true, "entity", "static_period", 0, 0)},
		{scenario: newPersistTestScenario("another valid model", false, "entity", "static_period_2", 0, 0)},
		{scenario: newPersistTestScenario("valid model with sample", false, "entity", "static_period", 0, 0, testSamples1...)},
		{scenario: newPersistTestScenario("invalid model with existing sample", true, "entity", "static_period", 0, 0, testSamples1...)},
		{scenario: newPersistTestScenario("valid model with samples", false, "entity", "static_period", 0, 0, testSamples2...)},
		{scenario: newPersistTestScenario("model with existing samples", true, "entity", "static_period", 0, 0, testSamples2...)},
		{scenario: newPersistTestScenario("model with duplicate input samples", true, "entity", "static_period", 0, 0, testDuplicteSamples...)},
	} {
		var (
			err    = svc.Persist(c.scenario.dp)
			errMsg = fmt.Sprintf("failed at scenario %d:%s", (i + 1), c.scenario.desc)
		)
		switch {
		case c.scenario.expectErr:
			assert.Error(t, err, errMsg)
		default:
			assert.NoError(t, err, errMsg)
		}
	}
}
