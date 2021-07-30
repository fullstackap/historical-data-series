package db

import (
	"context"
	"log"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"historical-data-series/api/service"
	"historical-data-series/api/service/apimodel"
	serverdb "historical-data-series/server/db"
)

type dataPointService struct {
	db         *mongo.Database
	ctx        context.Context
	collection *mongo.Collection
}

func NewDataPointService(db *mongo.Database, ctx context.Context) service.DataPointService {
	return &dataPointService{
		db:         db,
		ctx:        ctx,
		collection: db.Collection(serverdb.DBCollection),
	}
}

func (s *dataPointService) Persist(dp apimodel.DataPoint) error {
	if strings.TrimSpace(dp.Entity) == "" {
		return errors.New("entity cannot be empty")
	}
	if strings.TrimSpace(dp.StaticPeriod) == "" {
		return errors.New("staticPeriod cannot be empty")
	}

	// make sure that for one pair of entity and time period,
	// the application accepts one value per timestamp
	var (
		timestamps    = bson.A{}
		samples       = bson.A{}
		timestampsMap = make(map[uint64]int)
	)
	for _, s := range dp.Samples {
		timestamps = append(timestamps, s.Timestamp)

		samples = append(samples, bson.D{
			{"timestamp", s.Timestamp},
			{"value", s.Value},
		})

		timestampsMap[s.Timestamp]++
		if timestampsMap[s.Timestamp] > 1 {
			return errors.Errorf("detected duplicate timestamp '%d' in input", s.Timestamp)
		}
	}

	var doc = bson.D{
		{"entity", dp.Entity},
		{"staticPeriod", dp.StaticPeriod},
	}

	doc = bson.D{
		{"$or",
			bson.A{
				bson.D{
					{"entity", dp.Entity},
					{"staticPeriod", dp.StaticPeriod},
				},
				bson.D{
					{"entity", dp.Entity},
					{"staticPeriod", dp.StaticPeriod},
					{"samples", bson.D{
						{"$elemMatch", bson.D{
							{"timestamp",
								bson.D{{"$in", timestamps}},
							},
						},
						},
					}},
				},
			},
		},
	}

	var dps []apimodel.DataPoint
	cursor, err := s.collection.Find(s.ctx, doc)
	if err != nil {
		return errors.Wrapf(err, "failed at collection.Find")
	}

	if err = cursor.All(s.ctx, &dps); err != nil {
		return errors.Wrapf(err, "failed at cursor.All")
	}

	defer cursor.Close(s.ctx)

	if len(dps) > 0 {
		if dps[0].Entity == dp.Entity &&
			dps[0].StaticPeriod == dp.StaticPeriod &&
			len(dp.Samples) == 0 {
			return errors.Errorf("entity '%s' for static period '%s' already exists", dp.Entity, dp.StaticPeriod)
		}

		// since we are allowing only one doc per entity and period for now
		// the first index is suppose to be our document
		// so here again make sure we have not saved already the same timestamp
		for _, s := range dps[0].Samples {
			timestampsMap[s.Timestamp]++
			if timestampsMap[s.Timestamp] > 1 {
				return errors.Errorf("timestamp '%d' already exists", s.Timestamp)
			}
		}
	}

	switch {
	case len(dps) > 0 && len(dp.Samples) > 0:
		log.Println("Updating record...")
		// update record
		var (
			docFilters = bson.D{
				{"entity", dp.Entity},
				{"staticPeriod", dp.StaticPeriod},
			}

			docAddToSetUpdateData = bson.M{
				"$addToSet": bson.M{
					"samples": bson.M{"$each": samples},
				},
			}

			docSetUpdateData = bson.D{
				{"$min", bson.D{{"sampleStartTime", dp.Samples.MinTimestamp()}}},
				{"$max", bson.D{{"sampleEndTime", dp.Samples.MaxTimestamp()}}},
			}
		)

		if err := s.updateDocumentM(&dp, docFilters, docAddToSetUpdateData); err != nil {
			return errors.Wrap(err, "failed at s.updateDocument for docAddToSetUpdateData")
		}

		if err := s.updateDocumentD(&dp, docFilters, docSetUpdateData); err != nil {
			return errors.Wrap(err, "failed at s.updateDocument for docSetUpdateData")
		}
	default:
		// insert record
		var bsonD bson.D
		if len(dp.Samples) > 0 {
			log.Println("Inserting record with samples...")

			bsonD = bson.D{
				{"entity", dp.Entity},
				{"staticPeriod", dp.StaticPeriod},
				{"sampleStartTime", dp.Samples.MinTimestamp()},
				{"sampleEndTime", dp.Samples.MaxTimestamp()},
				{"samples", samples},
			}
		} else {
			log.Println("Inserting record without samples...")

			bsonD = bson.D{
				{"entity", dp.Entity},
				{"staticPeriod", dp.StaticPeriod},
				{"sampleStartTime", uint64(0)},
				{"sampleEndTime", uint64(0)},
				{"samples", bson.A{}},
			}
		}

		if _, err := s.collection.InsertOne(s.ctx, bsonD); err != nil {
			return errors.Wrapf(err, "failed to insert data for entity '%s' and staticPeriod '%s'", dp.Entity, dp.StaticPeriod)
		}
	}

	return nil
}

func (s *dataPointService) Retrieve(filters apimodel.Filters) (dps []apimodel.DataPoint, err error) {
	var (
		pipeline        = make([]bson.M, 0)
		matchStageParts = bson.M{}
		groupStage      = bson.M{}
		limitStage      = bson.M{}
		collection      = s.db.Collection(serverdb.DBCollection)
	)

	if strings.TrimSpace(filters.Entity) != "" {
		matchStageParts["entity"] = filters.Entity
	}
	if strings.TrimSpace(filters.Period) != "" {
		matchStageParts["staticPeriod"] = filters.Period
	}

	if filters.Start > 0 {
		matchStageParts["sampleStartTime"] = filters.Start
	}

	if filters.End > 0 {
		matchStageParts["sampleEndTime"] = filters.End
	}

	matchStage := bson.M{"$match": matchStageParts}

	if filters.From > 0 {
		groupStage = bson.M{
			"$addFields": bson.M{"samples": bson.M{
				"$filter": bson.M{
					"input": "$samples",
					"as":    "samples",
					"cond": bson.M{
						"$gte": []interface{}{
							"$$samples.timestamp", filters.From,
						},
					},
				}},
			},
		}
	}

	if filters.From > 0 {
		limitStage = bson.M{"$limit": filters.From}
	}

	pipeline = append(pipeline, matchStage, groupStage, limitStage)

	cursor, err := collection.Aggregate(s.ctx, pipeline)
	if err != nil {
		return dps, errors.Wrapf(err, "failed at collection.Aggregate")
	}

	if err := cursor.All(s.ctx, &dps); err != nil {
		return dps, errors.Wrapf(err, "failed at cursor.All")
	}

	defer cursor.Close(s.ctx)

	return
}

func (s *dataPointService) updateDocumentM(dp *apimodel.DataPoint, docFilters bson.D, docUpdateData bson.M) (err error) {
	result, err := s.collection.UpdateOne(
		s.ctx,
		docFilters,
		docUpdateData,
	)
	return s.processUpdateResult(dp, result, err)
}

func (s *dataPointService) updateDocumentD(dp *apimodel.DataPoint, docFilters bson.D, docUpdateData bson.D) (err error) {
	result, err := s.collection.UpdateOne(
		s.ctx,
		docFilters,
		docUpdateData,
	)
	return s.processUpdateResult(dp, result, err)
}

func (s *dataPointService) processUpdateResult(dp *apimodel.DataPoint, result *mongo.UpdateResult, err error) error {
	if err != nil {
		return errors.Wrapf(err, "failed to update data for entity '%s' and staticPeriod '%s'", dp.Entity, dp.StaticPeriod)
	}

	if result.MatchedCount == 0 {
		return errors.Errorf("no match was found for entity '%s' and staticPeriod '%s'", dp.Entity, dp.StaticPeriod)
	}

	if result.ModifiedCount == 0 {
		return errors.Errorf("no modification was made on document with entity '%s' and staticPeriod '%s'", dp.Entity, dp.StaticPeriod)
	}

	return nil
}
