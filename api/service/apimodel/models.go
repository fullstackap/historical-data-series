package apimodel

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Sample struct {
	Timestamp uint64      `json:"timestamp" bson:"timestamp"`
	Value     interface{} `json:"value" bson:"value"`
}

type Samples []Sample

func (s Samples) SortByTimestamp(isAsc bool) {
	sort.SliceStable(s, func(i, j int) bool {
		if isAsc {
			return s[i].Timestamp < s[j].Timestamp
		}

		return s[i].Timestamp > s[j].Timestamp
	})
}

func (s Samples) MinTimestamp() (t uint64) {
	if len(s) > 0 {
		s.SortByTimestamp(true)
		return s[0].Timestamp
	}
	return
}

func (s Samples) MaxTimestamp() (t uint64) {
	if len(s) > 0 {
		s.SortByTimestamp(false)
		return s[0].Timestamp
	}
	return
}

type DataPoint struct {
	ID              primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	Entity          string             `json:"entity" bson:"entity"`
	StaticPeriod    string             `json:"staticPeriod" bson:"staticPeriod"`
	SampleStartTime uint64             `json:"sampleStartTime,omitempty" bson:"sampleStartTime,omitempty"`
	SampleEndTime   uint64             `json:"sampleEndTime,omitempty" bson:"sampleEndTime,omitempty"`
	Samples         Samples            `json:"samples,omitempty" bson:"samples,omitempty"`
}
