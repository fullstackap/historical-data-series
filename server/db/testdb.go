package db

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	testContextTimeoutSecs = 30
)

type TestDBInstance struct {
	sync.Mutex
}

// GetDB gets a context with timeout and a mongo db client
func (d *TestDBInstance) GetTestDB() (
	ctx context.Context,
	cancel context.CancelFunc,
	client *mongo.Client,
	disconnect func(),
	err error,
) {
	d.Lock()
	defer d.Unlock()

	log.Println("Setting up test db...")

	// get a context with timeout
	ctx, cancel = context.WithTimeout(context.Background(), testContextTimeoutSecs*time.Second)

	// connect to mongo db client
	clientOpts := options.Client().ApplyURI(DBURI).SetDirect(true)

	client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrapf(err, "failed at test mongo.Connect")
	}

	disconnect = func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Got test db client...")

	// check if MongoDB server has been found and connected to
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, nil, nil, nil, errors.Wrapf(err, "failed at test client.Ping")
	}

	log.Println("Connected to test db client...")

	return
}

func init() {

}
