package db

import (
	"context"
	"log"
	"sync"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type DBInstance struct {
	sync.Mutex
}

// GetDB gets a context without timeout and a mongo db client
func (d *DBInstance) GetDB() (
	ctx context.Context,
	client *mongo.Client,
	disconnect func(),
	err error,
) {
	d.Lock()
	defer d.Unlock()

	log.Println("Setting up db...")

	// get a context without timeout
	ctx = context.Background()

	// connect to mongo db client
	clientOpts := options.Client().ApplyURI(DBURI).SetDirect(true)

	client, err = mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed at mongo.Connect")
	}

	disconnect = func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Got db client...")

	// check if MongoDB server has been found and connected to
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed at client.Ping")
	}

	log.Println("Connected to db client...")

	return
}
