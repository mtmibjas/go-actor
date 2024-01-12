package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
)

type mongoDB struct {
	DB     *mongo.Database
	dbOpts *options.DatabaseOptions
	client *mongo.Client
}

type BsonD bson.D
type BsonM bson.M
type BsonE bson.E

var Mongo mongoDB

func MongoInit() {

	uri := "mongodb://innovent-user:hvBf0iaN5FX2lBWt@157.175.196.81:27017/innoventstaging?retryWrites=true&w=majority&authSource=admin&directConnection=true"

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var err error
	Mongo.client, err = mongo.Connect(ctx, options.Client().ApplyURI(uri).SetMonitor(otelmongo.NewMonitor(otelmongo.WithCommandAttributeDisabled(false))))
	if err != nil {
		return
	}

	// Use the client to ping the server
	err = Mongo.client.Ping(ctx, nil)
	if err != nil {
		return
	}
	Mongo.DB = Mongo.client.Database("innoventstaging", Mongo.dbOpts)

}

func ObjectId(id string) primitive.ObjectID {
	objectId, _ := primitive.ObjectIDFromHex(id)
	return objectId
}

func CloseMongoDB(ctx context.Context) {
	if err := Mongo.client.Disconnect(ctx); err != nil {
		return
	}

}

func UpdateOption() *options.UpdateOptions {
	return options.Update()
}
