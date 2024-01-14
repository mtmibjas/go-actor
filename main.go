package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type DBActor struct {
	client *mongo.Client
}

func NewDBActor(connectionString string) (*DBActor, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, err
	}

	// Check the connection
	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return nil, err
	}

	return &DBActor{client: client}, nil
}

func (da *DBActor) ExecuteTransaction(operations ...func(context.Context, *mongo.SessionContext) error) error {
	session, err := da.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(context.Background())

	err = session.StartTransaction()
	if err != nil {
		return err
	}

	for _, op := range operations {
		err := op(context.WithValue(context.Background(), "session", session), session)
		if err != nil {
			session.AbortTransaction(context.Background())
			return err
		}
	}

	err = session.CommitTransaction(context.Background())
	if err != nil {
		return err
	}

	return nil
}

type WorkerActor struct {
	dbActor *DBActor
}

func NewWorkerActor(dbActor *DBActor) *WorkerActor {
	return &WorkerActor{dbActor: dbActor}
}

func (wa *WorkerActor) PerformTask(ctx context.Context, taskID int) error {
	// Access the session from the context
	_, ok := ctx.Value("session").(mongo.Session)
	if !ok {
		return fmt.Errorf("session not found in context")
	}

	collection := wa.dbActor.client.Database("your_database_name").Collection("your_collection_name")

	// Perform task operation (Update in this case)
	_, err := collection.UpdateOne(ctx, bson.M{"_id": taskID}, bson.M{"$set": bson.M{"status": "completed"}})
	if err != nil {
		return err
	}

	return nil
}

func main() {

	connectionString := ""

	dbActor, err := NewDBActor(connectionString)
	if err != nil {
		log.Fatal("Error connecting to MongoDB:", err)
	}

	// Simulate tasks being performed by workers within a single transaction
	err = dbActor.ExecuteTransaction(func(ctx context.Context, session *mongo.SessionContext) error {
		worker1 := NewWorkerActor(dbActor)
		if err := worker1.PerformTask(ctx, 1); err != nil {
			return err
		}

		worker2 := NewWorkerActor(dbActor)
		if err := worker2.PerformTask(ctx, 2); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		log.Fatal("Error executing transaction:", err)
	}

	// Allow time for database operations to complete
	time.Sleep(time.Second)
}
