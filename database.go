package main

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const MogSessionKey = "mgo_session"

func (o *Objectql) initMongodb(ctx context.Context, uri string) (err error) {
	o.mongoClientOpts = options.Client().ApplyURI(uri)
	o.mongoClient, err = mongo.Connect(ctx, o.mongoClientOpts)
	if err != nil {
		return
	}
	o.mongoCollectionOptions = options.Collection().SetWriteConcern(writeconcern.Majority())
	return
}

func (o *Objectql) getCollection(api string) *mongo.Collection {
	return o.mongoClient.Database("test").Collection(api)
}

func (o *Objectql) WithTransaction(ctx context.Context, fn func(ctx context.Context) (interface{}, error)) (interface{}, error) {
	if mongo.SessionFromContext(ctx) != nil {
		return fn(ctx)
	} else {
		session, err := o.mongoClient.StartSession()
		if err != nil {
			return nil, err
		}
		defer session.EndSession(ctx)
		return session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
			return fn(ctx)
		})
	}
}

func (o *Objectql) mongoFindAll(ctx context.Context, table string, filter bson.M, selects string) ([]bson.M, error) {
	findOptions := options.Find()
	if len(selects) > 0 {
		findOptions.SetProjection(StringArrayToProjection(strings.Split(selects, ",")))
	}
	cursor, err := o.getCollection(table).Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}
	var result []bson.M
	err = cursor.All(ctx, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Objectql) mongoFindOne(ctx context.Context, table string, filter bson.M, selects string) (bson.M, error) {
	findOneOptions := options.FindOne()
	if len(selects) > 0 {
		findOneOptions.SetProjection(StringArrayToProjection(strings.Split(selects, ",")))
	}
	var result bson.M
	err := o.getCollection(table).FindOne(ctx, filter, findOneOptions).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o *Objectql) mongoCount(ctx context.Context, table string, filter bson.M) (int64, error) {
	count, err := o.getCollection(table).CountDocuments(ctx, filter)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (o *Objectql) mongoInsert(ctx context.Context, table string, doc bson.M) (string, error) {
	insertResult, err := o.getCollection(table).InsertOne(ctx, doc)
	if err != nil {
		return "", err
	}
	return insertResult.InsertedID.(primitive.ObjectID).Hex(), nil
}

func (o *Objectql) mongoUpdateById(ctx context.Context, table string, id string, doc bson.M) error {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	_, err = o.getCollection(table).UpdateByID(ctx, objectId, bson.M{
		"$set": doc,
	})
	return err
}

func (o *Objectql) mongoDeleteById(ctx context.Context, table string, id string) error {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	_, err = o.getCollection(table).DeleteOne(ctx, bson.M{"_id": objectId})
	return err
}

func ObjectIdFromHex(id string) primitive.ObjectID {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		panic(err)
	}
	return objectId
}

func StringArrayToProjection(arr []string) bson.M {
	result := bson.M{}
	for _, v := range arr {
		result[v] = 1
	}
	return result
}
