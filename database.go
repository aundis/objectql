package main

import (
	"context"

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
	return o.mongoClient.Database("tset").Collection(api)
	// if ctx != nil && ctx.Value(MogSessionKey) != nil {
	// 	mgoSession := ctx.Value(MogSessionKey).(*mgo.Session)
	// 	return session.DB("test").C(api).With(mgoSession)
	// } else {
	// 	return session.DB("test").C(api)
	// }
}
