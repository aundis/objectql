package main

import (
	"gopkg.in/mgo.v2"
)

func (o *Objectql) getCollection(api string) *mgo.Collection {
	return session.DB("test").C(api)
}
