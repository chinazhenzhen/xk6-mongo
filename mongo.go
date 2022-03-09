package xk6_mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	k6common "go.k6.io/k6/js/common"
	k6modules "go.k6.io/k6/js/modules"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/mongo".
func init() {
	k6modules.Register("k6/x/mongo", new(Mongo))
}

// Mongo is the k6 extension for a Mongo client.
type Mongo struct{}

// Client is the Mongo client wrapper.
type Client struct {
	client *mongo.Client
}

// XClient represents the Client constructor (i.e. `new mongo.Client()`) and
// returns a new Mongo client object.
// connURI -> mongodb://username:password@address:port/db?connect=direct
func (m *Mongo) XClient(ctxPtr *context.Context, connURI string) interface{} {
	rt := k6common.GetRuntime(*ctxPtr)
	clientOptions := options.Client().ApplyURI(connURI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}
	return k6common.Bind(rt, &Client{client: client}, ctxPtr)
}

func (c *Client) Insert(database string, collection string, doc map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertOne(context.TODO(), doc)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Find(database string, collection string, filter map[string]string) bson.Raw {
	db := c.client.Database(database)
	col := db.Collection(collection)
	//log.Print("filter is ", filter)
	cur, err := col.Find(context.TODO(), filter)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	defer cur.Close(context.Background())

	for cur.Next(context.Background()) {
		result := struct {
			_id string
			a   int32
		}{}
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		return cur.Current
		//log.Print(raw)
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (c *Client) FindOne(database string, collection string, filter map[string]string) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	var result bson.M
	opts := options.FindOne().SetSort(bson.D{{"_id", 1}})
	log.Print("filter is ", filter)
	err := col.FindOne(context.TODO(), filter, opts).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("found document %v", result)
	return nil
}
