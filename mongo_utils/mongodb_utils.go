package mongo_utils

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-utils/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

// OpenMongoDbConnection get connection of mongodb
func openMongoDbConnection(dbserver string, dbname string, dbuser string, dbsecret string) (utils.Map, error) {
	var err error
	var dburl string

	log.Println("OpenMongoDbConnection :: Begin")

	// 20221013 KN: Added code for localhost
	if strings.Contains(dbserver, "localhost") || strings.Contains(dbserver, "127.0.0.1") {
		dburl = dbserver
	} else {
		dburl = fmt.Sprintf("mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority", dbuser, dbsecret, dbserver)
	}
	log.Println("OpenMongoDbConnection :: URL ", dburl)

	client, err := mongo.NewClient(options.Client().ApplyURI(dburl))
	if err != nil {
		log.Println("OpenMongoDbConnection :: Client Creation Error")
		return nil, err
		// log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Println("OpenMongoDbConnection :: Connection Check Failed")
		return nil, err
		// log.Fatal(err)
	}

	dbmap := utils.Map{}

	dbmap[db_common.DB_CONNECTION] = client
	dbmap[db_common.DB_NAME] = dbname
	dbmap[db_common.DB_TYPE] = db_common.DATABASE_TYPE_MONGODB

	log.Println("OpenMongoDbConnection :: End")

	return dbmap, nil
}

// // GetMongoDbCollection - Collection return
// func GetMongoDbCollection(dbmap utils.Map, collectionName string) (*mongo.Collection, error) {

// 	dbname := os.Getenv("ZERVEE_DB_NAME")
// 	log.Printf("GetMongoDbCollection :: Begin %s %s", dbname, collectionName)

// 	client := dbmap[db_common.DB_CONNECTION].(*mongo.Client)

// 	collection := client.Database(dbname).Collection(collectionName)
// 	log.Println("GetMongoDbCollection :: end")

// 	return collection, nil
// }

// GetMongoDbCollection - Collection return
func GetMongoDbCollection(dbmap utils.Map, collectionName string) (*mongo.Collection, context.Context, error) {

	log.Println("GetMongoDbCollection :: Begin", collectionName)

	var collection *mongo.Collection

	txnclient, okv1 := dbmap[db_common.DB_CONNECTION]
	txnsessionctx, okv2 := dbmap["session_context"]

	dbnameval, okv3 := dbmap[db_common.DB_NAME]

	if !okv3 {
		err := utils.AppError{}
		err.ErrorCode = "S020101"
		err.ErrorMsg = "Connection not found"
		err.ErrorDetail = "Connection not created, create connection before query"
		return nil, nil, &err
	}

	if okv1 {
		client := txnclient.(*mongo.Client)
		dbname := dbnameval.(string)
		collection = client.Database(dbname).Collection(collectionName)
	} else {
		err := utils.AppError{}
		err.ErrorCode = "S020102"
		err.ErrorMsg = "Connection not found"
		err.ErrorDetail = "Connection not created, create connection before query"
	}

	sessionContext := context.Background()
	if okv2 {
		sessionContext = txnsessionctx.(mongo.SessionContext)
	}
	log.Println("GetMongoDbCollection :: end")
	return collection, sessionContext, nil
}

// CloseMongoDb - Close db connection
func closeMongoDb(dbmap utils.Map) error {

	log.Println("CloseMongoDb :: Begin")

	client := dbmap[db_common.DB_CONNECTION].(*mongo.Client)

	if client == nil {
		log.Println("Connection to MongoDB not open.")
		log.Println("CloseMongoDb :: End")
		return nil
	}
	// Close the connection once no longer needed
	err := client.Disconnect(context.Background())

	if err != nil {
		log.Println("CloseMongoDb :: Disconnect Error")
		return err
		// log.Fatal(err)
	} else {
		log.Println("Connection to MongoDB closed.")
	}

	log.Println("CloseMongoDb :: End")
	return nil
}

func txnBegin(dbmap utils.Map) utils.Map {
	dbconnection := dbmap[db_common.DB_CONNECTION].(*mongo.Client)
	session, err := dbconnection.StartSession()
	log.Println("Mongo DB TxnBegin: StartSession", err)

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)

	err = session.StartTransaction(txnOpts)
	log.Println("Mongo DB TxnBegin", err)
	var ctx = context.Background()

	if err = mongo.WithSession(ctx, session, func(sessionContext mongo.SessionContext) error {
		dbmap["context"] = ctx
		dbmap["session"] = session
		dbmap["session_context"] = sessionContext
		return err

	}); err != nil {
		log.Println("Session start error ", err)
	}

	log.Println("Mongo DB TxnBegin")
	return dbmap
}

func txnCommit(dbmap utils.Map) error {

	var err error
	txnsessionctx, okv1 := dbmap["session_context"]
	txnsession, okv2 := dbmap["session"]
	txnctx, okv3 := dbmap["context"]

	if okv1 && okv2 && okv3 {
		sessionContext := txnsessionctx.(mongo.SessionContext)
		session := txnsession.(mongo.Session)
		ctx := txnctx.(context.Context)
		err := session.CommitTransaction(sessionContext)
		if err == nil {
			delete(dbmap, "session_context")
			delete(dbmap, "session")
			delete(dbmap, "context")
		}
		log.Println("TxnCommit", err)
		session.EndSession(ctx)
	}
	return err
}

func txnRollback(dbmap utils.Map) error {

	var err error
	txnsessionctx, okv1 := dbmap["session_context"]
	txnsession, okv2 := dbmap["session"]
	txnctx, okv3 := dbmap["context"]

	if okv1 && okv2 && okv3 {
		sessionContext := txnsessionctx.(mongo.SessionContext)
		session := txnsession.(mongo.Session)
		ctx := txnctx.(context.Context)
		err := session.AbortTransaction(sessionContext)
		if err == nil {
			delete(dbmap, "session_context")
			delete(dbmap, "session")
			delete(dbmap, "context")
		}
		log.Println("TxnRollback", err)
		session.EndSession(ctx)
	}
	return err
}
