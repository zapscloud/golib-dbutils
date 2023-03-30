package zapsdb_utils

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-utils/utils"
	"github.com/zapscloud/golib/database"
	"github.com/zapscloud/golib/zaps"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

// OpenZapsDbConnection get connection of mongodb
func openZapsDbConnection(dbapp string, dbuser string, dbsecret string) (utils.Map, error) {
	var err error

	log.Println("OpenZapsDbConnection :: Begin")

	zapscloud, err := zaps.NewZapsCloud(dbuser, dbsecret, dbapp)
	// zapscloud.SetStage("dev")

	log.Println("Error ", err)

	client, err := database.NewZapsDB(zapscloud)
	log.Println("Error ", err)

	dbmap := utils.Map{}
	dbmap[db_common.DB_CONNECTION] = client
	dbmap[db_common.DB_TYPE] = db_common.DATABASE_TYPE_ZAPSDB

	log.Println("OpenZapsDbConnection :: End")

	return dbmap, nil
}

// CloseZapsDb - Close db connection
func closeZapsDb(dbmap utils.Map) error {

	dbconnection := dbmap[db_common.DB_CONNECTION].(*database.ZapsDB)

	log.Println("CloseZapsDb :: Begin")
	if dbconnection == nil {
		log.Println("Connection to MongoDB not open.")
		log.Println("CloseZapsDb :: End")
		return nil
	}
	// Close the connection once no longer needed
	log.Println("CloseZapsDb :: End")
	return nil
}

// CloseZapsDb - Close db connection
func GetConnection(dbmap utils.Map) (*database.ZapsDB, string) {

	dbconnection := dbmap[db_common.DB_CONNECTION].(*database.ZapsDB)

	log.Println("CloseZapsDb :: Begin")
	if dbconnection == nil {
		log.Println("Connection to MongoDB not open.")
		log.Println("CloseZapsDb :: End")
		return nil, ""
	}
	// Close the connection once no longer needed
	log.Println("CloseZapsDb :: End")

	transactionid := ""
	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transactionid = txnval.(string)
	}
	return dbconnection, transactionid

}

func txnBegin(dbmap utils.Map) utils.Map {

	dbconnection := dbmap[db_common.DB_CONNECTION].(*database.ZapsDB)
	txn, err := dbconnection.StartTransaction()
	log.Println("TxnBegin", err)
	dbmap[db_common.DB_TRANSACTION] = txn["transaction_id"]
	log.Println("TxnBegin")
	return dbmap
}

func txnCommit(dbmap utils.Map) error {

	var err error
	dbconnection := dbmap[db_common.DB_CONNECTION].(*database.ZapsDB)

	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transaction := txnval.(string)
		res, err := dbconnection.CommitTransaction(transaction)
		if err == nil {
			delete(dbmap, db_common.DB_TRANSACTION)
		}
		log.Println("TxnCommit", err, res)
	}
	return err
}

func txnRollback(dbmap utils.Map) error {

	var err error

	dbconnection := dbmap[db_common.DB_CONNECTION].(*database.ZapsDB)
	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transaction := txnval.(string)
		res, err := dbconnection.RollbackTransaction(transaction)
		if err == nil {
			delete(dbmap, db_common.DB_TRANSACTION)
		}
		log.Println("TxnRollback", err, res)
	}
	return err
}
