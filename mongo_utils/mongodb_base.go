package mongo_utils

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-utils/utils"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

type BaseMongoService struct {
	client utils.Map
}

func (p *BaseMongoService) OpenDatabaseService(props utils.Map) error {

	log.Println("MongoDB Connection Props ", props)
	dbserver, err := utils.IsMemberExist(props, db_common.DB_SERVER)
	if err != nil {
		return err
	}

	dbname, err := utils.IsMemberExist(props, db_common.DB_NAME)
	if err != nil {
		return err
	}

	dbuser, err := utils.IsMemberExist(props, db_common.DB_USER)
	if err != nil {
		return err
	}

	dbsecret, err := utils.IsMemberExist(props, db_common.DB_SECRET)
	if err != nil {
		return err
	}

	p.client, err = openMongoDbConnection(dbserver, dbname, dbuser, dbsecret)
	if err != nil {
		log.Println("OpenMongoService Error", err)
		return err
	}
	log.Printf("UserMongoService ")
	return err
}

func (p *BaseMongoService) CloseDatabaseService() {
	log.Printf("CloseMongoService ")
	closeMongoDb(p.client)
}

func (p *BaseMongoService) GetClient() utils.Map {
	log.Printf("Get Client")
	return (p.client)
}

func (p *BaseMongoService) BeginTransaction() {
	log.Printf("BeginTransaction ")
	txnBegin(p.client)
}

func (p *BaseMongoService) CommitTransaction() {
	log.Printf("CommitTransaction ")
	txnCommit(p.client)
}

func (p *BaseMongoService) RollbackTransaction() {
	log.Printf("RollbackTransaction ")
	txnRollback(p.client)
}
