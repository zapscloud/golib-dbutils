package mysql_utils

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-utils/utils"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

type BaseMySqlService struct {
	client utils.Map
}

func (p *BaseMySqlService) OpenDatabaseService(props utils.Map) error {
	var err error

	log.Println("MySqlDB Connection Props ", props)
	dbhost, err := utils.IsMemberExist(props, db_common.DB_SERVER)
	if err != nil {
		return err
	}

	dbport, err := utils.IsMemberExist(props, db_common.DB_PORT)
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

	p.client, err = getMySqlDbConnection(dbhost, dbport, dbname, dbuser, dbsecret)
	if err != nil {
		log.Println("OpenMySqlService Error", err)
	}
	log.Printf("UserMySqlService ")
	return err
}

func (p *BaseMySqlService) CloseDatabaseService() {
	log.Printf("CloseMySqlService ")
	closeMySqlDb(p.client)
}

func (p *BaseMySqlService) GetClient() utils.Map {
	log.Printf("Get MySqlService Client")
	return (p.client)
}

func (p *BaseMySqlService) BeginTransaction() {
	log.Printf("BeginTransaction ")
	txnBegin(p.client)
}

func (p *BaseMySqlService) CommitTransaction() {
	log.Printf("CommitTransaction ")
	txnCommit(p.client)
}

func (p *BaseMySqlService) RollbackTransaction() {
	log.Printf("RollbackTransaction ")
	txnRollback(p.client)
}
