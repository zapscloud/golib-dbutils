package zapsdb_utils

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-utils/utils"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

type BaseZapsService struct {
	client utils.Map
}

// OpenDatabaseService - Construct OpenDatabaseService
func (p *BaseZapsService) OpenDatabaseService(props utils.Map) error {

	log.Println("ZapsDB Connection Props ", props)
	dbapp, err := utils.IsMemberExist(props, db_common.DB_APP)
	if err != nil {
		return err
	}

	dbuser, err := utils.IsMemberExist(props, db_common.DB_KEY)
	if err != nil {
		return err
	}

	dbsecret, err := utils.IsMemberExist(props, db_common.DB_SECRET)
	if err != nil {
		return err
	}

	p.client, err = openZapsDbConnection(dbapp, dbuser, dbsecret)
	if err != nil {
		log.Println("OpenZapsService Error", err)
	}
	log.Printf("UserZapsService ")
	return err
}

func (p *BaseZapsService) CloseDatabaseService() {
	log.Printf("CloseZapsService ")
	closeZapsDb(p.client)
}

func (p *BaseZapsService) GetClient() utils.Map {
	log.Printf("Get ZapsService Client")
	return (p.client)
}

func (p *BaseZapsService) BeginTransaction() {
	log.Printf("BeginTransaction ")
	txnBegin(p.client)
}

func (p *BaseZapsService) CommitTransaction() {
	log.Printf("CommitTransaction ")
	txnCommit(p.client)
}

func (p *BaseZapsService) RollbackTransaction() {
	log.Printf("RollbackTransaction ")
	txnRollback(p.client)
}
