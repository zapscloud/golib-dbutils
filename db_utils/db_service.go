package db_utils

import (
	"log"

	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/mongo_utils"
	"github.com/zapscloud/golib-dbutils/mysql_utils"
	"github.com/zapscloud/golib-dbutils/zapsdb_utils"
	"github.com/zapscloud/golib-utils/utils"
)

type DatabaseService struct {
	dbService interfaceDatabaseService
}

type interfaceDatabaseService interface {
	OpenDatabaseService(props utils.Map) error
	CloseDatabaseService()
	GetClient() utils.Map
	BeginTransaction()
	CommitTransaction()
	RollbackTransaction()
}

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

// OpenDatabaseService
func (p *DatabaseService) OpenDatabaseService(props utils.Map) error {
	log.Printf("MongoDBService:: OpenDBService")

	// Get DatabaseType from props
	dbType, err := db_common.GetDatabaseType(props)
	if err != nil {
		return err
	}

	// Get Appropriate DBObject
	p.dbService = p.getDatabaseService(dbType)

	// Open DBService
	err = p.dbService.OpenDatabaseService(props)

	return err
}

// CloseDatabaseService
func (p *DatabaseService) CloseDatabaseService() {
	log.Printf("MongoDBService:: CloseDBService")

	// Open DBService
	p.dbService.CloseDatabaseService()
}

// GetClient
func (p *DatabaseService) GetClient() utils.Map {
	return p.dbService.GetClient()
}

// Begin Transaction
func (p *DatabaseService) BeginTransaction() {
	p.dbService.BeginTransaction()
}

// Commit Transaction
func (p *DatabaseService) CommitTransaction() {
	p.dbService.CommitTransaction()
}

// Rollback Transaction
func (p *DatabaseService) RollbackTransaction() {
	p.dbService.RollbackTransaction()
}

func (p *DatabaseService) getDatabaseService(dbType db_common.DatabaseType) interfaceDatabaseService {
	var serviceDatabase interfaceDatabaseService

	switch dbType {
	case db_common.DATABASE_TYPE_MONGODB:
		serviceDatabase = &mongo_utils.BaseMongoService{}
	case db_common.DATABASE_TYPE_ZAPSDB:
		serviceDatabase = &zapsdb_utils.BaseZapsService{}
	case db_common.DATABASE_TYPE_MYSQLDB:
		serviceDatabase = &mysql_utils.BaseMySqlService{}
	}
	return serviceDatabase
}
