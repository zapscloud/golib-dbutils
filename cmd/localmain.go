package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kr/pretty"
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-dbutils/mongo_utils"
	"github.com/zapscloud/golib-dbutils/mysql_utils"
	"github.com/zapscloud/golib-utils/utils"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {

	TestMongoDB()
	log.Println("Sleeping....")
	time.Sleep(5 * 1000 * time.Millisecond)

}

func TestMongoDB() {
	var result utils.Map

	staffId := "user_ck1vobbo82bmsj74qphg"
	businessId := "biz_ck1vbggh9gvr8qg3kpcg"

	srv := mongo_utils.BaseMongoService{}

	filter := bson.D{{Key: "staff_id", Value: staffId}, {}}

	filter = append(filter,
		bson.E{Key: "business_id", Value: businessId},
		bson.E{Key: db_common.FLD_IS_DELETED, Value: false})

	log.Println("Get:: Got filter ", filter)

	for i := 0; i < 10; i++ {
		srv.OpenDatabaseService(GetReadDBCreds())

		collection, ctx, err := mongo_utils.GetMongoDbCollection(srv.GetClient(), "zc_hr_staffs")
		if err != nil {
			return
		}

		singleResult := collection.FindOne(ctx, filter)
		if singleResult.Err() != nil {
			log.Println("Get:: Record not found ", singleResult.Err())
			return
		}

		singleResult.Decode(&result)
		if err != nil {
			log.Println("Error in decode", err)
			return
		}
		log.Println("Found Record: ", result)

		srv.CloseDatabaseService()

	}

}

func GetReadDBCreds() utils.Map {
	dbtype := db_common.DATABASE_TYPE_MONGODB
	dbserver := os.Getenv("READ_MONGODB_SERVER")
	dbname := os.Getenv("READ_MONGODB_NAME")
	dbuser := os.Getenv("READ_MONGODB_USER")
	dbsecret := os.Getenv("READ_MONGODB_SECRET")

	readCreds := utils.Map{
		db_common.DB_TYPE:   dbtype,
		db_common.DB_SERVER: dbserver,
		db_common.DB_NAME:   dbname,
		db_common.DB_USER:   dbuser,
		db_common.DB_SECRET: dbsecret,
	}

	return readCreds
}

func TestMySQL() {
	fmt.Println("Merchant Module")

	dbUser := os.Getenv("READ_DB_USER")
	dbPass := os.Getenv("READ_DB_PASSWORD")
	dbName := os.Getenv("READ_DB_DATABASE")
	dbHost := os.Getenv("READ_DB_SRV_HOST")
	dbPort := os.Getenv("READ_DB_PORT")

	dsn := mysql_utils.BuildDSN(dbHost, dbPort, dbUser, dbPass, dbName)
	log.Println("DataSourceName READ", dsn)

	srv := mysql_utils.BaseMySqlService{}

	var props utils.Map

	rdberr := srv.OpenDatabaseService(props)
	if rdberr != nil {
		log.Fatalln(rdberr)
	}
	defer srv.CloseDatabaseService()

	query_list := "select * from tbl_merchant limit 10"
	lst, err := mysql_utils.ExecQuery(srv.GetClient(), query_list, utils.Map{})
	if rdberr != nil {
		log.Fatalln(err)
	}

	log.Println("List values ")
	pretty.Print(lst)

	query_one := "select * from tbl_merchant limit 10"
	result, err := mysql_utils.ExecSelectOne(srv.GetClient(), query_one, utils.Map{})
	if rdberr != nil {
		log.Fatalln(err)
	}

	log.Println("Result Query One")
	pretty.Print(result)

	dbUser = os.Getenv("WRITE_DB_USER")
	dbPass = os.Getenv("WRITE_DB_PASSWORD")
	dbName = os.Getenv("WRITE_DB_DATABASE")
	dbHost = os.Getenv("WRITE_DB_SRV_HOST")
	dbPort = os.Getenv("WRITE_DB_PORT")

	dsn = mysql_utils.BuildDSN(dbHost, dbPort, dbUser, dbPass, dbName)
	fmt.Println("DataSourceName WRITE", dsn)

	wsrv := mysql_utils.BaseMySqlService{}

	wdberr := wsrv.OpenDatabaseService(props)
	if wdberr != nil {
		log.Fatalln(wdberr)
	}
	defer wsrv.CloseDatabaseService()
}
