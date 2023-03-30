package main

import (
	"fmt"
	"log"
	"os"

	"github.com/kr/pretty"
	"github.com/zapscloud/golib-dbutils/mysql_utils"
	"github.com/zapscloud/golib-utils/utils"
)

func main() {
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
