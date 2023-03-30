package mysql_utils

import (
	"database/sql"
	"log"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/zapscloud/golib-dbutils/db_common"
	"github.com/zapscloud/golib-utils/utils"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
}

func openMySqlDbConnection(dataSourceName string) (*sqlx.DB, error) {
	var err error

	log.Println("OpenMySqlDbConnection :: Begin")

	dbDriver := "mysql"
	dbconnection, err := sqlx.Connect(dbDriver, dataSourceName)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("OpenMySqlDbConnection :: End")

	return dbconnection, nil
}

func BuildDSN(db_host string, db_port string, db_user string, db_password string, db_name string) string {

	log.Println("BuildDSN :: Begin")
	dsn := db_user + ":" + db_password + "@tcp(" + db_host + ":" + db_port + ")/" + db_name // daabase stuff
	log.Println("BuildDSN :: End")

	return dsn
}

// getMySqlDbConnection - Collection return
func getMySqlDbConnection(dbhost string, dbport string, dbname string, dbuser string, dbpassword string) (utils.Map, error) {

	log.Printf("GetMySqlDbCollection :: Begin")

	log.Println("BuildDSN :: Begin")
	dataSourceName := dbuser + ":" + dbpassword + "@tcp(" + dbhost + ":" + dbport + ")/" + dbname // daabase stuff
	log.Println("BuildDSN :: End")

	dbconnection, err := openMySqlDbConnection(dataSourceName)

	log.Println("GetMySqlDbCollection :: end : Connection Name", dataSourceName)
	log.Println("GetMySqlDbCollection :: end", err)

	dbmap := utils.Map{}

	dbmap[db_common.DB_CONNECTION] = dbconnection
	dbmap[db_common.DB_NAME] = dbname
	dbmap[db_common.DB_TYPE] = db_common.DATABASE_TYPE_MYSQLDB

	return dbmap, nil
}

// CloseMySqlDb - Close db connection
func closeMySqlDb(dbmap utils.Map) error {

	dbconnection := dbmap[db_common.DB_CONNECTION].(*sqlx.DB)

	log.Println("CloseMySqlDb :: Begin")
	if dbconnection == nil {
		log.Println("Connection to MySqlDb not open.")
		log.Println("CloseMySqlDb :: End")
		return nil
	}
	// Close the connection once no longer needed
	err := dbconnection.Close()

	if err != nil {
		log.Println("CloseMySqlDb :: Close Error")
		log.Fatal(err)
	} else {
		log.Println("Connection to MySqlDb closed.")
	}

	log.Println("CloseMySqlDb :: End")
	return nil
}

func transformRowsToMap(rows *sqlx.Rows) (utils.Map, error) {

	result := utils.Map{}

	columns, err := rows.Columns()
	if err != nil {
		return result, err
	}

	columnsTypes, err := rows.ColumnTypes()
	if err != nil {
		return result, err
	}

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for i := 0; i < count; i++ {
		valuePtrs[i] = &values[i]

	}
	rows.Scan(valuePtrs...)
	result = make(utils.Map)
	for i, col := range columns {
		var v interface{}
		val := values[i]
		b, _ := val.([]byte)
		if columnsTypes[i].DatabaseTypeName() == "BIGINT" {
			v, _ = strconv.Atoi(string(b))
		} else if columnsTypes[i].DatabaseTypeName() == "DOUBLE" {
			v, _ = strconv.ParseFloat(string(b), 32)
		} else if columnsTypes[i].DatabaseTypeName() == "CHAR" {
			v = string(b)
		} else if columnsTypes[i].DatabaseTypeName() == "VARCHAR" {
			v = string(b)
		} else if columnsTypes[i].DatabaseTypeName() == "INT" {
			v = val
		} else {
			v = string(b)
		}
		result[col] = v
	}
	return result, err
}

func ExecQuery(dbmap utils.Map, sqlString string, params utils.Map) ([]utils.Map, error) {
	results := []utils.Map{}
	log.Println("results", results)

	var err error
	var rows *sqlx.Rows

	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transaction := txnval.(*sqlx.Tx)
		rows, err = transaction.NamedQuery(sqlString, params)
	} else if txnval, ok := dbmap[db_common.DB_CONNECTION]; ok {
		dbconnection := txnval.(*sqlx.DB)
		rows, err = dbconnection.NamedQuery(sqlString, params)
	} else {
		err := utils.AppError{}
		err.ErrorCode = "5001"
		err.ErrorMsg = "Connection not found"
		err.ErrorDetail = "Connection not created, create connection before query"
		return results, &err
	}

	if err != nil {
		return results, err
	}
	defer rows.Close()

	for rows.Next() {
		result, err := transformRowsToMap(rows)
		log.Println("Result row to map ", err)
		results = append(results, result)
	}
	return results, nil
}

func ExecSelectOne(dbmap utils.Map, sqlString string, params utils.Map) (utils.Map, error) {

	var err error
	var rows *sqlx.Rows

	result := utils.Map{}

	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transaction := txnval.(*sqlx.Tx)
		rows, err = transaction.NamedQuery(sqlString, params)
	} else if txnval, ok := dbmap[db_common.DB_CONNECTION]; ok {
		dbconnection := txnval.(*sqlx.DB)
		rows, err = dbconnection.NamedQuery(sqlString, params)
	} else {
		err := utils.AppError{}
		err.ErrorCode = "5001"
		err.ErrorMsg = "Connection not found"
		err.ErrorDetail = "Connection not created, create connection before query"
		return result, &err
	}

	if err != nil {
		return result, err
	}
	log.Println("rows", rows)
	defer rows.Close()

	if rows.Next() {
		result, err = transformRowsToMap(rows)
		log.Println("Result row to map ", err)
		return result, err
	}
	return result, nil
}

func Exec(dbmap utils.Map, sqlString string, params utils.Map) (utils.Map, error) {
	result := utils.Map{}
	log.Println("Update Query ", sqlString, params)

	var resultSet sql.Result
	var err error

	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transaction := txnval.(*sqlx.Tx)
		resultSet, err = transaction.NamedExec(sqlString, params)
	} else if dbval, ok := dbmap[db_common.DB_CONNECTION]; ok {
		dbconnection := dbval.(*sqlx.DB)
		resultSet, err = dbconnection.NamedExec(sqlString, params)
	} else {
		err := utils.AppError{}
		err.ErrorCode = "5001"
		err.ErrorMsg = "Connection not found"
		err.ErrorDetail = "Connection not created, create connection before query"
		return result, &err
	}

	log.Println("Update err ", err)
	rowsAffected, err := resultSet.RowsAffected()
	log.Println("rowsAffected err ", err)

	lastInsertId, err := resultSet.LastInsertId()
	log.Println("lastInsertId err ", err)

	result["rows_affected"] = rowsAffected
	result["last_insert_id"] = lastInsertId

	return result, nil
}

func txnBegin(dbmap utils.Map) utils.Map {

	dbconnection := dbmap[db_common.DB_CONNECTION].(*sqlx.DB)
	txn := dbconnection.MustBegin()
	dbmap[db_common.DB_TRANSACTION] = txn
	log.Println("TxnBegin")
	return dbmap
}

func txnCommit(dbmap utils.Map) error {

	var err error
	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transaction := txnval.(*sqlx.Tx)
		err := transaction.Commit()
		if err == nil {
			delete(dbmap, db_common.DB_TRANSACTION)
		}
		log.Println("TxnCommit", err)
	}
	return err
}

func txnRollback(dbmap utils.Map) error {

	var err error

	if txnval, ok := dbmap[db_common.DB_TRANSACTION]; ok {
		transaction := txnval.(*sqlx.Tx)
		err := transaction.Rollback()
		if err == nil {
			delete(dbmap, db_common.DB_TRANSACTION)
		}
		log.Println("TxnRollback", err)
	}
	return err
}
