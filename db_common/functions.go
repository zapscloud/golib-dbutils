package db_common

import (
	"time"

	"github.com/zapscloud/golib-utils/utils"
)

// Add Fields for Create
func AmendFldsforCreate(data utils.Map) utils.Map {
	data[FLD_CREATED_AT] = time.Now()
	data[FLD_IS_DELETED] = false

	return data
}

// Modify Fields for Update
func AmendFldsforUpdate(data utils.Map) utils.Map {
	// Delete below fields
	delete(data, FLD_DEFAULT_ID)
	delete(data, FLD_CREATED_AT)
	delete(data, FLD_CREATED_BY)

	// Add/modify Updated field
	data[FLD_UPDATED_AT] = time.Now()

	return data
}

func AmendFldsForGet(data utils.Map) utils.Map {

	// Delete below fields
	delete(data, FLD_DEFAULT_ID)
	delete(data, FLD_IS_DELETED)

	return data
}

func GetDatabaseType(dbDetails utils.Map) (DatabaseType, error) {
	var dbType DatabaseType
	var err error

	dataValue, dataOk := dbDetails[DB_TYPE]
	if dataOk {
		// DatabaseType value supplied in dbDetails
		dbType = dataValue.(DatabaseType)

		// Validate Database type
		if dbType == DATABASE_TYPE_MONGODB ||
			dbType == DATABASE_TYPE_ZAPSDB ||
			dbType == DATABASE_TYPE_MYSQLDB {
			err = nil
		} else {
			dbType = DATABASE_TYPE_NONE
			err = &utils.AppError{ErrorStatus: 400, ErrorMsg: "Invalid DatabaseType", ErrorDetail: "Invalid DatabaseType"}
		}

	} else {
		// DatabaseType value not supplied in dbDetails
		dbType = DATABASE_TYPE_NONE
		err = &utils.AppError{ErrorStatus: 400, ErrorMsg: "DatabaseType Not Supplied", ErrorDetail: "DatabaseType Not Supplied"}
	}

	return dbType, err
}
