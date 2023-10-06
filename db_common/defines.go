package db_common

const (
	LOCALE = "en_US"
)

// Enums
type DatabaseType byte

const (
	DATABASE_TYPE_NONE DatabaseType = iota
	DATABASE_TYPE_MONGODB
	DATABASE_TYPE_MYSQLDB
	DATABASE_TYPE_ZAPSDB
)

const (
	// Database collection prefix
	DB_COLLECTION_PREFIX = "zc_"

	// Common for all DBs
	DB_TYPE       = "dbtype"
	DB_OPEN_COUNT = "open_count"

	// MongoDB Parameters
	DB_SERVER = "dbserver"
	DB_PORT   = "dbport"
	DB_NAME   = "dbname"
	DB_USER   = "dbuser"
	DB_SECRET = "dbsecret"

	// ZapsDB Parameters
	DB_APP = "dbapp"
	DB_KEY = "dbkey"
	// DB_SECRET = "dbsecret" - Already defined above

	// DB Instances Parameters
	DB_CONNECTION  = "dbconnection"
	DB_TRANSACTION = "dbtransaction"
)

// MongoDB Query string const
const (
	MONGODB_SET           = "$set"
	MONGODB_CONDITION_AND = "$and"
	MONGODB_CONDITION_OR  = "$or"
	MONGODB_CONDITION_GT  = "$gt"
)

// Default values
const (
	DEF_REGION_ID   = "global"
	DEF_REGION_NAME = "Global database for all business"
)

const (
	LIST_SUMMARY      = "summary"
	LIST_TOTALSIZE    = "totalsize"
	LIST_FILTEREDSIZE = "filteredsize"
	LIST_RESULTSIZE   = "resultsize"
	LIST_RESULT       = "result"
)

// Common Fields
const (
	// Fields for all tables
	FLD_DEFAULT_ID = "_id"
	FLD_CREATED_AT = "created_at"
	FLD_CREATED_BY = "created_by"
	FLD_UPDATED_AT = "updated_at"
	FLD_UPDATED_BY = "updated_by"
	FLD_IS_DELETED = "is_deleted"

	// Fields for some tables
	FLD_IS_ACTIVATED      = "is_activated"
	FLD_IS_SUSPENDED      = "is_suspended"
	FLD_IS_VERIFIED       = "is_verified"
	FLD_IS_AUTO_GENERATED = "is_auto_generated" // Flag to indicate whether it generated by system or users
)

const BUSINESS_CREATED_FILTER_ALL = "ALL"
const BUSINESS_CREATED_FILTER_CUSTOMER = "CUSTOMER"
