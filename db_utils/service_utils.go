package db_utils

import (
	"fmt"

	"github.com/zapscloud/golib-utils/utils"
)

type CommonSvc interface {
	// Get - Find By Code
	Get(keyId string) (utils.Map, error)
	// Find - Find by filter
	Find(filter string) (utils.Map, error)
}

func GetUniqueKeyId(svc CommonSvc, key string, value string) string {
	filter := fmt.Sprintf(`{"%s":"%s"}`, key, value)

	_, err := svc.Find(filter)
	if err == nil {
		// KeyId already found, look for next one
		return GetUniqueKeyId(svc, key, utils.GenerateNextKeyId(value))
	}

	return value
}

func IsGetValueExist(svc CommonSvc, value string, shouldExist bool) (utils.Map, error) {
	var err error = nil

	resp, _ := svc.Get(value)
	if len(resp) == 0 {
		// Record not Found
		if shouldExist {
			err = &utils.AppError{ErrorStatus: 4023, ErrorMsg: "Get Error", ErrorDetail: "'" + value + "' is not exist"}
		}
	} else {
		// Record Found
		if !shouldExist {
			err = &utils.AppError{ErrorStatus: 4023, ErrorMsg: "Get Error", ErrorDetail: "'" + value + "' is already exist"}
		}
	}

	return resp, err
}

func IsFindValueExist(svc CommonSvc, key string, value string, shouldExist bool) (utils.Map, error) {
	var err error = nil
	filter := fmt.Sprintf(`{"%s":"%s"}`, key, value)

	resp, _ := svc.Find(filter)
	if len(resp) == 0 {
		// Record not Found
		if shouldExist {
			err = &utils.AppError{ErrorStatus: 4023, ErrorMsg: "Find Error", ErrorDetail: "'" + value + "' is not exist"}
		}
	} else {
		// Record Found
		if !shouldExist {
			err = &utils.AppError{ErrorStatus: 4023, ErrorMsg: "Find Error", ErrorDetail: "'" + value + "' is already exist"}
		}
	}
	return resp, err
}

func IsFindFilterExist(svc CommonSvc, filter string, shouldExist bool) (utils.Map, error) {
	var err error = nil

	resp, _ := svc.Find(filter)
	if len(resp) == 0 {
		// Record not Found
		if shouldExist {
			err = &utils.AppError{ErrorStatus: 4023, ErrorMsg: "Find Error", ErrorDetail: "'" + filter + "' is not exist"}
		}
	} else {
		// Record Found
		if !shouldExist {
			err = &utils.AppError{ErrorStatus: 4023, ErrorMsg: "Find Error", ErrorDetail: "'" + filter + "' is already exist"}
		}
	}
	return resp, err
}

// Get or Find Record depends on the QueryId
func GetRecord(svc CommonSvc, queryId string, keyId string) (utils.Map, error) {
	var data utils.Map
	var err error = nil

	if len(queryId) > 0 {
		// Yes FINDBY parameter is received, find the record based on fields which passed in
		data, err = IsFindValueExist(svc, queryId, keyId, true)

	} else {
		// No Query Parameter is passed, get the record based on given brandId
		data, err = IsGetValueExist(svc, keyId, true)
	}

	return data, err
}
