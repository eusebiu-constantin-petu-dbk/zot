package userdbfactory

import (
	"zotregistry.io/zot/errors"
	"zotregistry.io/zot/pkg/meta/userdb"
	boltdb_wrapper "zotregistry.io/zot/pkg/meta/userdb/boltdb-wrapper"
	dynamodb_wrapper "zotregistry.io/zot/pkg/meta/userdb/dynamodb-wrapper"
	dynamoParams "zotregistry.io/zot/pkg/meta/userdb/dynamodb-wrapper/params"
)

func Create(dbtype string, parameters interface{}) (userdb.UserSecurityDB, error) { //nolint:contextcheck
	switch dbtype {
	case "boltdb":
		{
			properParameters, ok := parameters.(boltdb_wrapper.DBParameters)
			if !ok {
				panic("failed type assertion")
			}

			return boltdb_wrapper.NewBoltDBWrapper(properParameters)
		}
	case "dynamodb":
		{
			properParameters, ok := parameters.(dynamoParams.DBDriverParameters)
			if !ok {
				panic("failed type assertion")
			}

			return dynamodb_wrapper.NewDynamoDBWrapper(properParameters)
		}
	default:
		{
			return nil, errors.ErrBadConfig
		}
	}
}