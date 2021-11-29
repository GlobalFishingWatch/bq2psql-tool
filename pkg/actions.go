package pkg

import (
	"github.com/GlobalFishingWatch/bq2psql-tool/internal/action"
	"github.com/GlobalFishingWatch/bq2psql-tool/types"
)

func ImportBigQueryToPostgres(params types.ImportParams, postgresConfig types.PostgresConfig) {
	action.ImportBigQueryToPostgres(params, postgresConfig)
}

func ImportCsvBigQueryToPostgres(params types.ImportCsvParams, postgresConfig types.CloudSqlConfig) {
	action.ImportCsvBigQueryToPostgres(params, postgresConfig)
}