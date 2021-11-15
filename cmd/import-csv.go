package cmd

import (
	"github.com/GlobalFishingWatch/bq2psql-tool/internal/action"
	"github.com/GlobalFishingWatch/bq2psql-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func init() {
	importCsvCmd.Flags().StringP("project-id", "", "", "Project id related to BigQuery database (required)")
	importCsvCmd.MarkFlagRequired("project-id")
	importCsvCmd.Flags().StringP("query", "", "", "Query to find data in BigQuery (required)")
	importCsvCmd.MarkFlagRequired("query")

	importCsvCmd.Flags().StringP("temporal-dataset", "", "", "The name of dataset to the temporal table")
	importCsvCmd.MarkFlagRequired("temporal-dataset")
	importCsvCmd.Flags().StringP("temporal-bucket", "", "", "The name of the bucket to upload the CSV")
	importCsvCmd.MarkFlagRequired("temporal-bucket")

	importCsvCmd.Flags().StringP("postgres-instance", "", "", "")
	importCsvCmd.MarkFlagRequired("postgres-instance")
	importCsvCmd.Flags().StringP("postgres-table", "", "", "")
	importCsvCmd.MarkFlagRequired("postgres-table")
	importCsvCmd.Flags().StringP("postgres-table-columns", "", "", "")
	importCsvCmd.MarkFlagRequired("postgres-table-columns")

	viper.BindPFlag("import-csv-project-id", importCsvCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("import-csv-query", importCsvCmd.Flags().Lookup("query"))
	viper.BindPFlag("import-csv-temporal-dataset", importCsvCmd.Flags().Lookup("temporal-dataset"))
	viper.BindPFlag("import-csv-temporal-bucket", importCsvCmd.Flags().Lookup("temporal-bucket"))
	viper.BindPFlag("import-csv-postgres-instance", importCsvCmd.Flags().Lookup("postgres-instance"))
	viper.BindPFlag("import-csv-postgres-table", importCsvCmd.Flags().Lookup("postgres-table"))
	viper.BindPFlag("import-csv-postgres-table-columns", importCsvCmd.Flags().Lookup("postgres-table-columns"))

	rootCmd.AddCommand(importCsvCmd)
}

var importCsvCmd = &cobra.Command{
	Use:   "import-csv",
	Short: "Import data from BigQuery to Postgres",
	Long:  `Import data from BigQuery to Postgres
Format:
	bq2psql import --project-id= --query= --table-name= --table-schema= --postgres-address= --postgres-user= --postgres-password= --postgres-database= --view-name=
Example:
	bq2psql import \
	  --project-id=world-fishing-827 \
	  --query="SELECT * FROM vessels" \
	  --table-name="vessels_2021_02_01" \
	  --table-schema="flag VARCHAR(3), first_transmission_date VARCHAR, last_transmission_date VARCHAR, id VARCHAR, mmsi VARCHAR, imo VARCHAR, callsign VARCHAR, shipname VARCHAR" \
	  --postgres-address="localhost:5432" \
	  --postgres-user="postgres" \
	  --postgres-password="XaD2sd$34Sdas1$ae" \
	  --postgres-database="postgres" 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing Import command")

		params := types.ImportCsvParams{
			Query:     viper.GetString("import-csv-query"),
			ProjectId: viper.GetString("import-csv-project-id"),
			TemporalDataset:    viper.GetString("import-csv-temporal-dataset"),
			TemporalBucket:    viper.GetString("import-csv-temporal-bucket"),
			DestinationTableName:    viper.GetString("import-postgres-table-name"),
		}

		postgresConfig := types.CloudSqlConfig{
			Instance:     viper.GetString("import-csv-postgres-instance"),
			Table:     viper.GetString("import-csv-postgres-table"),
			Columns:     viper.GetString("import-csv-postgres-table-columns"),
		}

		action.ImportCsvBigQueryToPostgres(params, postgresConfig)
		log.Println("→ Executing Import command finished")
	},
}

