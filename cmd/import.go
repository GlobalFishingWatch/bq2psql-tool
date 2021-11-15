package cmd

import (
	"github.com/GlobalFishingWatch/bq2psql-tool/internal/action"
	"github.com/GlobalFishingWatch/bq2psql-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

func init() {
	importCmd.Flags().StringP("project-id", "p", "", "Project id related to BigQuery database (required)")
	importCmd.MarkFlagRequired("project-id")
	importCmd.Flags().StringP("query", "q", "", "Query to find data in BigQuery (required)")
	importCmd.MarkFlagRequired("query")
	importCmd.Flags().StringP("table-name", "t", "", "The name of the new table")
	importCmd.MarkFlagRequired("table-name")
	importCmd.Flags().StringP("table-schema", "", "", "The schema to create the table")

	importCmd.Flags().StringP("postgres-address", "", "", "The address of the database")
	importCmd.MarkFlagRequired("postgres-address")
	importCmd.Flags().StringP("postgres-user", "", "", "The destination credentials user")
	importCmd.MarkFlagRequired("postgres-user")
	importCmd.Flags().StringP("postgres-password", "", "", "The destination credentials password")
	importCmd.MarkFlagRequired("postgres-password")
	importCmd.Flags().StringP("postgres-database", "", "", "The destination database name")
	importCmd.MarkFlagRequired("postgres-database")

	viper.BindPFlag("import-project-id", importCmd.Flags().Lookup("project-id"))
	viper.BindPFlag("import-query", importCmd.Flags().Lookup("query"))
	viper.BindPFlag("import-table-name", importCmd.Flags().Lookup("table-name"))
	viper.BindPFlag("import-table-schema", importCmd.Flags().Lookup("table-schema"))
	viper.BindPFlag("import-postgres-address", importCmd.Flags().Lookup("postgres-address"))
	viper.BindPFlag("import-postgres-user", importCmd.Flags().Lookup("postgres-user"))
	viper.BindPFlag("import-postgres-password", importCmd.Flags().Lookup("postgres-password"))
	viper.BindPFlag("import-postgres-database", importCmd.Flags().Lookup("postgres-database"))

	rootCmd.AddCommand(importCmd)
}

var importCmd = &cobra.Command{
	Use:   "import",
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

		params := types.ImportParams{
			Query:     viper.GetString("import-query"),
			ProjectId: viper.GetString("import-project-id"),
			TableName: viper.GetString("import-table-name"),
			Schema:    viper.GetString("import-table-schema"),
		}

		postgresConfig := types.PostgresConfig{
			Addr:     viper.GetString("import-postgres-address"),
			User:     viper.GetString("import-postgres-user"),
			Password: viper.GetString("import-postgres-password"),
			Database: viper.GetString("import-postgres-database"),
		}

		action.ImportBigQueryToPostgres(params, postgresConfig)
		log.Println("→ Executing Import command finished")
	},
}

