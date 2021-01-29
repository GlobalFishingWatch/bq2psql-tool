package cmd

import (
	"github.com/GlobalFishingWatch/bq2psql-tool/internal/action"
	"github.com/spf13/cobra"
	"log"
)

func init() {
	rootCmd.AddCommand(importCmd)
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from BigQuery to Postgres",
	Long:  `Import data from BigQuery to Postgres
Format:
	bq2psql import 
Example:
	bq2psql import`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing Import command")
		action.ImportBigQueryToPostgres()
	},
}

