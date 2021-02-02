package action

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/GlobalFishingWatch/bq2psql-tool/internal/common"
	"github.com/GlobalFishingWatch/bq2psql-tool/types"
	"github.com/GlobalFishingWatch/bq2psql-tool/utils"
	"github.com/jackc/pgx/v4"
	"google.golang.org/api/iterator"
	"log"
	"reflect"
	"strings"
)

var bqClient *bigquery.Client
var psClient *pgx.Conn

func ImportBigQueryToPostgres(params types.ImportParams, postgresConfig types.PostgresConfig) {
	ctx := context.Background()

	bqClient = common.CreateBigQueryClient(ctx, params.ProjectId)
	psClient = common.CreatePostgresClient(ctx, postgresConfig)
	defer bqClient.Close()
	defer psClient.Close(ctx)

	ch := make(chan  map[string]bigquery.Value, 100)

	log.Println("Creating table to check if exists before the query")
	createTable(ctx, params.TableName, params.Schema)

	log.Println("→ Getting results from BigQuery")
	getResultsFromBigQuery(ctx, params.Query, ch)

	log.Println("→ Importing results to Postgres")
	importToPostgres(ctx, ch, params.TableName)
}

// BigQuery Functions
func getResultsFromBigQuery(ctx context.Context, queryRequested string, ch chan  map[string]bigquery.Value) {
	iterator := makeQuery(ctx, queryRequested)
	go parseResultsToJson(iterator, ch)
}

func makeQuery(ctx context.Context, queryRequested string) (*bigquery.RowIterator) {
	log.Println("→ BQ →→ Making query to get data from bigQuery")
	query := bqClient.Query(queryRequested)
	query.AllowLargeResults = true
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("→ BQ →→ Error counting rows: %v", err)
	}
	return it
}

func parseResultsToJson(it *bigquery.RowIterator, ch chan  map[string]bigquery.Value) {
	log.Println("→ BQ →→ Parsing results to JSON")

	for {
		var values []bigquery.Value
		err := it.Next(&values)

		if err == iterator.Done {
			close(ch)
			break
		}
		if err != nil {
			log.Fatalf("→ BQ →→ Error: %v", err)
		}

		var dataMapped = toMapJson(values, it.Schema)
		ch <- dataMapped
	}
}

func toMapJson (values []bigquery.Value, schema bigquery.Schema) map[string]bigquery.Value {
	var columnNames = getColumnNames(schema)
	var dataMapped = make(map[string]bigquery.Value)
	for i := 0; i < len(columnNames); i++ {
		if schema[i].Type == "RECORD" {
			if values[i] == nil {
				dataMapped[columnNames[i]] = values[i]
				continue
			}
			valuesNested := values[i].([]bigquery.Value)
			var valuesParsed = make([]map[string]bigquery.Value, len(valuesNested))
			var aux = make(map[string]bigquery.Value)
			for c := 0; c < len(valuesNested); c++ {
				if reflect.TypeOf(valuesNested[c]).Kind() != reflect.Interface &&
					reflect.TypeOf(valuesNested[c]).Kind() != reflect.Slice {
					var columnNamesNested = getColumnNames(schema[i].Schema)
					aux[columnNamesNested[c]] = valuesNested[c]
					dataMapped[columnNames[i]] = aux
				} else {
					valuesParsed[c] = toMapJsonNested(valuesNested[c].([]bigquery.Value), schema[i].Schema)
					dataMapped[columnNames[i]] = valuesParsed
				}
			}
		} else {
			dataMapped[columnNames[i]] = values[i]
		}
	}
	return dataMapped
}

func toMapJsonNested (value []bigquery.Value, schema bigquery.Schema) map[string]bigquery.Value {
	var columnNames = getColumnNames(schema)
	var dataMapped = make(map[string]bigquery.Value)
	for c := 0; c < len(columnNames); c++ {
		dataMapped[columnNames[c]] = value[c]
	}
	return dataMapped
}

func getColumnNames(schema bigquery.Schema) []string {
	var columnNames = make([]string, 0)
	for i := 0; i < len(schema); i++ {
		columnNames = append(columnNames, schema[i].Name)
	}
	return columnNames
}

// Postgres functions
func importToPostgres(ctx context.Context, ch chan map[string]bigquery.Value, tableName string) {
	log.Println("→ PG →→ Importing data to Postgres")
	for doc := range ch {
		query := getInsertQuery(tableName, doc)
		_, err := psClient.Exec(ctx, query)
		if err != nil {
			log.Println(doc)
			log.Println("=======")
			log.Println(query)
			log.Fatalf("Error inserting: %v", err)
		}
	}
	log.Println("→ PG →→ Import process finished")
}

func createTable(ctx context.Context, tableName string, schema string) {
	createTableCommand := fmt.Sprintf(
	`CREATE TABLE %s (
				%v
           );`, tableName, schema)
	log.Printf("→ PG →→ Creating table with command %s", createTableCommand)
	_, err := psClient.Exec(ctx, createTableCommand)
	if err != nil {
		log.Fatalf("→ PG →→ Error creating table: %v", err)
	}

	log.Printf("→ PG →→ Successfully created table with name %v", tableName)
}

func getInsertQuery(table string, doc map[string]bigquery.Value) string {
	var query = fmt.Sprintf("INSERT INTO %v ", table)
	var columns = "("
	var values = "VALUES ("
	for column, value := range doc {
		var myType = reflect.ValueOf(value).Kind()
		if myType == reflect.Slice {
			continue
		} else if myType == reflect.String || myType == reflect.Struct {
			valueString := strings.Replace(fmt.Sprintf("%v",value), "'", `''`, -1)
			values = values + fmt.Sprintf("'%v'", valueString) +","
		} else if myType == reflect.Int {
			values = values + fmt.Sprintf("%v",value) + ","
		} else {
			values = values + "null,"
		}
		columns = columns + utils.CamelCaseToSnakeCase(column) + ","
	}
	columns = TrimSuffix(columns, ",")
	columns = columns + ") "
	values = TrimSuffix(values, ",")
	values = values + ");"
	query = query + columns + values
	return query
}

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}