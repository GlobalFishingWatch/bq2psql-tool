package action

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/GlobalFishingWatch/bq2psql-tool/internal/common"
	"github.com/GlobalFishingWatch/bq2psql-tool/types"
	"github.com/satori/go.uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
	"io/ioutil"
	"log"
	"strings"
	"time"
)


var bigQueryClient *bigquery.Client

func ImportCsvBigQueryToPostgres(params types.ImportCsvParams, cloudSqlConfig types.CloudSqlConfig) {
	ctx := context.Background()

	if cloudSqlConfig.Database == "" {
		cloudSqlConfig.Database = "postgres"
	}

	bigQueryClient = common.CreateBigQueryClient(ctx, params.ProjectId)
	defer bigQueryClient.Close()

	// Create a temporal table
	log.Println("→ Creating temporal table from query result")
	temporalTableName := createTemporalTable(ctx, params.TemporalDataset, params.Query)

	// Export events to csv
	log.Println("→ Exporting results from temporal table to gcs")
	exportTemporalTableToCsv(ctx, params.ProjectId, params.TemporalDataset, temporalTableName, params.TemporalBucket)

	// Delete intermediate table
	log.Println("→ Deleting temporal table")
	deleteTemporalTable(ctx, params.ProjectId, params.TemporalDataset, temporalTableName)

	// List objects, import data and delete object
	log.Println("→ Listing objects and importing to Postgres")
	listObjects(ctx, params.ProjectId, params.TemporalBucket, temporalTableName, cloudSqlConfig)
}

func listObjects(ctx context.Context, projectId string, bucketName string, temporalTable string, cloudSqlConfig types.CloudSqlConfig) {
	client, err := storage.NewClient(ctx)
	defer client.Close()

	if err != nil {
		log.Fatal("→ GCS →→ Error creating GCS client")
	}
	bkt := client.Bucket(bucketName)
	prefix := fmt.Sprintf(`bq2psql-tool/%s/`, temporalTable)
	query := &storage.Query{
		Prefix: prefix,
	}
	var names []string
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal("→ GCS →→ Error listing objects ", err)
		}
		names = append(names, attrs.Name)
	}

	for i := 0; i < len(names); i++ {
		uri := fmt.Sprintf(`gs://%s/%s`, bucketName, names[i])
		importFileToCloudSQL(ctx, projectId, cloudSqlConfig, uri)
		obj := bkt.Object(names[i])
		if err := obj.Delete(ctx); err != nil {
			log.Fatalf("Cannot delete object with name %s", names[i])
		}
	}
}

func createTemporalTable(ctx context.Context, datasetId string, queryRequested string) string {
	log.Println("→ BQ →→ Making query to get data from bigQuery")
	query := bigQueryClient.Query(queryRequested)
	query.AllowLargeResults = true
	currentTime := time.Now()
	temporalTableName := fmt.Sprintf("%s_%s", uuid.NewV4(), currentTime.Format("2006_01_02_15_04"))
	log.Printf("→ BQ →→ Temporal table name: %s", temporalTableName)
	dstTable := bigQueryClient.Dataset(datasetId).Table(string(temporalTableName))
	err := dstTable.Create(ctx, &bigquery.TableMetadata{ExpirationTime: time.Now().Add(5 * time.Hour)})
	if err != nil {
		log.Fatal("→ BQ →→ Error creating temporary table", err)
	}
	query.QueryConfig.Dst = dstTable
	log.Println("→ BQ →→ Exporting query to intermediate table")

	job, err := query.Run(context.Background())
	checkBigQueryJob(job, err)

	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempTable := config.(*bigquery.QueryConfig).Dst
	log.Println("→ BQ →→ Temp table", tempTable.TableID)
	return tempTable.TableID
}

func exportTemporalTableToCsv(ctx context.Context, projectId string, dataset string, temporalTable string, temporalBucket string) {
	temporalDataset := bigQueryClient.DatasetInProject(projectId, dataset)
	table := temporalDataset.Table(temporalTable)
	uri := fmt.Sprintf(`gs://%s/bq2psql-tool/%s/*.csv.gz`, temporalBucket, temporalTable)
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.Compression = "GZIP"
	gcsRef.DestinationFormat = "CSV"
	extractor := table.ExtractorTo(gcsRef)
	extractor.DisableHeader = true
	job, err := extractor.Run(ctx)
	checkBigQueryJob(job, err)
}

func checkBigQueryJob(job *bigquery.Job, err error) {
	if err != nil {
		log.Fatal("→ BQ →→ Error creating job", err)
	}
	for {
		log.Println("→ BQ →→ Checking status of job")
		status, err := job.Status(context.Background())
		if err != nil {
			log.Fatal("→ BQ →→ Error obtaining status", err)
		}
		log.Println("Done:", status.Done())
		if status.Done() {
			if len(status.Errors) > 0 {
				log.Fatal("Error", status.Errors)
			}
			break
		}
		time.Sleep(5 * time.Second)
	}
}

func deleteTemporalTable(ctx context.Context, projectId string, dataset string, temporalTable string) {
	temporalDataset := bigQueryClient.DatasetInProject(projectId, dataset)
	table := temporalDataset.Table(temporalTable)
	if err := table.Delete(ctx); err != nil {
		log.Fatalf("→ BQ →→Error deleteing temporal table %s", temporalTable)
	}
}

func importFileToCloudSQL(ctx context.Context, projectId string, cloudSqlConfig types.CloudSqlConfig, uri string) {
	columns := strings.Split(cloudSqlConfig.Columns, ",")
	sqlAdminService, err := sqladmin.NewService(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Importing data to database: %s", cloudSqlConfig.Database)
	importContext := &sqladmin.InstancesImportRequest{
		ImportContext: &sqladmin.ImportContext{
			Database: cloudSqlConfig.Database,
			ImportUser: "postgres",
			FileType: "CSV",
			Uri:      uri,
			CsvImportOptions: &sqladmin.ImportContextCsvImportOptions{
				Table:   cloudSqlConfig.Table,
				Columns: columns,
			},
		},
	}
	var operation *sqladmin.Operation
	for {
		log.Printf("→ PSSQL →→ Importing file (%s) to cloud sql (%s) and columns %s", uri, cloudSqlConfig.Table, strings.Join(columns, ","))
		log.Printf("→ PSSQL →→ Project: %s, Instance: %s", projectId, cloudSqlConfig.Instance)
		call := sqlAdminService.Instances.Import(projectId, cloudSqlConfig.Instance, importContext)
		operation, err = call.Do()
		if err != nil {
			newErr, ok := err.(*googleapi.Error)
			if !ok {
				log.Fatal("→ PSQL →→Error ingesting ", err, newErr)
			} else if newErr.Code == 409 || newErr.Code >= 500 {
				log.Printf("→ PSQL →→ Retrying file %s in 2 min", cloudSqlConfig.Table, newErr.Body)
				time.Sleep(2 * time.Minute)
				continue
			} else {
				log.Fatal("→ PSQL →→ Error google ingesting ", err, newErr)
			}
		}
		break
	}
	for {
		client, err := google.DefaultClient(oauth2.NoContext, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			log.Fatal(err)
		}
		resp, err := client.Get(operation.SelfLink)
		if err != nil {
			log.Fatal("→ PSQL →→ Error obtaining status of import", err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		var respJson map[string]interface{}
		err = json.Unmarshal(body, &respJson)
		if err != nil {
			log.Fatal("→ PSQL →→ Error unmarshal response", err)
		}
		log.Printf("→ PSQL  →→Status: %s", respJson["status"])
		if respJson["status"] == "PENDING" || respJson["status"] == "RUNNING" {
			time.Sleep(5 * time.Second)
			continue
		} else if respJson["status"] == "DONE" {
			if respJson["error"] != nil {
				if strings.Contains(fmt.Sprintf("%s", respJson["error"]), "cleanup after import is completed") {
					log.Println("→ PSQL →→ Cleenup error")
					break
				}
				log.Fatal("→ PSQL →→ Error importing", respJson["error"])
				panic(respJson["error"])
			} else {
				break
			}
		}
	}
}