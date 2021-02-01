package common

import (
	"cloud.google.com/go/bigquery"
	"context"
	"log"
)

func CreateBigQueryClient(ctx context.Context, projectId string) *bigquery.Client {
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("→ BQ →→ bigquery.NewClient: %v", err)
	}
	return client
}