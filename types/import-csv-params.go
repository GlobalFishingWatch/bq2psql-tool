package types

type ImportCsvParams struct {
	Query string
	ProjectId string
	TemporalDataset string
	TemporalBucket string
	DestinationTableName string
}