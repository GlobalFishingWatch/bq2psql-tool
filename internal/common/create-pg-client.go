package common

import (
	"context"
	"fmt"
	"github.com/GlobalFishingWatch/bq2psql-tool/types"
	"github.com/jackc/pgx/v4"
	"os"
)

func CreatePostgresClient(ctx context.Context, postgresConfig types.PostgresConfig) *pgx.Conn {
	uri := "postgresql://" + postgresConfig.Addr + "/" + postgresConfig.Database + "?user=" + postgresConfig.User + "&password=" + postgresConfig.Password
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	return conn
}