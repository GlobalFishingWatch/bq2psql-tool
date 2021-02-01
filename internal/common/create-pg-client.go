package common

import (
	"context"
	"fmt"
	"github.com/GlobalFishingWatch/bq2psql-tool/types"
	"github.com/jackc/pgx/v4"
	"os"
)

func CreatePostgresClient(ctx context.Context, postgresConfig types.PostgresConfig) *pgx.Conn {
	conn, err := pgx.Connect(ctx, "postgresql://localhost/postgres?user=postgres&password=a1234567")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	// defer conn.Close(ctx)
	return conn
}