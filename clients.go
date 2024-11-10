package gorche

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// Options represents the database and cache connection attributes.
type Options struct {
	DBAddr    string
	DBPort    int
	DBUser    string
	DBPwd     string
	DBName    string
	DBTmz     string
	CacheAddr string
	CachePort int
	CacheDB   int
}

// PostgresDSN returns the DSN to open the Postgres database connection.
// urlExample := "postgres://username:password@localhost:5432/database_name"
var PostgresDSN = func(ops *Options) string {
	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v", ops.DBUser, ops.DBPwd, ops.DBAddr, ops.DBPort, ops.DBName)
}

func newDatabaseClient(ctx context.Context, ops *Options) (*pgx.Conn, error) {
	return pgx.Connect(ctx, PostgresDSN(ops))
}

func newCacheClient(ops *Options) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", ops.CacheAddr, ops.CachePort),
	})
}
