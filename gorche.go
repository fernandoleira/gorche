package gorche

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	DATABASE_ADDR = "localhost"
	DATABASE_PORT = 5432
	DATABASE_USER = "admin"
	DATABASE_PWD  = "postgres"
	DATABASE_DB   = "gorche"
	DATABASE_TMZ  = "US/Eastern"
	CACHE_ADDR    = "localhost"
	CACHE_PORT    = 6379
	CACHE_DB      = 0
)

// PostgresDSN returns the DSN to open the Postgres database connection.
var PostgresDSN = func() string {
	return fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=disable TimeZone=%v", DATABASE_ADDR, DATABASE_PORT, DATABASE_USER, DATABASE_PWD, DATABASE_DB, DATABASE_TMZ)
}

func newDatabaseClient() (*gorm.DB, error) {
	return gorm.Open(postgres.Open(PostgresDSN()), &gorm.Config{})
}

func newCacheClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", CACHE_ADDR, CACHE_PORT),
	})
}

type Table struct {
	name   string
	schema interface{}
	cache  *redis.Client
	db     *gorm.DB
}

func NewTable(name string, schema interface{}) (*Table, error) {
	databaseClient, err := newDatabaseClient()
	if err != nil {
		return nil, err
	}

	cacheClient := newCacheClient()

	return &Table{
		name:   name,
		schema: schema,
		cache:  cacheClient,
		db:     databaseClient,
	}, nil
}

func (tb *Table) Close() error {
	err := tb.cache.Close()
	if err != nil {
		return fmt.Errorf("error closing the gorche connector: %v", err)
	}
	return nil
}

func (tb *Table) First(ctx context.Context) (map[string]string, error) {
	chq := tb.cache.HGetAll(ctx, "people:first")
	if chq.Err() == nil {
		return chq.Val(), nil
	}

	dbq := tb.db.WithContext(ctx).Table(tb.name).First(tb.schema)
	if dbq.Error != nil {
		return nil, dbq.Error
	}
	if dbq.RowsAffected == 0 {
		return nil, errors.New("no rows affected")
	}

	err := tb.cache.HSet(ctx, "people:first", map[string]any{}).Err()

	return map[string]string{}, err
}
