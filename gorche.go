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

type Person struct {
	PersonID  uint
	FirstName string
	LastName  string
	Nickname  string
	Age       uint
}

type Connector struct {
	cacheClient    *redis.Client
	databaseClient *gorm.DB
}

func NewConnector() (*Connector, error) {
	databaseClient, err := newDatabaseClient()
	if err != nil {
		return nil, err
	}

	cacheClient := newCacheClient()

	return &Connector{
		cacheClient:    cacheClient,
		databaseClient: databaseClient,
	}, nil
}

func (c *Connector) Close() {
	c.cacheClient.Close()
}

func (c *Connector) First(ctx context.Context) (string, error) {
	person := Person{}
	q := c.databaseClient.WithContext(ctx).First(&person)
	if q.Error != nil {
		return "", q.Error
	}
	if q.RowsAffected == 0 {
		return "", errors.New("no rows affected")
	}
	return fmt.Sprintf("%s %s %d", person.FirstName, person.LastName, person.Age), nil
}
