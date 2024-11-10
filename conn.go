package gorche

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type Conn struct {
	name   string
	colums []string
	cache  *redis.Client
	db     *pgx.Conn
}

func NewConn(ctx context.Context, name string, colums []string, ops *Options) (*Conn, error) {
	databaseClient, err := newDatabaseClient(ctx, ops)
	if err != nil {
		return nil, err
	}

	cacheClient := newCacheClient(ops)

	return &Conn{
		name:   name,
		colums: colums,
		cache:  cacheClient,
		db:     databaseClient,
	}, nil
}

func (c *Conn) Close(ctx context.Context) error {
	err := c.db.Close(ctx)
	if err != nil {
		return fmt.Errorf("error closing the database connector: %v", err)
	}
	err = c.cache.Close()
	if err != nil {
		return fmt.Errorf("error closing the cache connector: %v", err)
	}
	return nil
}
