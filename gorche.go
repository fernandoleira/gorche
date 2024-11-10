package gorche

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// First return the first row found at the Table in the form of a map.
//
// The function will query the cache first to check if the value was previously
// indexed. If not, it will query the database and store its value in the cache
// before returning.
func (c *Conn) First(ctx context.Context) (map[string]any, error) {
	cacheq := c.cache.HGetAll(ctx, fmt.Sprintf("%s:first", c.name))
	if cacheq.Err() == nil && len(cacheq.Val()) > 0 {
		dst := make(map[string]any)
		for key, val := range cacheq.Val() {
			dst[key] = val
		}
		return dst, nil
	}

	rows, err := c.db.Query(ctx, fmt.Sprintf("select %s from %s limit 1", strings.Join(c.colums, ", "), c.name))
	if err != nil {
		return nil, fmt.Errorf("error querying the db: %v", err)
	}
	defer rows.Close()

	dst, err := pgx.CollectOneRow(rows, pgx.RowToMap)
	if err != nil {
		return nil, fmt.Errorf("error scanning the resulting row: %v", err)
	}

	err = c.cache.HSet(ctx, fmt.Sprintf("%s:first", c.name), dst).Err()
	return dst, err
}

// Query runs a select call in the database.
func (c *Conn) Query(ctx context.Context, dst interface{}) error {
	return fmt.Errorf("unimplemented")
}

// Insert adds a new record to the database and saves it in the cache.
func (c *Conn) Insert(ctx context.Context, src interface{}) error {
	return fmt.Errorf("unimplemented")
}

// Update updates an existing record in the database.
func (c *Conn) Update(ctx context.Context, src interface{}) error {
	return fmt.Errorf("unimplemented")
}

// Delete removes a record from the database and the cache if it exists.
func (c *Conn) Delete(ctx context.Context, src interface{}) error {
	return fmt.Errorf("unimplemented")
}
