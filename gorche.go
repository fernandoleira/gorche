package gorche

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

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

func ModelToMap(sch interface{}) (map[string]string, error) {
	t := reflect.TypeOf(sch).Elem()
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model with type %v is unsupported. Only struct models are supported", t.Kind().String())
	}

	m := make(map[string]string)
	v := reflect.ValueOf(sch).Elem()
	for i := 0; i < v.NumField(); i++ {
		key := t.Field(i)
		val := v.Field(i)

		switch val.Kind() {
		case reflect.String:
			m[key.Name] = val.String()
		case reflect.Bool:
			m[key.Name] = strconv.FormatBool(v.Bool())
		case reflect.Int:
			m[key.Name] = strconv.FormatInt(val.Int(), 10)
		case reflect.Uint:
			m[key.Name] = strconv.FormatUint(val.Uint(), 10)
		default:
			return nil, fmt.Errorf("unexpected type %v received", v.Kind().String())
		}
	}

	return m, nil
}

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

// First return the first row found at the Table in the form of a map.
//
// The function will query the cache first to check if the value was previously
// indexed. If not, it will query the database and store its value in the cache
// before returning.
func (c *Conn) First(ctx context.Context) (map[string]string, error) {
	cacheq := c.cache.HGetAll(ctx, fmt.Sprintf("%s:first", c.name))
	if cacheq.Err() == nil && len(cacheq.Val()) > 0 {
		return cacheq.Val(), nil
	}

	row := c.db.QueryRow(ctx, fmt.Sprintf("select %s from %s limit 1", strings.Join(c.colums, ", "), c.name))
	raw := make(map[string]string)
	for _, col := range c.colums {
		sc := []any{"", "", "", 0}
		err := row.Scan(sc...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		fmt.Println(col)
		//raw[col] = sc
	}

	// raw, err := ModelToMap(c.model)
	// if err != nil {
	// 	return nil, fmt.Errorf("could not convert schema to map: %v", err)
	// }

	err := c.cache.HSet(ctx, fmt.Sprintf("%s:first", c.name), raw).Err()

	return raw, err
}

// Query runs a select call in the database.
func (c *Conn) Query() (map[string]string, error) {
	return nil, fmt.Errorf("unimplemented")
}

// Insert adds a new record to the database and saves it in the cache.
func (c *Conn) Insert(ctx context.Context) error {
	return fmt.Errorf("unimplemented")
}

// Update updates an existing record in the database.
func (c *Conn) Update(ctx context.Context) error {
	return fmt.Errorf("unimplemented")
}

// Delete removes a record from the database and the cache if it exists.
func (c *Conn) Delete(ctx context.Context) error {
	return fmt.Errorf("unimplemented")
}
