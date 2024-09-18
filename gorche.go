package gorche

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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
var PostgresDSN = func(ops *Options) string {
	return fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=disable TimeZone=%v", ops.DBAddr, ops.DBPort, ops.DBUser, ops.DBPwd, ops.DBName, ops.DBTmz)
}

func newDatabaseClient(ops *Options) (*gorm.DB, error) {
	return gorm.Open(postgres.Open(PostgresDSN(ops)), &gorm.Config{})
}

func newCacheClient(ops *Options) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", ops.CacheAddr, ops.CachePort),
	})
}

func SchemaToMap(sch interface{}) (map[string]string, error) {
	t := reflect.TypeOf(sch).Elem()
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("schema with type %v is unsupported. Only struct schemas are supported", t.Kind().String())
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

type Table struct {
	name   string
	schema interface{}
	cache  *redis.Client
	db     *gorm.DB
}

func NewTable(name string, schema interface{}, ops *Options) (*Table, error) {
	databaseClient, err := newDatabaseClient(ops)
	if err != nil {
		return nil, err
	}

	cacheClient := newCacheClient(ops)

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

// First return the first row found at the Table in the form of a map.
//
// The function will query the cache first to check if the value was previously
// indexed. If not, it will query the database and store its value in the cache
// before returning.
func (tb *Table) First(ctx context.Context) (map[string]string, error) {
	chq := tb.cache.HGetAll(ctx, fmt.Sprintf("%s:first", tb.name))
	if chq.Err() == nil && len(chq.Val()) > 0 {
		return chq.Val(), nil
	}

	q := tb.db.WithContext(ctx).Table(tb.name).First(tb.schema)
	if q.Error != nil {
		return nil, q.Error
	}
	if q.RowsAffected == 0 {
		return nil, errors.New("no rows affected")
	}

	raw, err := SchemaToMap(tb.schema)
	if err != nil {
		return nil, fmt.Errorf("could not convert schema to map: %v", err)
	}

	err = tb.cache.HSet(ctx, fmt.Sprintf("%s:first", tb.name), raw).Err()

	return raw, err
}
