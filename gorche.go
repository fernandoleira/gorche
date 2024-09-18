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
	name  string
	model interface{}
	cache *redis.Client
	db    *gorm.DB
}

func NewConn(name string, mdl interface{}, ops *Options) (*Conn, error) {
	if reflect.TypeOf(mdl).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("cannot use a non pointer value as the model of the table")
	}

	databaseClient, err := newDatabaseClient(ops)
	if err != nil {
		return nil, err
	}

	cacheClient := newCacheClient(ops)

	return &Conn{
		name:  name,
		model: mdl,
		cache: cacheClient,
		db:    databaseClient,
	}, nil
}

func (c *Conn) Close() error {
	err := c.cache.Close()
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
func (c *Conn) First(ctx context.Context) (map[string]string, error) {
	cacheq := c.cache.HGetAll(ctx, fmt.Sprintf("%s:first", c.name))
	if cacheq.Err() == nil && len(cacheq.Val()) > 0 {
		return cacheq.Val(), nil
	}

	dbq := c.db.WithContext(ctx).Table(c.name).First(c.model)
	if dbq.Error != nil {
		return nil, dbq.Error
	}
	if dbq.RowsAffected == 0 {
		return nil, errors.New("no rows affected")
	}

	raw, err := ModelToMap(c.model)
	if err != nil {
		return nil, fmt.Errorf("could not convert schema to map: %v", err)
	}

	err = c.cache.HSet(ctx, fmt.Sprintf("%s:first", c.name), raw).Err()

	return raw, err
}
