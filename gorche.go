package gorche

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	CACHE_ADDR = "localhost"
	CACHE_PORT = 6379
	CACHE_DB   = 0
)

func Helper() {
	fmt.Println("This is a helper")
}

// func DBConn() (*gorm.DB, error) {
// 	return gorm.Open(postgres.Open(), &gorm.Config{})
// }

func CacheConn(ctx context.Context) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", CACHE_ADDR, CACHE_PORT),
	})
}
