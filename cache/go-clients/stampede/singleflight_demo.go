// Package stampede demonstrates the Thundering Herd / Cache Stampede problem
// and solves it with golang.org/x/sync/singleflight.
//
// Problem:
//   When a popular cache key expires, N concurrent goroutines all get a MISS
//   simultaneously and all hammer the DB. For N=1000 goroutines, that's 1000
//   DB queries for the same data.
//
// Solution:
//   singleflight.Group collapses all concurrent calls for the same key into
//   one real DB query. All other callers wait and share the result.
//
// Run:
//   go run stampede/singleflight_demo.go
//
// Requires: Redis on localhost:6379
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var (
	rdb    *redis.Client
	sfg    singleflight.Group
	dbHits atomic.Int64 // count how many times DB is actually queried
)

// ─── Fake DB ─────────────────────────────────────────────────────────────────

func fetchFromDB(key string) (string, error) {
	dbHits.Add(1)
	time.Sleep(100 * time.Millisecond) // simulate slow query
	return fmt.Sprintf("db-value-for-%s", key), nil
}

// ─── WITH singleflight ───────────────────────────────────────────────────────

func getWithSingleflight(ctx context.Context, key string) (string, error) {
	// Try cache first
	val, err := rdb.Get(ctx, key).Result()
	if err == nil {
		return val, nil
	}

	// On MISS: use singleflight to deduplicate concurrent fetches
	// The string key groups concurrent calls — only one goroutine calls fetchFromDB.
	result, err, shared := sfg.Do(key, func() (interface{}, error) {
		data, dbErr := fetchFromDB(key)
		if dbErr != nil {
			return nil, dbErr
		}
		// Populate cache inside the singleflight closure so only one writer exists
		rdb.Set(ctx, key, data, 30*time.Second)
		return data, nil
	})

	if err != nil {
		return "", err
	}

	_ = shared // shared=true means this caller got a coalesced result
	return result.(string), nil
}

// ─── WITHOUT singleflight (for comparison) ───────────────────────────────────

func getWithoutSingleflight(ctx context.Context, key string) (string, error) {
	val, err := rdb.Get(ctx, key).Result()
	if err == nil {
		return val, nil
	}
	// Every goroutine hits DB on MISS — stampede!
	data, dbErr := fetchFromDB(key)
	if dbErr != nil {
		return "", dbErr
	}
	rdb.Set(ctx, key, data, 30*time.Second)
	return data, nil
}

// ─── Demo harness ────────────────────────────────────────────────────────────

func runDemo(ctx context.Context, label string, fn func(context.Context, string) (string, error)) {
	const goroutines = 1000
	const key = "hot-product:featured"

	// Ensure cache is empty (simulates expiry)
	rdb.Del(ctx, key)
	dbHits.Store(0)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := fn(ctx, key); err != nil {
				log.Printf("error: %v", err)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n[%s]\n", label)
	fmt.Printf("  Goroutines   : %d\n", goroutines)
	fmt.Printf("  DB hits      : %d  ← this is the key metric\n", dbHits.Load())
	fmt.Printf("  Total time   : %v\n", elapsed)
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	ctx := context.Background()

	rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis unreachable: %v", err)
	}

	fmt.Println("🔥 Cache Stampede Demo")
	fmt.Println("   Simulating 1000 goroutines racing on a fresh cache MISS...")

	runDemo(ctx, "WITHOUT singleflight (stampede)", getWithoutSingleflight)
	// Expected: DB hits ≈ 1000

	runDemo(ctx, "WITH singleflight (collapsed)", getWithSingleflight)
	// Expected: DB hits = 1

	fmt.Println("\n✅ singleflight reduces 1000 DB hits to 1.")
}
