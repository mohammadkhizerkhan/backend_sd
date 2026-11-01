// Package cache_aside demonstrates the Cache-Aside (Lazy Loading) pattern.
//
// Flow:
//   1. Read from cache → if HIT return immediately
//   2. On MISS → read from DB → write to cache with TTL → return
//
// Run:
//   cd cache/go-clients
//   go mod tidy
//   go run cache_aside/cache_aside.go
//
// Requires: Redis on localhost:6379 (docker compose up redis-master)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─── Types ───────────────────────────────────────────────────────────────────

type Product struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Price float64 `json:"price"`
}

// ─── Fake DB ─────────────────────────────────────────────────────────────────

// simulateDB pretends to run a slow SQL query (~50ms).
func simulateDB(ctx context.Context, productID int) (*Product, error) {
	time.Sleep(50 * time.Millisecond) // simulate network + query latency
	if productID > 1000 {
		return nil, nil // product does not exist
	}
	return &Product{ID: productID, Name: fmt.Sprintf("Product-%d", productID), Price: float64(productID) * 9.99}, nil
}

// ─── Cache-Aside Logic ───────────────────────────────────────────────────────

const cacheTTL = 5 * time.Minute

func getProduct(ctx context.Context, rdb *redis.Client, productID int) (*Product, string, error) {
	cacheKey := fmt.Sprintf("product:%d", productID)

	// ── Step 1: Try cache ──────────────────────────────────────────────────
	val, err := rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		// CACHE HIT
		var p Product
		if jsonErr := json.Unmarshal([]byte(val), &p); jsonErr != nil {
			return nil, "HIT(corrupt)", jsonErr
		}
		return &p, "HIT", nil
	}
	if err != redis.Nil {
		// Redis itself is down — fail open (go to DB)
		log.Printf("⚠️  Redis error: %v — falling back to DB", err)
	}

	// ── Step 2: Cache MISS → query DB ─────────────────────────────────────
	product, dbErr := simulateDB(ctx, productID)
	if dbErr != nil {
		return nil, "MISS", dbErr
	}

	// ── Step 3: Null Object caching (prevents cache penetration) ──────────
	if product == nil {
		// Cache a sentinel value so repeated queries for non-existent IDs
		// don't hammer the DB. Short TTL to allow eventual existence.
		rdb.Set(ctx, cacheKey, `"__null__"`, 30*time.Second)
		return nil, "MISS(null)", nil
	}

	// ── Step 4: Populate cache ─────────────────────────────────────────────
	data, _ := json.Marshal(product)
	rdb.Set(ctx, cacheKey, data, cacheTTL)

	return product, "MISS", nil
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Cannot connect to Redis: %v", err)
	}
	fmt.Println("✅ Connected to Redis")

	ids := []int{42, 42, 99, 42, 9999} // 9999 → non-existent

	for _, id := range ids {
		start := time.Now()
		product, status, err := getProduct(ctx, rdb, id)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("  ❌ product:%d ERROR: %v\n", id, err)
			continue
		}
		if product == nil {
			fmt.Printf("  🔍 product:%d [%s] NOT FOUND — took %v\n", id, status, elapsed)
			continue
		}
		fmt.Printf("  📦 product:%d [%s] → %s $%.2f — took %v\n", id, status, product.Name, product.Price, elapsed)
	}

	// Expected output:
	//   product:42  [MISS]  → ~50ms (DB hit)
	//   product:42  [HIT]   → ~0.1ms (cache hit)
	//   product:99  [MISS]  → ~50ms
	//   product:42  [HIT]   → ~0.1ms
	//   product:9999 [MISS(null)] → ~50ms
}
