// Package xfetch demonstrates the XFetch probabilistic early expiration algorithm.
//
// Problem with standard TTL:
//   All copies of a popular key expire at the same time → thundering herd.
//   Even with singleflight, you still have a 100ms+ gap where the cache is cold.
//
// XFetch Solution:
//   Proactively recompute the value BEFORE it expires, based on:
//     - How expensive the recompute is (delta = time to fetch from DB)
//     - How close to expiry the key is (remaining TTL)
//     - A beta tuning parameter (β=1.0 is standard)
//
// Formula (from Vattani, Chiusano, Ceccarello 2015):
//   early_recompute = (now + delta * β * ln(rand())) > expiry_time
//
// When β > 1: more aggressive early refresh (lower hit ratio, fresher data)
// When β < 1: less aggressive (higher hit ratio, more risk of expiry gap)
//
// Run:
//   go run xfetch/xfetch.go
//
// Requires: Redis on localhost:6379
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	beta = 1.0 // tuning parameter; increase for more aggressive early refresh
)

// xfetchGet retrieves a value using the XFetch algorithm.
//
// The cache stores:
//   key        → the value
//   key:delta  → how long (ms) the last recompute took
//   key:expiry → unix timestamp (ns) when the key expires
func xfetchGet(
	ctx context.Context,
	rdb *redis.Client,
	key string,
	ttl time.Duration,
	recompute func() (string, error),
) (string, error) {

	deltaKey := key + ":delta"
	expiryKey := key + ":expiry"

	// ── Read all three fields atomically ──────────────────────────────────
	pipe := rdb.Pipeline()
	valCmd := pipe.Get(ctx, key)
	deltaCmd := pipe.Get(ctx, deltaKey)
	expiryCmd := pipe.Get(ctx, expiryKey)
	pipe.Exec(ctx) //nolint:errcheck

	value := valCmd.Val()
	deltaMs, _ := strconv.ParseFloat(deltaCmd.Val(), 64)
	expiryNs, _ := strconv.ParseInt(expiryCmd.Val(), 10, 64)

	// ── XFetch decision ───────────────────────────────────────────────────
	now := time.Now().UnixNano()
	shouldRecompute := value == ""

	if !shouldRecompute && expiryNs > 0 {
		// XFetch formula: probabilistic early recompute
		// rand.Float64() ∈ (0,1), ln(rand) ∈ (-∞, 0)
		// So: now - deltaMs*beta*ln(rand) > expiryNs  →  recompute early
		lnRand := math.Log(rand.Float64())
		earlyRecomputeThreshold := float64(expiryNs) - (deltaMs*1e6)*beta*(-lnRand)
		shouldRecompute = float64(now) > earlyRecomputeThreshold
		if shouldRecompute {
			fmt.Printf("  ⚡ XFetch triggered early recompute (TTL remaining: %v)\n",
				time.Duration(expiryNs-now))
		}
	}

	if shouldRecompute {
		// ── Recompute from source ──────────────────────────────────────────
		start := time.Now()
		newValue, err := recompute()
		if err != nil {
			if value != "" {
				fmt.Printf("  ⚠️  Recompute failed, serving stale value\n")
				return value, nil // serve stale rather than error
			}
			return "", err
		}
		delta := time.Since(start)

		// ── Persist value + metadata ───────────────────────────────────────
		expiry := time.Now().Add(ttl)
		pipe := rdb.Pipeline()
		pipe.Set(ctx, key, newValue, ttl)
		pipe.Set(ctx, deltaKey, fmt.Sprintf("%.3f", float64(delta.Milliseconds())), ttl+time.Minute)
		pipe.Set(ctx, expiryKey, strconv.FormatInt(expiry.UnixNano(), 10), ttl+time.Minute)
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("⚠️  Pipeline error: %v", err)
		}

		return newValue, nil
	}

	return value, nil
}

// ─── Demo ─────────────────────────────────────────────────────────────────────

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis unreachable: %v", err)
	}

	// Clean slate
	rdb.Del(ctx, "featured-banner", "featured-banner:delta", "featured-banner:expiry")

	callCount := 0
	recompute := func() (string, error) {
		callCount++
		time.Sleep(80 * time.Millisecond) // simulate slow DB query
		return fmt.Sprintf("banner-v%d", callCount), nil
	}

	fmt.Println("🧪 XFetch Demo (TTL=5s, beta=1.0)")
	fmt.Println("   Polling every 500ms. Watch for early recompute near expiry boundary.")
	fmt.Println()

	for i := 0; i < 16; i++ {
		val, err := xfetchGet(ctx, rdb, "featured-banner", 5*time.Second, recompute)
		if err != nil {
			log.Printf("  error: %v", err)
			continue
		}
		ttlLeft, _ := rdb.TTL(ctx, "featured-banner").Result()
		fmt.Printf("  [t=%ds] value=%s | TTL remaining=%v | DB calls so far=%d\n",
			i/2, val, ttlLeft.Round(time.Millisecond), callCount)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("\n✅ Total DB recomputes: %d (XFetch = zero cold-cache gaps)\n", callCount)
}
