// Package dist_lock demonstrates a production-grade Redis distributed lock.
//
// Anti-patterns covered:
//   ❌ SETNX + EXPIRE (two commands, not atomic — deadlock if process dies between them)
//   ❌ DEL without checking owner (another process could release your lock)
//
// Correct approach:
//   ✅ SET key uuid NX PX milliseconds   (atomic acquire)
//   ✅ Lua script for compare-and-delete  (atomic release)
//   ✅ Watchdog goroutine for TTL renewal (prevents expiry during long processing)
//
// Run:
//   go run dist_lock/dist_lock.go
//
// Requires: Redis on localhost:6379
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─── Lua Scripts ─────────────────────────────────────────────────────────────

// releaseScript atomically checks that the caller still owns the lock before deleting.
// Without this check, a slow process could release a lock that was re-acquired by
// another process after the TTL expired.
var releaseScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

// renewScript atomically extends the TTL only if the caller still owns the lock.
var renewScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

// ─── Lock ────────────────────────────────────────────────────────────────────

type DistributedLock struct {
	rdb        *redis.Client
	key        string
	ownerID    string        // UUID unique to this lock acquisition
	ttl        time.Duration // initial TTL for the lock key
	renewEvery time.Duration // how often the watchdog renews
	cancel     context.CancelFunc
}

var ErrLockNotAcquired = errors.New("lock not acquired: already held by another process")

// Acquire tries to acquire the lock. Returns ErrLockNotAcquired if someone else holds it.
func Acquire(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (*DistributedLock, error) {
	ownerID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63())

	// SET key ownerID NX PX <ms>  — atomic, single command
	ok, err := rdb.SetNX(ctx, key, ownerID, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("redis error during acquire: %w", err)
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}

	lock := &DistributedLock{
		rdb:        rdb,
		key:        key,
		ownerID:    ownerID,
		ttl:        ttl,
		renewEvery: ttl / 3, // renew at 1/3 of TTL to stay well ahead of expiry
	}

	// Start watchdog goroutine — keeps the lock alive during long processing
	watchCtx, cancel := context.WithCancel(context.Background())
	lock.cancel = cancel
	go lock.watchdog(watchCtx)

	return lock, nil
}

// watchdog periodically renews the lock TTL so it doesn't expire mid-processing.
func (l *DistributedLock) watchdog(ctx context.Context) {
	ticker := time.NewTicker(l.renewEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ttlMs := l.ttl.Milliseconds()
			result, err := renewScript.Run(ctx, l.rdb, []string{l.key}, l.ownerID, ttlMs).Int()
			if err != nil || result == 0 {
				// Lock was released externally (e.g., TTL expired or stolen in Redlock scenario)
				log.Printf("⚠️  Watchdog: lock %q was lost! result=%d err=%v", l.key, result, err)
				return
			}
			log.Printf("🔄 Watchdog: renewed %q by %v", l.key, l.ttl)
		}
	}
}

// Release atomically releases the lock if we still own it.
func (l *DistributedLock) Release(ctx context.Context) error {
	l.cancel() // stop watchdog

	result, err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.ownerID).Int()
	if err != nil {
		return fmt.Errorf("redis error during release: %w", err)
	}
	if result == 0 {
		return fmt.Errorf("lock %q was already released or expired (ownerID mismatch)", l.key)
	}
	return nil
}

// ─── Demo ────────────────────────────────────────────────────────────────────

func criticalSection(ctx context.Context, rdb *redis.Client, workerID int) {
	lockKey := "order:checkout:user-42"

	lock, err := Acquire(ctx, rdb, lockKey, 5*time.Second)
	if err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			fmt.Printf("  👷 Worker %d: could not acquire lock (already held)\n", workerID)
			return
		}
		log.Printf("  worker %d: unexpected error: %v", workerID, err)
		return
	}

	fmt.Printf("  🔐 Worker %d: acquired lock (owner=%s...)\n", workerID, lock.ownerID[:12])

	// Simulate work that takes longer than renewEvery (watchdog kicks in)
	time.Sleep(4 * time.Second)

	if err := lock.Release(ctx); err != nil {
		log.Printf("  worker %d: release error: %v", workerID, err)
		return
	}
	fmt.Printf("  🔓 Worker %d: released lock\n", workerID)
}

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis unreachable: %v", err)
	}

	// Clean up any leftover lock from previous run
	rdb.Del(ctx, "order:checkout:user-42")

	fmt.Println("🔐 Distributed Lock Demo")
	fmt.Println("   Two workers race to acquire the same lock.")
	fmt.Println("   Only one should succeed; watchdog keeps it alive for 4s of work.")
	fmt.Println()

	// Launch two concurrent workers — only one should acquire the lock
	go criticalSection(ctx, rdb, 1)
	time.Sleep(100 * time.Millisecond) // small delay so worker 1 acquires first
	go criticalSection(ctx, rdb, 2)

	time.Sleep(6 * time.Second) // wait for both workers to finish
	fmt.Println("\n✅ Demo complete.")
}
