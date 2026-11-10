# Redis HyperLogLog, Bitmaps & Geospatial Reference Manual

This manual follows the natural architectural progression: **Problem Statement $\rightarrow$ HyperLogLog Internals ($12\text{ KB}$ Cardinality Estimation) $\rightarrow$ Bitmaps & Bitfields (Cohort Retention & Packed Counters) $\rightarrow$ Geospatial ZSET Internals (52-Bit Geohash) $\rightarrow$ Cluster Constraints (`CROSSSLOT`) $\rightarrow$ Production Golang Implementations**.

---

## 1. Problem Statement

1. **The Exact Cardinality Memory Explosion (`COUNT(DISTINCT)`)**: Counting unique daily visitors, unique ad impressions, or unique search queries at scale (e.g., 100 million unique UUIDs/day) using a Redis `SET` (`SADD` + `SCARD`) consumes **3.2 GB to 6.4 GB of RAM per metric**. **HyperLogLog (`PFADD` / `PFCOUNT` / `PFMERGE`)** solves this by estimating cardinality up to $2^{64}$ unique items using **at most 12 KB of memory** with a standard error of **$0.81\%$**.
2. **Fast Multi-Day User Cohort Intersections**: Answering questions like *"Which users logged in on Monday AND Tuesday AND Wednesday?"* across 50 million users requires expensive multi-table joins in SQL. **Redis Bitmaps (`SETBIT` / `BITCOUNT` / `BITOP`)** represent each user as a single bit ($50\text{M users} = 6.25\text{ MB}$) and execute bitwise `AND`/`OR`/`XOR` across tens of millions of users in **under 2 milliseconds**.
3. **Real-Time 2D Proximity Search**: Finding all active delivery drivers within a 3 km radius of a customer requires spatial indexing. **Redis Geospatial (`GEOADD` / `GEOSEARCH`)** encodes 2D coordinates into a **52-bit Z-order Geohash** stored inside a standard Sorted Set (`ZSET`), enabling sub-millisecond radius and bounding-box queries.

---

## 2. HyperLogLog Architecture & Internal Mechanics

### 1. How 16,384 6-Bit Registers Count Billions of Items in 12 KB
When you execute `PFADD uv:2026-11-08 "visitor_uuid_99"`, Redis never stores the string `"visitor_uuid_99"`. Instead:
1. Redis hashes the input using **64-bit MurmurHash64A**.
2. **Bucket Selection (Lower 14 bits):** The first 14 bits ($2^{14} = 16,384$) select one register index $j \in [0, 16383]$.
3. **Leading-Zero Rank $\rho$ (Upper 50 bits):** Redis counts the number of consecutive leading `0` bits plus 1 (the position of the leftmost `1` bit, max value 51, which fits in a **6-bit register**).
4. **Register Update:** Redis sets $\text{Register}[j] = \max(\text{Register}[j], \rho)$.
5. **Harmonic Mean (`PFCOUNT`):** Redis calculates the bias-corrected harmonic mean across all 16,384 registers:
   $$E = \alpha_m \cdot m^2 \cdot \left( \sum_{j=0}^{16383} 2^{-\text{Register}[j]} \right)^{-1}, \quad \sigma = \frac{1.04}{\sqrt{16384}} \approx \mathbf{0.81\%}$$

```
16-Byte Header ("HYLL" + Encoding + Cached 64-bit Cardinality) + 12,288 Bytes (16,384 x 6-bit Registers)
= Fixed Maximum Size of 12,304 Bytes (~12 KB)!
```

### 2. Sparse vs. Dense Representation Escalation
- **Sparse Encoding (`< 3,000 Bytes`):** Newly created HLLs with few elements use Run-Length Encoding (`ZERO`, `XZERO`, `VAL` opcodes) to compress empty zero registers, consuming **as little as 128 bytes**!
- **Dense Encoding (`12,304 Bytes`):** Once the sparse byte representation exceeds `hll-sparse-max-bytes` (default `3000`) or any register exceeds `32`, Redis transparently converts the HLL to the fixed 12 KB dense bit-packed array.
- **Union Merging (`PFMERGE dest src1 src2`):** Computes the deduplicated union of multiple HLLs in $O(m)$ time by taking the register-wise maximum: $\text{Dest}[j] = \max(\text{Src}_1[j], \text{Src}_2[j])$.

---

## 3. Bitmaps & Bitfields: Bit-Level Analytics

### 1. Bitmaps (`SETBIT`, `GETBIT`, `BITCOUNT`, `BITOP`)
- Operates directly on a Redis `STRING` up to 512 MB ($2^{32} \approx 4.29\text{ Billion bits}$).
- `SETBIT dau:2026-11-08 <user_id_offset> 1`: Sets the bit at `<user_id_offset>` to `1` in $O(1)$ time.
- `BITCOUNT dau:2026-11-08`: Uses hardware CPU `POPCNT` instructions to count all `1` bits.
- `BITOP AND dest_key day1 day2 day3`: Computes bitwise intersection across multiple days.

> **The Sparse Offset Memory Trap:** Never pass raw 64-bit Snowflake IDs or random hash integers (e.g., `3,500,000,000`) directly to `SETBIT`! Setting bit `3,500,000,000` forces Redis to immediately allocate $3.5\times 10^9 / 8 \approx \mathbf{437\text{ MB}}$ of zero-padded memory! Always use sequential zero-based integer IDs or shard bitmaps into fixed 1M-bit chunks (`key = dau:chunk:(id / 1000000)`, `offset = id % 1000000`).

### 2. Bitfields (`BITFIELD`)
Allows packing arrays of arbitrary-width integers (signed `i1`..`i64` or unsigned `u1`..`u63`) into a single Redis key with explicit overflow protection:
- `WRAP`: Wraps around on integer overflow.
- `SAT`: **Saturates** at the maximum/minimum boundary (e.g., an unsigned 4-bit counter `u4` stays at `15` when incremented past `15`).
- `FAIL`: Aborts the increment on overflow and returns `nil`.

---

## 4. Geospatial Indexing (`GEOADD`, `GEOSEARCH`): 52-Bit Geohash on ZSETs

1. **Physical Storage (`ZSET`)**: A Redis Geo index is physically a standard Sorted Set (`TYPE key` returns `zset`). Because it is a `ZSET`, you remove offline members using standard **`ZREM key member`**!
2. **52-Bit Z-Order Interleaving**: Redis quantizes Longitude ($[-180^\circ, 180^\circ]$) into 26 bits and Latitude ($[-85.05^\circ, 85.05^\circ]$) into 26 bits, interleaving them into a **52-bit integer** stored losslessly inside the 53-bit mantissa of the ZSET's IEEE-754 `double` score (~0.6m precision).
3. **9-Box Neighbor Radius Scan (`GEOSEARCH`)**: To find points within radius $R$, Redis calculates the Geohash bounding box for the center point **plus its 8 surrounding neighbor grid cells**, performs 9 fast $O(\log M)$ skiplist range scans on the ZSET, and filters exact distances using the **Haversine great-circle formula**.

---

## 5. Hands-On Golang Implementation (`github.com/redis/go-redis/v9`)

```go
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	ctx := context.Background()

	// 1. HyperLogLog: Track unique visitors across 2 days & merge with {uv} hash tag
	_ = rdb.PFAdd(ctx, "hll:{uv}:day1", "usr_1", "usr_2", "usr_3").Err()
	_ = rdb.PFAdd(ctx, "hll:{uv}:day2", "usr_3", "usr_4", "usr_5").Err()
	_ = rdb.PFMerge(ctx, "hll:{uv}:union", "hll:{uv}:day1", "hll:{uv}:day2").Err()
	uvCount, _ := rdb.PFCount(ctx, "hll:{uv}:union").Result()
	fmt.Printf("1. [HyperLogLog] 2-Day Deduplicated Unique Visitors: %d (Expected: 5)\n", uvCount)

	// 2. Bitmaps: Compute 2-Day Consecutive Retention Cohort via BITOP AND
	_ = rdb.SetBit(ctx, "bmp:{dau}:day1", 101, 1).Err()
	_ = rdb.SetBit(ctx, "bmp:{dau}:day1", 202, 1).Err()
	_ = rdb.SetBit(ctx, "bmp:{dau}:day2", 202, 1).Err() // Only User 202 active both days!
	_ = rdb.BitOpAnd(ctx, "bmp:{dau}:retained", "bmp:{dau}:day1", "bmp:{dau}:day2").Err()
	retained, _ := rdb.BitCount(ctx, "bmp:{dau}:retained", nil).Result()
	fmt.Printf("2. [Bitmap AND]  Consecutive Retained Users:         %d (Expected: 1)\n", retained)

	// 3. Geospatial: Add drivers & query within 3 km radius using GEOSEARCH
	_ = rdb.GeoAdd(ctx, "geo:drivers:sf",
		&redis.GeoLocation{Name: "driver_A", Longitude: -122.4194, Latitude: 37.7749},
		&redis.GeoLocation{Name: "driver_B", Longitude: -122.4089, Latitude: 37.7833},
	).Err()
	nearby, _ := rdb.GeoSearchLocation(ctx, "geo:drivers:sf", &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude: -122.4194, Latitude: 37.7749, Radius: 3.0, RadiusUnit: "km", Sort: "ASC",
		},
		WithDist: true,
	}).Result()
	fmt.Printf("3. [Geospatial]  Found %d drivers within 3km (Nearest: %s at %.2f km)\n",
		len(nearby), nearby[0].Name, nearby[0].Dist)
}
```
