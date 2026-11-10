#!/bin/sh
# Wait for redis master to be reachable before starting Sentinel
until redis-cli -h redis-master -p 6379 ping >/dev/null 2>&1; do
  echo "Waiting for redis-master..."
  sleep 1
done

# Create sentinel configuration.
# NOTE: Since Redis 6.2, Sentinel rejects hostnames unless
# `resolve-hostnames yes` is declared BEFORE the `monitor` line.
cat > /tmp/sentinel.conf <<'EOF'
port 26379
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel monitor mymaster redis-master 6379 2
sentinel down-after-milliseconds mymaster 3000
sentinel failover-timeout mymaster 6000
sentinel parallel-syncs mymaster 1
EOF

exec redis-sentinel /tmp/sentinel.conf
