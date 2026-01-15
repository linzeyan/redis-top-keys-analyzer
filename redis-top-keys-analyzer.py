#!/usr/bin/env python3
"""
Redis Top Keys Analyzer
找出每種資料類型中佔用記憶體最多的前 10 個 keys
"""

import redis
from collections import defaultdict
import sys

# ------------------------------------------------------------
# 參數解析：支援 host、host:port、host port
# ------------------------------------------------------------
def parse_host_port():
    host = "localhost"
    port = 6379

    args = sys.argv[1:]

    if len(args) == 0:
        return host, port

    if len(args) == 1:
        # input -> host 或 host:port
        if ":" in args[0]:
            h, p = args[0].split(":", 1)
            return h, int(p)
        return args[0], port

    if len(args) >= 2:
        return args[0], int(args[1])

    return host, port


host, port = parse_host_port()

print(f"連線 Redis: {host}:{port}")

# Redis Connect
r = redis.Redis(host=host, port=port, decode_responses=True)

print("正在掃描 Redis keys...")
print("這可能需要幾分鐘，請稍候...\n")

type_keys = defaultdict(list)
scanned = 0
errors = 0

cursor = 0
while True:
    cursor, keys = r.scan(cursor=cursor, count=1000)

    for key in keys:
        try:
            mem = r.memory_usage(key)
            key_type = r.type(key)

            if mem:
                type_keys[key_type].append((mem, key))
                scanned += 1

                if scanned % 10000 == 0:
                    print(f"已掃描 {scanned} keys...", end="\r")
        except Exception:
            errors += 1

    if cursor == 0:
        break

print(f"\n完成！共掃描 {scanned} keys (錯誤: {errors})\n")
print("=" * 120)

# ------------------------------------------------------------
# 顯示每種類型 Top10
# ------------------------------------------------------------
for key_type in ["string", "list", "set", "zset", "hash", "stream"]:
    if key_type not in type_keys:
        continue

    sorted_keys = sorted(type_keys[key_type], reverse=True)[:10]

    if not sorted_keys:
        continue

    print(f"\n🔸 {key_type.upper()} - Top 10")
    print("-" * 120)
    print(f"{'排名':<6} {'記憶體 (MB)':<15} {'記憶體 (Bytes)':<20} {'Key'}")
    print("-" * 120)

    for idx, (mem, key) in enumerate(sorted_keys, 1):
        mem_mb = mem / (1024 * 1024)
        key_display = key if len(key) <= 80 else key[:77] + "..."
        print(f"{idx:<6} {mem_mb:<15.3f} {mem:<20,} {key_display}")

    total_keys = len(type_keys[key_type])
    total_mem = sum(m for m, _ in type_keys[key_type])
    top10_mem = sum(m for m, _ in sorted_keys)
    top10_percentage = (top10_mem / total_mem * 100) if total_mem > 0 else 0

    print(f"\n  統計: 此類型共 {total_keys:,} keys, 總記憶體 {total_mem/(1024*1024):.2f} MB")
    print(f"  Top 10 佔比: {top10_percentage:.2f}% ({top10_mem/(1024*1024):.2f} MB)")

# ------------------------------------------------------------
# 總體摘要
# ------------------------------------------------------------
print("\n" + "=" * 120)
print("總體摘要")
print("=" * 120)

total_all_mem = sum(sum(m for m, _ in keys) for keys in type_keys.values())
print(f"{'類型':<15} {'Keys 數量':<15} {'總記憶體 (MB)':<20} {'佔比'}")
print("-" * 120)

for key_type in sorted(type_keys.keys()):
    count = len(type_keys[key_type])
    mem = sum(m for m, _ in type_keys[key_type])
    mem_mb = mem / (1024 * 1024)
    percentage = (mem / total_all_mem * 100) if total_all_mem > 0 else 0

    print(f"{key_type:<15} {count:<15,} {mem_mb:<20.2f} {percentage:.2f}%")

print(f"\n總計: {scanned:,} keys, {total_all_mem/(1024*1024):.2f} MB")
