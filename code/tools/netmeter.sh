#!/usr/bin/env bash
# netmeter.sh - run any command and report how many bytes went over the ZeroTier interface.
#   bash ~/project_search_engine/code/tools/netmeter.sh spark-submit ... pagerank.py
# Answers "where does the network traffic actually go": run it on each node (or just on the
# driver node) for each stage and compare. Counts ALL traffic on the overlay during the command.
ZT_IF=$(ip -o link show | awk -F': ' '$2 ~ /^zt/ {print $2; exit}')
[ -z "$ZT_IF" ] && { echo "no zt* interface found"; exit 1; }
rx0=$(cat /sys/class/net/$ZT_IF/statistics/rx_bytes); tx0=$(cat /sys/class/net/$ZT_IF/statistics/tx_bytes)
t0=$(date +%s)
"$@"
rc=$?
rx1=$(cat /sys/class/net/$ZT_IF/statistics/rx_bytes); tx1=$(cat /sys/class/net/$ZT_IF/statistics/tx_bytes)
awk -v h="$(hostname)" -v i="$ZT_IF" -v s="$(( $(date +%s) - t0 ))" -v rx=$((rx1-rx0)) -v tx=$((tx1-tx0)) -v rc=$rc \
  'BEGIN { printf "\n[netmeter] %s on %s: %ss, received %.2f MB, sent %.2f MB (exit %s)\n", h, i, s, rx/1048576, tx/1048576, rc }'
exit $rc
