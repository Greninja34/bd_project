#!/usr/bin/env bash
# bd meter <command...> - run a command and report how many bytes went over the ZeroTier interface.
#   bd meter bd pagerank 1
# Counts ALL traffic on the interface that holds this node's team IP during the command.
BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export BD_HOME BD_SCRIPT=1
# shellcheck source=env.sh
. "$BD_HOME/scripts/env.sh"
bd_need_detected
[ "$#" -gt 0 ] || die "usage: bd meter <command...>"
IFC="$(bd_iface_of "$MY_IP")"
[ -n "$IFC" ] || die "no interface holds $MY_IP"
rx0=$(cat "/sys/class/net/$IFC/statistics/rx_bytes"); tx0=$(cat "/sys/class/net/$IFC/statistics/tx_bytes")
t0=$(date +%s)
"$@"
rc=$?
rx1=$(cat "/sys/class/net/$IFC/statistics/rx_bytes"); tx1=$(cat "/sys/class/net/$IFC/statistics/tx_bytes")
awk -v h="$BD_NODE" -v i="$IFC" -v s="$(( $(date +%s) - t0 ))" -v rx=$((rx1-rx0)) -v tx=$((tx1-tx0)) -v rc=$rc \
  'BEGIN { printf "\n[netmeter] %s on %s: %ss, received %.2f MB, sent %.2f MB (exit %s)\n", h, i, s, rx/1048576, tx/1048576, rc }'
exit $rc
