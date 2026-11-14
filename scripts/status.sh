#!/usr/bin/env bash
# bd status [--now] - wait (up to ~3 min) until the whole cluster is healthy. Works on any node.
#   2 live DataNodes (node2 + node3 IPs), 2 ALIVE Spark workers, HDFS safemode OFF, /spark-logs exists
set -euo pipefail
BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export BD_HOME BD_SCRIPT=1
# shellcheck source=env.sh
. "$BD_HOME/scripts/env.sh"
bd_need_detected

N2="$(bd_ip node2)"; N3="$(bd_ip node3)"
LIMIT=180; [ "${1:-}" = "--now" ] && LIMIT=0

datanodes() {                          # IPs of live DataNodes, one per line
  hdfs dfsadmin -report 2>/dev/null | awk '/^Live datanodes/ {l=1; next} /^Dead datanodes/ {l=0} l && /^Name:/ {split($2,a,":"); print a[1]}' || true
}
workers() {                            # IPs of ALIVE workers, one per line
  curl -s -m 3 "http://$N3:8080/json/" 2>/dev/null | python3 -c '
import sys, json
try:
    for w in json.load(sys.stdin).get("workers", []):
        if w.get("state") == "ALIVE": print(w.get("host"))
except Exception:
    pass' || true
}

check_once() {
  DN="$(datanodes | sort | tr '\n' ' ')"; WK="$(workers | sort | tr '\n' ' ')"
  SAFE="$(hdfs dfsadmin -safemode get 2>/dev/null || true)"
  hdfs dfs -test -d /spark-logs 2>/dev/null && LOGS=yes || LOGS=no
  DN_OK=0; WK_OK=0; SAFE_OK=0
  [[ "$DN" == *"$N2 "* && "$DN" == *"$N3 "* ]] && DN_OK=1
  [[ "$WK" == *"$N2 "* && "$WK" == *"$N3 "* ]] && WK_OK=1
  [[ "$SAFE" == *OFF* ]] && SAFE_OK=1
  [ "$DN_OK$WK_OK$SAFE_OK$LOGS" = "111yes" ]
}

t0=$(date +%s)
while :; do
  if check_once; then break; fi
  now=$(( $(date +%s) - t0 ))
  [ "$now" -ge "$LIMIT" ] && break
  printf '  waiting (%ss): datanodes [%s] workers [%s] safemode [%s] /spark-logs %s\n' "$now" "${DN:-none}" "${WK:-none}" "${SAFE:-no answer}" "$LOGS"
  sleep 6
done

step "Cluster status (seen from $BD_NODE)"
bad=0
if [ "$DN_OK" = 1 ]; then ok "2 live DataNodes: $DN"; else bad=1; fail "live DataNodes: ${DN:-none} (want $N2 and $N3)"; hint "node2/node3: bd start ; DataNode IP must be the ZeroTier IP: bd doctor"; fi
if [ "$WK_OK" = 1 ]; then ok "2 ALIVE Spark workers: $WK"; else bad=1; fail "ALIVE workers: ${WK:-none} (want $N2 and $N3)"; hint "node2/node3: bd start ; master UI http://$N3:8080"; fi
if [ "$SAFE_OK" = 1 ]; then ok "HDFS safe mode is OFF"; else bad=1; fail "safe mode: ${SAFE:-NameNode not answering}"; hint "wait 30 s after the NameNode starts; node2: bd start"; fi
if [ "$LOGS" = yes ]; then ok "/spark-logs exists in HDFS"; else bad=1; fail "/spark-logs missing"; hint "node2: bd hdfs-format creates it (or: hdfs dfs -mkdir -p /spark-logs)"; fi
if [ "$bad" = 0 ]; then echo "  Cluster is healthy."; else echo "  Not healthy yet. Run bd doctor, bd logs (TROUBLESHOOTING.md)."; exit 1; fi
