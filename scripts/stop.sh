#!/usr/bin/env bash
# bd stop        - stop the daemons of this node (and uvicorn on node4)
# bd stop --old  - stop only the course's old single-node daemons (YARN, 2nd NameNode, ...)
set -euo pipefail
BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export BD_HOME BD_SCRIPT=1
# shellcheck source=env.sh
. "$BD_HOME/scripts/env.sh"
bd_need_detected

if [ "${1:-}" = "--old" ]; then bd_stop_old; exit 0; fi

case "$BD_NODE" in
  node1) echo "node1 runs no daemons."; exit 0 ;;
  node4) "$BD_HOME/bd" serve stop; exit 0 ;;
esac

step "Stop daemons on $BD_NODE"
if [ "$BD_NODE" = node3 ]; then stop-history-server.sh >/dev/null 2>&1 || true; fi
stop-worker.sh >/dev/null 2>&1 || true
[ "$BD_NODE" = node3 ] && { stop-master.sh >/dev/null 2>&1 || true; }
hdfs --daemon stop datanode >/dev/null 2>&1 || true
[ "$BD_NODE" = node2 ] && { hdfs --daemon stop namenode >/dev/null 2>&1 || true; }
sleep 2
left="$(jps 2>/dev/null | grep -E 'NameNode|DataNode|Master|Worker|HistoryServer' || true)"
if [ -z "$left" ]; then ok "all daemons stopped"; else warn "still running: $left"; hint "bd stop --old   (if these are old course daemons)"; fi
