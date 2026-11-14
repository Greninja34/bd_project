#!/usr/bin/env bash
# bd start - start the daemons of THIS node (node2 and node3 only; node1/node4 are clients).
# Order between nodes does not matter: DataNodes and workers keep retrying until their master is up.
set -euo pipefail
BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export BD_HOME BD_SCRIPT=1
# shellcheck source=env.sh
. "$BD_HOME/scripts/env.sh"
bd_need_detected

N3="$(bd_ip node3)"
HDFS_URI=hdfs://node2:9000
MARK="$(mktemp)"

running() { jps 2>/dev/null | grep -qw "$1"; }

log_files() { ls "$HADOOP_LOG_DIR"/*.log "$HADOOP_LOG_DIR"/*.out "$SPARK_LOG_DIR"/*.out 2>/dev/null || true; }

log_mark() { for f in $(log_files); do echo "$f $(wc -l < "$f")"; done > "$MARK"; }

log_scan() {                          # look only at log lines written since log_mark
  local f from tmp found=0
  tmp="$(mktemp)"
  for f in $(log_files); do
    from="$(awk -v f="$f" '$1 == f {print $2}' "$MARK")"
    tail -n +"$(( ${from:-0} + 1 ))" "$f" > "$tmp"
    if bd_known_errors "$tmp"; then found=1; hint "(in $f)"; fi
  done
  rm -f "$tmp"
  [ "$found" = 0 ] && ok "no known errors in the new daemon logs"
}

start_hdfs() {                        # start_hdfs namenode|datanode PORT
  if running "$( [ "$1" = namenode ] && echo NameNode || echo DataNode )"; then ok "$1 already running"; return; fi
  hdfs --daemon start "$1"
  if bd_wait_port "$MY_IP" "$2" 40; then ok "$1 up (port $2 on $MY_IP)"; else fail "$1 did not open port $2 on $MY_IP within 40 s"; fi
}

start_spark() {                       # start_spark master|worker
  if running "$( [ "$1" = master ] && echo Master || echo Worker )"; then ok "spark $1 already running"; return; fi
  if [ "$1" = master ]; then start-master.sh >/dev/null; bd_wait_port "$MY_IP" 7077 30 && ok "master up (spark://$N3:7077)" || fail "master did not open 7077"
  else start-worker.sh "spark://$N3:7077" >/dev/null; bd_wait_port "$MY_IP" 8081 30 && ok "worker up (UI $MY_IP:8081), registering at spark://$N3:7077" || fail "worker did not open 8081"; fi
}

start_history() {
  if running HistoryServer; then ok "history server already running"; return; fi
  local i
  for ((i = 0; i < 18; i++)); do hdfs dfs -test -d /spark-logs 2>/dev/null && break; sleep 5; done
  if ! hdfs dfs -test -d /spark-logs 2>/dev/null; then
    warn "/spark-logs does not exist yet (is HDFS up and formatted on node2?). History server not started."
    hint "run bd start again once bd status shows HDFS is ready"; return
  fi
  SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$HDFS_URI/spark-logs" start-history-server.sh >/dev/null
  bd_wait_port "$MY_IP" 18080 30 && ok "history server up (http://$MY_IP:18080)" || fail "history server did not open 18080"
}

case "$BD_NODE" in
  node1|node4) echo "$BD_NODE is a client node: no daemons to start."; exit 0 ;;
esac
[ -d "$BD_STATE/conf/hadoop" ] || die "no config yet: run bd setup"
mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" "$SPARK_LOG_DIR" "$SPARK_PID_DIR"

step "Old course daemons"
if [ "${1:-}" = "--keep-old" ]; then warn "keeping old daemons (--keep-old)"; else bd_stop_old; fi

step "Start daemons on $BD_NODE ($MY_IP)"
log_mark
if [ "$BD_NODE" = node2 ]; then
  [ -f "$BD_STATE/hdfs/namenode/current/VERSION" ] || die "NameNode is not formatted yet. Run once: bd hdfs-format"
  start_hdfs namenode 9000
  start_hdfs datanode 9866
  start_spark worker
else
  start_hdfs datanode 9866
  start_spark master
  start_spark worker
  start_history
fi
sleep 3
step "Log check"
log_scan
echo
echo "Next: bd status   (waits until the whole cluster is healthy)"
rm -f "$MARK"
