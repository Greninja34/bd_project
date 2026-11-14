#!/usr/bin/env bash
# bd hdfs-format [--force] [--yes] - node2 only, ONCE: format the NameNode, start it, create the HDFS folders.
set -euo pipefail
BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export BD_HOME BD_SCRIPT=1
# shellcheck source=env.sh
. "$BD_HOME/scripts/env.sh"
bd_need_detected

FORCE=0; YES=0
for a in "$@"; do case "$a" in --force) FORCE=1 ;; --yes) YES=1 ;; esac; done
NN_DIR="$BD_STATE/hdfs/namenode"

[ -d "$BD_STATE/conf/hadoop" ] || die "no config yet: run bd setup"
mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR"
bd_stop_old

if [ -f "$NN_DIR/current/VERSION" ] && [ "$FORCE" = 0 ]; then
  fail "the NameNode in $NN_DIR is already formatted. Formatting again would delete the whole HDFS."
  hint "Just start it: bd start.   To really start over: bd hdfs-format --force (then every DataNode dir must be wiped: bd datanode wipe)"
  exit 1
fi
if [ "$YES" = 0 ]; then
  read -r -p "Format HDFS on node2 (erases all HDFS metadata in $NN_DIR)? Type yes: " ans
  [ "$ans" = yes ] || { echo "aborted."; exit 1; }
fi

if jps 2>/dev/null | grep -qE 'NameNode|DataNode'; then "$BD_HOME/scripts/stop.sh" >/dev/null || true; fi
if [ "$FORCE" = 1 ]; then rm -rf "$NN_DIR" "$BD_STATE/hdfs/datanode"; fi
step "Format"
hdfs namenode -format -nonInteractive -force 2>&1 | grep -E "successfully formatted|ERROR|Exception" || true
[ -f "$NN_DIR/current/VERSION" ] || die "format failed: see $HADOOP_LOG_DIR and TROUBLESHOOTING.md"
ok "formatted $NN_DIR"

step "Start the NameNode"
hdfs --daemon start namenode
bd_wait_port "$MY_IP" 9000 60 || die "NameNode did not open port 9000 (bd logs)"
ok "NameNode listening on $MY_IP:9000"
for _ in $(seq 1 30); do
  hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && break
  sleep 3
done
hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF || hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true

step "Create the HDFS folders"
hdfs dfs -mkdir -p /search/input/phase-0 /search/input/phase-1 /search/exports /warehouse /spark-logs
hdfs dfs -ls -R /
if [ "$FORCE" = 1 ]; then
  warn "DataNodes on node3 (and node2) still hold the OLD cluster ID: on node3 run: bd stop ; bd datanode wipe ; bd start"
fi
echo
echo "Next (RUNBOOK B4): bd start on node2 and node3, then bd status."
