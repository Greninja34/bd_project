#!/usr/bin/env bash
# doctor.sh  -  run me on ANY node when something is broken, then paste the whole output in the group chat.
#   bash ~/bd_project/scripts/doctor.sh
# READ-ONLY: it changes nothing and starts nothing. Every command has a timeout, so it never hangs.
# Sections: this machine, /etc/hosts, ping + ZeroTier, daemons (jps) and their ports, the ports of the
# other nodes, HDFS and Spark as seen from here, firewall, and the last ERROR/WARN lines of every daemon
# log with the fix from TROUBLESHOOTING.md#log-patterns. Ends with "doctor: N FAIL" (exit code 1 if any).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -n "${NODE_NAME:-}" ] || . "$HERE/env.sh"
LOGS="${STATE_DIR:-$HERE/../state}/logs"
BAD=0

if [ -t 1 ]; then G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; B=$'\033[1m'; Z=$'\033[0m'; else G=; R=; Y=; B=; Z=; fi
say()  { printf '\n%s== %s%s\n' "$B" "$*" "$Z"; }
ok()   { printf '  %sOK%s   %s\n' "$G" "$Z" "$*"; }
warn() { printf '  %sWARN%s %s\n' "$Y" "$Z" "$*"; }
bad()  { printf '  %sFAIL%s %s\n' "$R" "$Z" "$1"; [ -n "${2:-}" ] && echo "       fix: $2"; BAD=$((BAD + 1)); }
node_ip() { local v="NODE${1#node}_IP"; echo "${!v:-}"; }
open_port() { timeout 3 bash -c "</dev/tcp/$1/$2" 2>/dev/null; }
loopback() { grep -qE '^(127\.|\[?::1|\[::ffff:127)'; }

# grep pattern | fix   (the table of TROUBLESHOOTING.md#log-patterns)
PATTERNS=(
  'Initial job has not accepted any resources|executors cannot reach the driver / no free cores: TROUBLESHOOTING#no-resources'
  'Master removed our application|Failed to connect to master|Could not connect to spark://|cannot reach the Spark master node3:7077: TROUBLESHOOTING#no-resources'
  'Python in worker has different version|executor Python is not 3.10: TROUBLESHOOTING#py-version'
  'python3.10: (not found|No such file)|Cannot run program "/usr/bin/python3.10"|python3.10 missing on a worker: TROUBLESHOOTING#py-missing'
  'Incompatible clusterIDs|old DataNode folder after a second format: TROUBLESHOOTING#clusterid'
  'Name node is in safe mode|SafeModeException|normal ~30 s after NameNode start: TROUBLESHOOTING#safemode'
  'could only be written to [0-9]+ of the [0-9]+ minReplication|client cannot reach a DataNode on 9866: TROUBLESHOOTING#minreplication'
  'Connection refused.*:9000|Call From .* to node2:9000 failed|NameNode not running / not reachable: TROUBLESHOOTING#refused-9000'
  'Permission denied|dfs.permissions.enabled is not false on node2: TROUBLESHOOTING#permission'
  'TABLE_OR_VIEW_NOT_FOUND|Table or view not found|NoSuchTableException|an earlier step did not finish: TROUBLESHOOTING#table-not-found'
  'ClassNotFoundException.*iceberg|Iceberg jar missing: TROUBLESHOOTING#iceberg-class'
  'FAIL index built from the current docs snapshot|FAIL PageRank computed from the current docs snapshot|stale index/PageRank: TROUBLESHOOTING#stale-export'
  'already exists|export version or tag name taken: TROUBLESHOOTING#exists'
  'BindException|Address already in use|port held by another process: TROUBLESHOOTING#port-in-use'
  'UnknownHostException|a node name does not resolve (/etc/hosts): TROUBLESHOOTING#ping'
  'No space left on device|disk full: df -h'
)
annotate() {   # annotate <file> <n>: the last n ERROR/WARN lines, each followed by its fix
  local line p
  while IFS= read -r line; do
    echo "    ${line:0:200}"
    for p in "${PATTERNS[@]}"; do
      if echo "$line" | grep -qE "${p%|*}"; then echo "        -> ${p##*|}"; break; fi
    done
  done < <(grep -E 'ERROR|WARN|Exception|FAIL' "$1" 2>/dev/null | tail -"$2")
}

say "this machine"
echo "  date: $(date)   hostname: $(hostname)   user: $USER"
if [ -n "${NODE_NAME:-}" ]; then ok "I am $NODE_NAME, team IP $MY_IP on $(ip -4 -o addr show | awk -v ip="$MY_IP" '$4 ~ "^"ip"/" {print $2; exit}')"
else bad "none of the team.env IPs is on this machine" "ip -4 -br a ; join ZeroTier and authorize it in Central (TROUBLESHOOTING#zt-ip)"; fi
grep -qi microsoft /proc/version 2>/dev/null && { echo "  WSL2 detected: ZeroTier must run INSIDE Ubuntu (TROUBLESHOOTING#wsl2)"; command -v zerotier-cli >/dev/null || bad "zerotier-one is not installed inside this WSL Ubuntu" "RUNBOOK S02"; }
java -version 2>&1 | head -1 | sed 's/^/  java: /'
[ -x /usr/bin/python3.10 ] && echo "  python3.10: $(/usr/bin/python3.10 --version 2>&1)" || echo "  python3.10: not installed (needed on nodes 1-3)"
echo "  HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-unset}  SPARK_CONF_DIR=${SPARK_CONF_DIR:-unset}  SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-unset}"
[ -d "${STATE_DIR:-/nonexistent}/conf/hadoop" ] && ok "config generated (state/conf)" || bad "no config in state/conf" "RUNBOOK S09, S10"
[ -x "$HERE/../venv/bin/python" ] && ok "venv exists" || bad "no venv" "RUNBOOK S04"
if [ "${NODE_NAME:-node4}" != node4 ]; then
  [ -f "${STATE_DIR:-}/jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar" ] && ok "Iceberg jar in state/jars" || bad "Iceberg jar missing" "RUNBOOK S08"
  case ${SPARK_LOCAL_IP:-} in 127.*|localhost) bad "SPARK_LOCAL_IP=$SPARK_LOCAL_IP" "open a new terminal and activate (TROUBLESHOOTING#solo-leftover)" ;; esac
fi

say "/etc/hosts"
for n in node1 node2 node3 node4; do
  got=$(getent hosts $n | awk '{print $1; exit}')
  [ "$got" = "$(node_ip $n)" ] && ok "$n -> $got" || bad "$n -> '${got:-nothing}', team.env says $(node_ip $n)" "RUNBOOK S05"
done
got=$(getent hosts "$(hostname)" | awk '{print $1; exit}')
[ "$got" = "${MY_IP:-x}" ] && ok "$(hostname) -> $got" || bad "$(hostname) -> '${got:-nothing}', expected ${MY_IP:-your ZeroTier IP}" "RUNBOOK S05: the 127.0.1.1 line breaks Hadoop/Spark (TROUBLESHOOTING#dn-ip)"
grep -E '^127\.0\.1\.1' /etc/hosts | grep -qw "$(hostname)" && bad "an active 127.0.1.1 line maps this hostname to loopback" "comment it out (RUNBOOK S05)"

say "ping and ZeroTier"
for n in node1 node2 node3 node4; do
  [ "$n" = "${NODE_NAME:-}" ] && continue
  ping -c1 -W2 "$(node_ip $n)" >/dev/null 2>&1 && ok "ping $n" || bad "ping $n failed" "ZeroTier joined + authorized on both sides? (TROUBLESHOOTING#ping)"
done
if command -v zerotier-cli >/dev/null; then
  if info=$(timeout 10 sudo -n zerotier-cli info 2>/dev/null || timeout 10 zerotier-cli info 2>/dev/null); then
    ok "$info   <- the node IDs must differ on all 4 nodes (TROUBLESHOOTING#dup-id)"
    timeout 10 sudo -n zerotier-cli peers 2>/dev/null | awk '$3=="LEAF" || $4=="LEAF" {print "          peer " $1 " " $5 " " $6}' | head -8
    echo "          DIRECT is good, RELAY works but is slow (TROUBLESHOOTING#mtu)"
  else warn "cannot run zerotier-cli without a sudo password: try 'sudo zerotier-cli info' and 'sudo zerotier-cli peers'"; fi
else warn "zerotier-cli not found on this machine"; fi

say "daemons on this node (jps) and their ports"
jps 2>/dev/null | grep -v Jps | sed 's/^/  /'
while read -r pid name; do
  case $name in NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager|Master|Worker|HistoryServer)
    tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q "${STATE_DIR:-/nonexistent}" || bad "$name ($pid) is an old course daemon" "RUNBOOK S11 (TROUBLESHOOTING#old-daemons)" ;; esac
done < <(jps 2>/dev/null)
case ${NODE_NAME:-} in node2) want="9000 9870 9866 8081" ;; node3) want="7077 8080 8081 9866 18080" ;; node4) want="8000" ;; *) want="" ;; esac
for p in $want; do
  line=$(ss -tln 2>/dev/null | awk -v p=":$p\$" '$4 ~ p {print $4; exit}')
  if [ -z "$line" ]; then warn "nothing listens on port $p (daemon not started yet?)"
  elif echo "$line" | loopback; then bad "port $p listens only on loopback ($line)" "/etc/hosts maps this hostname to 127.x: RUNBOOK S05, then restart the daemon"
  else ok "port $p listening on $line"; fi
done

say "ports of the other nodes (from this machine)"
for spec in "node2 9000 HDFS NameNode" "node2 9870 NameNode UI" "node2 9866 DataNode" "node3 9866 DataNode" "node3 7077 Spark master" "node3 8080 master UI" "node3 18080 history server" "node4 8000 search service"; do
  if open_port "$(node_ip ${spec%% *})" "$(echo "$spec" | awk '{print $2}')"; then ok "$spec"; else warn "$spec: no answer (not started yet, or blocked: check.sh network)"; fi
done

say "HDFS as seen from here"
if command -v hdfs >/dev/null && open_port "${NODE2_IP:-node2}" 9000; then
  rep=$(timeout 20 hdfs dfsadmin -report 2>/dev/null)
  n=$(echo "$rep" | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')
  ips=$(echo "$rep" | awk '/^Live datanodes/ {l=1; next} /^Dead datanodes/ {l=0} l && /^Name:/ {split($2,a,":"); print a[1]}' | tr '\n' ' ')
  if [ "${n:-0}" = 2 ] && [[ "$ips" == *"${NODE2_IP:-x}"* && "$ips" == *"${NODE3_IP:-x}"* ]]; then ok "2 live DataNodes: $ips"
  else bad "live DataNodes: ${n:-0} [$ips], want 2: ${NODE2_IP:-} ${NODE3_IP:-}" "127.x or NAT address = that node's /etc/hosts is wrong (TROUBLESHOOTING#dn-ip); missing = TROUBLESHOOTING#dn-missing"; fi
  timeout 20 hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && ok "safe mode OFF" || warn "safe mode is ON (normal for 30 s after the NameNode starts)"
else warn "NameNode node2:9000 does not answer (not started, or this node cannot reach it)"; fi

say "Spark master as seen from here (http://${NODE3_IP:-node3}:8080/json/)"
if json=$(curl -s -m 5 "http://${NODE3_IP:-node3}:8080/json/") && [ -n "$json" ]; then
  echo "$json" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("  master %s %s, cores used %s of %s" % (d["url"], d["status"], d["coresused"], d["cores"]))
for w in d["workers"]: print("  worker %s %s" % (w["host"], w["state"]))
for a in d.get("activeapps", []): print("  RUNNING application:", a["name"])' 2>/dev/null
  w=$(echo "$json" | python3 -c 'import sys, json; print(" ".join(x["host"] for x in json.load(sys.stdin)["workers"] if x["state"] == "ALIVE"))' 2>/dev/null)
  [[ "$w" == *"${NODE2_IP:-x}"* && "$w" == *"${NODE3_IP:-x}"* ]] && ok "2 ALIVE workers: $w" || bad "ALIVE workers: ${w:-none}" "start-worker.sh spark://${NODE3_IP:-node3}:7077 on node2 / node3 (TROUBLESHOOTING#one-worker)"
else warn "no answer from the Spark master (not started, or not reachable from here)"; fi

say "firewall"
if command -v ufw >/dev/null; then
  st=$(sudo -n ufw status 2>/dev/null | head -1)
  case $st in *inactive*) ok "ufw inactive" ;; "") warn "ufw status unreadable without sudo: run 'sudo ufw status'" ;; *) ok "ufw $st"; sudo -n ufw status 2>/dev/null | grep ALLOW | head -8 | sed 's/^/          /' ;; esac
else ok "no ufw on this machine"; fi

say "last ERROR/WARN lines of each daemon log, with the fix"
for f in "$LOGS"/hadoop/*.log "$LOGS"/spark/*.out; do
  [ -f "$f" ] || continue
  echo "  --- ${f#"$LOGS"/}"; annotate "$f" 15
done
echo "  --- the 3 newest job logs"
for f in $(ls -t "$LOGS"/*.log 2>/dev/null | head -3); do echo "  --- ${f#"$LOGS"/}"; annotate "$f" 15; done

echo
if [ "$BAD" = 0 ]; then echo "doctor: no FAIL on ${NODE_NAME:-this node}. Paste this output in the chat if something still does not work."
else echo "doctor: $BAD FAIL on ${NODE_NAME:-this node}. Fix the first FAIL, run it again, paste the output in the chat if stuck."; exit 1; fi
