#!/usr/bin/env bash
# bd doctor - read-only health check of THIS node. Every line is OK / WARN / FAIL with a one-line fix.
set -uo pipefail
BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export BD_HOME BD_SCRIPT=1
# shellcheck source=env.sh
. "$BD_HOME/scripts/env.sh"
bd_need_detected

BAD=0
f() { fail "$@"; BAD=$((BAD + 1)); }                 # count failures

N2="$(bd_ip node2)"; N3="$(bd_ip node3)"

step "This machine"
ok "I am $BD_NODE, team IP $MY_IP on interface $(bd_iface_of "$MY_IP" || true)  (hostname $(hostname))"
if bd_is_wsl; then
  ok "running inside WSL2"
  command -v zerotier-cli >/dev/null || { f "zerotier-one is not installed inside this WSL Ubuntu"; hint "TROUBLESHOOTING.md#wsl2"; }
fi
[ -d "$BD_STATE/conf/hadoop" ] && ok "config generated (state/conf)" || { f "no config in state/conf"; hint "bd setup"; }
[ -x "$BD_HOME/venv/bin/python" ] && ok "venv exists" || { f "no venv"; hint "bd setup"; }
[ -z "${SPARK_LOCAL_IP:-}" ] || [ "$BD_NODE" = node4 ] || [ "$SPARK_LOCAL_IP" = "$MY_IP" ] || { f "SPARK_LOCAL_IP=$SPARK_LOCAL_IP (should be $MY_IP)"; hint "open a new terminal and source the venv's activate"; }
if [ "$BD_NODE" != node4 ]; then
  [ -f "$BD_STATE/jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar" ] && ok "Iceberg jar in state/jars" || { f "Iceberg jar missing"; hint "bd setup"; }
  [ -x /usr/bin/python3.10 ] && ok "/usr/bin/python3.10 exists" || { f "/usr/bin/python3.10 missing"; hint "bd setup"; }
fi

step "/etc/hosts"
for i in 1 2 3 4; do
  got="$(getent hosts "node$i" | awk '{print $1; exit}')"
  [ "$got" = "$(bd_ip node$i)" ] && ok "node$i -> $got" || { f "node$i -> '${got:-nothing}', team.env says $(bd_ip node$i)"; hint "bd setup"; }
done
got="$(getent hosts "$(hostname)" | awk '{print $1; exit}')"
[ "$got" = "$MY_IP" ] && ok "$(hostname) -> $got" || { f "$(hostname) -> '${got:-nothing}', expected $MY_IP"; hint "bd setup (the 127.0.1.1 line breaks Hadoop/Spark)"; }

step "Reach the other nodes (ping)"
for i in 1 2 3 4; do
  [ "node$i" = "$BD_NODE" ] && continue
  if ping -c1 -W2 "$(bd_ip node$i)" >/dev/null 2>&1; then ok "ping node$i"; else f "ping node$i failed"; hint "ZeroTier joined and authorized? TROUBLESHOOTING.md#ping"; fi
done

step "ZeroTier"
if command -v zerotier-cli >/dev/null; then
  if info="$(sudo -n zerotier-cli info 2>/dev/null)"; then
    ok "$info   <- node IDs must be different on all 4 nodes (TROUBLESHOOTING.md#dup-id)"
    sudo -n zerotier-cli peers 2>/dev/null | awk '$4=="LEAF" {print "          peer " $1 " " $3 " " $6 " " $7 " " $8}' | head -8
    echo "          DIRECT is good; RELAY works but is slow (TROUBLESHOOTING.md#mtu)"
  else warn "cannot run 'sudo zerotier-cli' without a password prompt: skipped (try: sudo zerotier-cli info ; sudo zerotier-cli peers)"; fi
else warn "zerotier-cli not found on this machine"; fi

step "Daemons on this node (jps)"
if command -v jps >/dev/null; then jps | grep -v Jps | sed 's/^/          /' || true; fi
old="$(bd_old_daemons)"
[ -z "$old" ] || { f "old course daemons running: $(echo "$old" | tr '\n' ' ')"; hint "bd stop --old"; }
case "$BD_NODE" in
  node2) want="9000 9870 9866 8081" ;;
  node3) want="7077 8080 8081 9866 18080" ;;
  node4) want="8000" ;;
  *) want="" ;;
esac
for p in $want; do
  line="$(ss -tln 2>/dev/null | awk -v p=":$p\$" '$4 ~ p {print $4; exit}')"
  if [ -z "$line" ]; then warn "nothing listens on port $p yet (bd start / bd serve)"
  elif [[ "$line" == 127.* ]]; then f "port $p listens only on loopback ($line)"; hint "/etc/hosts maps this hostname to 127.x: bd setup"
  else ok "port $p listening on $line"; fi
done

step "Cluster ports (from here)"
for spec in "node2 9000" "node2 9866" "node3 9866" "node3 7077" "node3 8080" "node4 8000"; do
  set -- $spec
  if bd_port_open "$(bd_ip "$1")" "$2" 2; then ok "$1:$2 answers"; else warn "$1:$2 does not answer (not started yet, or blocked: bd netcheck)"; fi
done

if command -v hdfs >/dev/null && bd_port_open "$N2" 9000 2; then
  step "HDFS"
  rep="$(hdfs dfsadmin -report 2>/dev/null)"
  n="$(echo "$rep" | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')"
  ips="$(echo "$rep" | awk '/^Live datanodes/ {l=1; next} /^Dead datanodes/ {l=0} l && /^Name:/ {split($2,a,":"); print a[1]}' | tr '\n' ' ')"
  if [ "${n:-0}" = 2 ] && [[ "$ips" == *"$N2"* && "$ips" == *"$N3"* ]]; then ok "2 live DataNodes: $ips"
  else f "live DataNodes: ${n:-0} [$ips] (want 2: $N2 $N3)"; hint "a 127.x or NAT address means that node's /etc/hosts is wrong: bd setup, bd stop, bd start"; fi
  hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && ok "safe mode OFF" || warn "safe mode is ON (normal 30 s after start)"
fi
if json="$(curl -s -m 3 "http://$N3:8080/json/" 2>/dev/null)" && [ -n "$json" ]; then
  step "Spark master"
  w="$(echo "$json" | python3 -c 'import sys,json; print(" ".join(x["host"] for x in json.load(sys.stdin)["workers"] if x["state"]=="ALIVE"))' 2>/dev/null)"
  [[ "$w" == *"$N2"* && "$w" == *"$N3"* ]] && ok "2 ALIVE workers: $w" || { f "ALIVE workers: ${w:-none}"; hint "node2/node3: bd start"; }
fi

step "Firewall"
if command -v ufw >/dev/null; then
  st="$(sudo -n ufw status 2>/dev/null | head -1)"
  case "$st" in *inactive*) ok "ufw inactive" ;; "") warn "ufw status unreadable without sudo (run: sudo ufw status)" ;; *) ok "ufw $st"; sudo -n ufw status 2>/dev/null | grep -E "ALLOW" | sed 's/^/          /' | head -8 ;; esac
else ok "no ufw"; fi

echo
if [ "$BAD" = 0 ]; then echo "doctor: no FAIL."; else echo "doctor: $BAD FAIL. Fix the first one, then run bd doctor again."; exit 1; fi
