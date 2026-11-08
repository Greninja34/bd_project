#!/usr/bin/env bash
# doctor.sh - run on ANY node when something is broken. Read-only: it changes nothing.
#   bash ~/project_search_engine/code/tools/doctor.sh
# Prints: who am I, overlay IP, /etc/hosts, ping to the other nodes, Java services (jps),
# and whether every port the cluster uses is reachable from this machine.

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()  { printf '  \033[32mOK\033[0m   %s\n' "$*"; }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

say "this machine"
echo "  hostname: $(hostname)   user: $USER"
ZT_IF=$(ip -o link show | awk -F': ' '$2 ~ /^zt/ {print $2; exit}')
if [ -n "$ZT_IF" ]; then
  ok "ZeroTier interface $ZT_IF: $(ip -4 -o addr show "$ZT_IF" | awk '{print $4}')"
else
  bad "no ZeroTier interface (zt...) - is zerotier-one running and joined?"
fi
java -version 2>&1 | head -1 | sed 's/^/  java: /'
[ -x /usr/bin/python3.10 ] && echo "  python3.10: $(/usr/bin/python3.10 --version 2>&1)" || echo "  python3.10: not installed (needed on nodes 1-3)"

say "/etc/hosts entries for node1-node4"
grep -E '\bnode[1-4]\b' /etc/hosts | sed 's/^/  /'
[ "${PIPESTATUS[0]}" -eq 0 ] || bad "no nodeN lines in /etc/hosts"
if grep -qE '^127\.0\.1\.1' /etc/hosts; then
  bad "a 127.0.1.1 line is still active - comment it out (daemons would bind to loopback)"
fi

say "ping"
for n in node1 node2 node3 node4; do
  ip=$(getent hosts "$n" | awk '{print $1}')
  if [ -z "$ip" ]; then bad "$n does not resolve"; continue; fi
  if ping -c1 -W2 "$n" >/dev/null 2>&1; then ok "$n ($ip)"; else bad "$n ($ip) no reply"; fi
done
if command -v zerotier-cli >/dev/null; then
  echo "  ZeroTier peer paths (DIRECT is good, RELAY works but is slow):"
  sudo -n zerotier-cli peers 2>/dev/null | awk 'NR>1 && $4=="LEAF" {print "   ", $1, $4, $5}' || echo "    (run with sudo to see peers)"
fi

say "java services on this node (jps)"
jps 2>/dev/null | grep -v Jps | sed 's/^/  /'

say "ports (from this machine)"
check() {  # host port what
  if timeout 3 bash -c "</dev/tcp/$1/$2" 2>/dev/null; then ok "$1:$2  $3"; else bad "$1:$2  $3"; fi
}
check node2 9000 "HDFS NameNode RPC"
check node2 9870 "HDFS NameNode web UI"
check node2 9866 "HDFS DataNode (node2) data transfer"
check node3 9866 "HDFS DataNode (node3) data transfer"
check node3 7077 "Spark master"
check node3 8080 "Spark master web UI"
check node2 8081 "Spark worker (node2) web UI"
check node3 8081 "Spark worker (node3) web UI"
check node4 8000 "search service (node4)"

if command -v hdfs >/dev/null; then
  say "HDFS as seen from here"
  timeout 20 hdfs dfsadmin -report 2>/dev/null | grep -E "^(Live|Dead) datanodes|^Name:|^Hostname:" | sed 's/^/  /' \
    || bad "hdfs dfsadmin -report failed (NameNode down, or fs.defaultFS wrong in core-site.xml)"
fi
if command -v ufw >/dev/null; then
  say "firewall"
  sudo -n ufw status 2>/dev/null | head -12 | sed 's/^/  /' || echo "  (run with sudo to see ufw status)"
fi
