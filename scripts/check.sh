#!/usr/bin/env bash
# check.sh <stage> [arg]  -  READ-ONLY gate. It only LOOKS (ping, nc, jps, ss, hdfs -ls/-cat, curl);
# it never starts, stops, writes or runs the pipeline. (Only the hdfs test file of the `hdfs` stage is
# written and deleted at once, under /search/.check-*.)
#
#   bash ~/bd_project/scripts/check.sh <stage> [arg]
#   stages: setup network hdfs spark ingest0 ingest1 index export <vN> serve [vN] pulled <vN> down all
#
# Every check prints OK, WARN or FAIL. A FAIL also prints one line with the fix and the
# TROUBLESHOOTING.md anchor. The last line is "GATE <stage> PASSED" or "GATE <stage> FAILED (n problems)"
# and the exit code is 1 on failure: nobody moves on to the next RUNBOOK stage before it says PASSED.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -n "${NODE_NAME:-}" ] || . "$HERE/env.sh"          # not activated in this terminal: load our environment

PROBLEMS=0   # problems found in the stage that is running
TOTAL=0      # problems over all stages (exit code)

# ---------- small helpers ----------
ok()   { echo "OK    $*"; }
warn() { echo "WARN  $*"; }
note() { echo "      $*"; }
fail() { echo "FAIL  $1"; echo "      fix: $2  (TROUBLESHOOTING.md#$3)"; PROBLEMS=$((PROBLEMS + 1)); }
node_ip() { local v="NODE${1#node}_IP"; echo "${!v:-}"; }                    # node3 -> 172.23.10.141
has_spark() { [ "${NODE_NAME:-}" != node4 ]; }                               # only nodes 1-3 have Spark
probe() {  # probe <ip> <port>  ->  open | refused | timeout   (refused = route works, nobody listens)
  local out; out=$(LC_ALL=C nc -zv -w 3 "$1" "$2" 2>&1)
  case $out in *succeeded*|*" open"*) echo open ;; *refused*) echo refused ;; *) echo timeout ;; esac
}
expect() {  # expect <what> <got> <want> <fix> <anchor>
  if [ "$2" = "$3" ]; then ok "$1 = $3"; else fail "$1 is '${2:-unset}', must be '$3'" "$4" "$5"; fi
}
hval() { sed -n "s|.*<name>$1</name><value>\([^<]*\)</value>.*|\1|p" "$HADOOP_CONF_DIR"/*-site.xml 2>/dev/null | head -1; }
sval() { awk -v k="$1" '$1 == k {print $2}' "$SPARK_CONF_DIR/spark-defaults.conf" 2>/dev/null; }
hdfs_up() { [ "$(probe "$NODE2_IP" 9000)" = open ]; }

# Iceberg table sizes WITHOUT Spark: read the table's metadata json from HDFS (cheap, no job).
# prints "CUR=<rows now> ALL=<rows at every snapshot, comma separated>"
snaps() {
  local v; v=$(hdfs dfs -cat "/warehouse/search/$1/metadata/version-hint.text" 2>/dev/null) || return 1
  hdfs dfs -cat "/warehouse/search/$1/metadata/v$v.metadata.json" 2>/dev/null | python3 -c '
import json, sys
m = json.load(sys.stdin); s = m.get("snapshots", [])
tot = lambda x: x["summary"].get("total-records", "?")
cur = [tot(x) for x in s if x["snapshot-id"] == m.get("current-snapshot-id")]
print("CUR=%s ALL=%s" % (cur[0] if cur else "none", ",".join(tot(x) for x in s)))'
}
total_of() { snaps "$1" | sed -n 's/^CUR=\([^ ]*\).*/\1/p'; }
ever_had() { snaps "$1" | sed 's/.*ALL=//' | tr , '\n' | grep -qx "$2"; }
table_is() {  # table_is <table> <rows now>
  local got; got=$(total_of "$1")
  expect "table $1 rows" "$got" "$2" "run the step that writes $1 (RUNBOOK) and read its log" table-not-found
}
lines_of() { hdfs dfs -cat "$1" 2>/dev/null | wc -l; }

# ---------- stage: setup (all nodes) ----------
stage_setup() {
  local n ip got host jar pid name k
  host=$(hostname)
  if [ -n "${NODE_NAME:-}" ]; then ok "env.sh loaded: I am $NODE_NAME ($MY_IP)"
  else fail "env.sh found none of the team.env IPs on this machine" "ip -4 -br a ; join ZeroTier and authorize it in Central" zt-ip; return; fi
  [ "${HADOOP_CONF_DIR:-}" = "$STATE_DIR/conf/hadoop" ] && ok "HADOOP_CONF_DIR = state/conf/hadoop" \
    || fail "HADOOP_CONF_DIR is '${HADOOP_CONF_DIR:-unset}'" "redo RUNBOOK S09, then open a new terminal and activate" conf
  for k in nc curl python3 jps; do command -v "$k" >/dev/null || fail "'$k' is not installed" "sudo apt install -y netcat-openbsd curl python3 (jps comes with Java)" versions; done
  # /etc/hosts
  for n in node1 node2 node3 node4; do
    ip=$(node_ip $n); got=$(getent hosts $n | awk '{print $1; exit}')
    [ "$got" = "$ip" ] && ok "$n resolves to $ip" || fail "$n resolves to '${got:-nothing}', team.env says $ip" "redo RUNBOOK S05 (/etc/hosts block)" ping
  done
  got=$(getent hosts "$host" | awk '{print $1; exit}')
  [ "$got" = "$MY_IP" ] && ok "hostname $host resolves to $MY_IP" \
    || fail "hostname $host resolves to '${got:-nothing}', must be $MY_IP" "redo RUNBOOK S05: comment out 127.0.1.1, put the hostname on your own line" dn-ip
  grep -E '^127\.0\.1\.1' /etc/hosts | grep -qw "$host" \
    && fail "/etc/hosts still maps $host to 127.0.1.1" "redo RUNBOOK S05 (the sed that comments it out)" dn-ip
  # versions
  java -version 2>&1 | head -1 | grep -q '"11\.' && ok "java 11" || fail "java is not 11 ($(java -version 2>&1 | head -1))" "install Java 11, JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" versions
  hadoop version 2>/dev/null | head -1 | grep -q 'Hadoop 3\.3\.6' && ok "hadoop 3.3.6" || fail "hadoop is not 3.3.6 ($(hadoop version 2>&1 | head -1))" "HADOOP_HOME=/opt/hadoop must be 3.3.6" versions
  if has_spark; then
    spark-submit --version 2>&1 | grep -qm1 'version 3\.3\.4' && ok "spark 3.3.4" || fail "spark is not 3.3.4" "SPARK_HOME=/opt/spark must be 3.3.4" versions
    /usr/bin/python3.10 --version >/dev/null 2>&1 && ok "/usr/bin/python3.10 exists" \
      || fail "/usr/bin/python3.10 missing" "sudo ln -s /usr/local/bin/python3.10 /usr/bin/python3.10" py-missing
    [ -x "$PROJECT_HOME/venv/bin/python" ] && ok "venv exists" || fail "no venv in $PROJECT_HOME/venv" "redo RUNBOOK S04" path-venv
  else
    "$PROJECT_HOME/venv/bin/python" -c 'import fastapi, uvicorn, pandas, pyarrow' 2>/dev/null && ok "venv has fastapi uvicorn pandas pyarrow" \
      || fail "venv missing or node4 dependencies not installed" "redo RUNBOOK S04 (pip install -r requirements.txt)" path-venv
  fi
  grep -q 'scripts/env.sh' "$PROJECT_HOME/venv/bin/activate" 2>/dev/null && ok "activate loads env.sh" || fail "venv/bin/activate does not load env.sh" "last line of RUNBOOK S04" path-venv
  # generated Hadoop config
  expect "fs.defaultFS" "$(hval fs.defaultFS)" hdfs://node2:9000 "redo RUNBOOK S09" conf
  expect "dfs.replication" "$(hval dfs.replication)" 2 "redo RUNBOOK S09" conf
  expect "dfs.permissions.enabled" "$(hval dfs.permissions.enabled)" false "redo RUNBOOK S09" permission
  expect "dfs.namenode.rpc-bind-host" "$(hval dfs.namenode.rpc-bind-host)" 0.0.0.0 "redo RUNBOOK S09" conf
  expect "replace-datanode-on-failure" "$(hval dfs.client.block.write.replace-datanode-on-failure.policy)" NEVER "redo RUNBOOK S09" conf
  if has_spark; then   # generated Spark config + Iceberg jar
    jar="$STATE_DIR/jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar"
    if [ -f "$jar" ] && echo "43d0915f3eeec9b732307aee8334f5a84843e568  $jar" | sha1sum -c --quiet 2>/dev/null; then ok "Iceberg jar present, sha1 ok"
    else fail "Iceberg jar missing or damaged" "redo RUNBOOK S08" iceberg-class; fi
    expect "spark.master" "$(sval spark.master)" "spark://$NODE3_IP:7077" "redo RUNBOOK S10" conf
    expect "spark.driver.host" "$(sval spark.driver.host)" "$MY_IP" "redo RUNBOOK S10" conf
    expect "spark.driver.port" "$(sval spark.driver.port)" 40000 "redo RUNBOOK S10" conf
    expect "spark.hadoop.dfs.replication" "$(sval spark.hadoop.dfs.replication)" 2 "redo RUNBOOK S10" conf
    expect "iceberg warehouse" "$(sval spark.sql.catalog.iceberg_catalog.warehouse)" hdfs://node2:9000/warehouse "redo RUNBOOK S10" conf
    grep -q "SPARK_LOCAL_IP=$MY_IP\$" "$SPARK_CONF_DIR/spark-env.sh" 2>/dev/null && ok "spark-env.sh: SPARK_LOCAL_IP=$MY_IP" \
      || fail "spark-env.sh does not set SPARK_LOCAL_IP=$MY_IP" "redo RUNBOOK S10" conf
    case ${SPARK_LOCAL_IP:-} in 127.*|localhost) fail "SPARK_LOCAL_IP=$SPARK_LOCAL_IP in this shell" "open a new terminal and activate" solo-leftover ;; esac
  else
    [ -d "$STATE_DIR/serving" ] && ok "state/serving exists" || fail "state/serving missing" "mkdir -p $STATE_DIR/serving (RUNBOOK S10)" conf
  fi
  grep -nE 'SPARK_LOCAL_(IP|HOSTNAME)=(127\.|localhost)' ~/.bashrc ~/.profile 2>/dev/null \
    && fail "a startup file exports SPARK_LOCAL_IP=127..." "sed -i '/SPARK_LOCAL_\\(IP\\|HOSTNAME\\)/d' ~/.bashrc, new terminal" solo-leftover
  # leftovers of the course: any daemon that does not run from OUR state/ folder is an old one
  while read -r pid name; do
    case $name in NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager|Master|Worker|HistoryServer)
      if tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q "$STATE_DIR"; then note "$name ($pid) is ours"
      else fail "$name ($pid) is an old course daemon" "stop it with the course config (RUNBOOK S11)" old-daemons; fi ;;
    esac
  done < <(jps 2>/dev/null)
}

# ---------- stage: network (all nodes) ----------
ports_of() {  # the ports a node must accept (daemon ports; 40000 = Spark driver, only open during a job)
  case $1 in node1) echo 40000 ;; node2) echo "9000 9870 9866 40000 40020" ;; node3) echo "7077 8080 9866 18080 40000 40020" ;; node4) echo 8000 ;; esac
}
stage_network() {
  local n ip p r up down closed zt peers
  grep -qi microsoft /proc/version 2>/dev/null && note "WSL2 detected: ZeroTier must run INSIDE Ubuntu, not only on Windows (TROUBLESHOOTING.md#wsl2)"
  ip -4 -o addr show | grep -q " $MY_IP/" && ok "$MY_IP is on an interface of this machine" || fail "$MY_IP is not on this machine" "ZeroTier: join + authorize" zt-ip
  if command -v zerotier-cli >/dev/null; then
    zt=$(zerotier-cli info 2>/dev/null || sudo -n zerotier-cli info 2>/dev/null)
    [ -n "$zt" ] && note "ZeroTier node ID: $(echo "$zt" | awk '{print $3}') ($(echo "$zt" | awk '{print $5}')); all 4 IDs must differ" || warn "cannot read 'zerotier-cli info' (run it with sudo yourself)"
    peers=$(sudo -n zerotier-cli peers 2>/dev/null | grep LEAF)
    if [ -n "$peers" ]; then
      note "ZeroTier peers: $(echo "$peers" | grep -c DIRECT) DIRECT, $(echo "$peers" | grep -c RELAY) RELAY"
      echo "$peers" | grep -q RELAY && warn "some peers are RELAY (slow): UDP 9993 blocked? try another network (TROUBLESHOOTING.md#mtu)"
    else note "peers DIRECT/RELAY not read (needs passwordless sudo): run 'sudo zerotier-cli peers | grep LEAF'"; fi
  else warn "zerotier-cli not found here (fine in a container)"; fi
  if command -v ufw >/dev/null && sudo -n ufw status >/dev/null 2>&1; then
    if sudo -n ufw status | grep -q inactive; then ok "ufw inactive"
    else for n in node1 node2 node3 node4; do ip=$(node_ip $n); [ "$ip" = "$MY_IP" ] && continue
           sudo -n ufw status | grep -q "$ip" && ok "ufw allows $n" || fail "ufw has no rule for $n ($ip)" "sudo ufw allow from $ip (RUNBOOK S06)" ufw; done; fi
  else warn "ufw state not read (no ufw, or sudo needs a password): run 'sudo ufw status'"; fi
  for n in node1 node2 node3 node4; do
    ip=$(node_ip $n); [ "$n" = "$NODE_NAME" ] && continue
    if ! ping -c1 -W2 "$ip" >/dev/null 2>&1; then fail "ping $n ($ip) fails" "ZeroTier up and authorized on both sides, /etc/hosts" ping; continue; fi
    up=""; down=""; closed=""
    for p in $(ports_of $n); do
      r=$(probe "$ip" "$p")
      case $r in open) up="$up $p" ;; refused) closed="$closed $p" ;; *) down="$down $p" ;; esac
    done
    ok "ping $n; reachable ports:${up:- none}"
    [ -n "$closed" ] && note "$n ports with no listener yet (route is fine, skipped):$closed"
    [ -n "$down" ] && fail "$n ports TIMED OUT (something drops them):$down" "on $n: sudo ufw allow from $MY_IP ; node3 on WSL2: ZeroTier inside Ubuntu (TROUBLESHOOTING.md#wsl2)" ufw
  done
}

# ---------- stage: hdfs (any node: NameNode, 2 DataNodes, folders, a replicated test file) ----------
stage_hdfs() {
  local report live ips want out d f got repl
  if hdfs_up; then ok "NameNode answers on node2:9000"
  else fail "nothing answers on node2:9000" "on node2: hdfs --daemon start namenode (RUNBOOK S19)" refused-9000; return; fi
  if [ "$NODE_NAME" = node2 ]; then
    ss -tln | awk '$4 ~ /:9000$/ {print $4}' | grep -qE '^(127\.|\[?::1|\[::ffff:127)' \
      && fail "the NameNode listens on loopback only" "fix /etc/hosts (RUNBOOK S05), restart the NameNode" dn-ip || ok "NameNode listens on a non-loopback address"
  fi
  report=$(hdfs dfsadmin -report 2>&1)
  live=$(echo "$report" | grep -m1 '^Live datanodes' | grep -o '[0-9]\+' | head -1)
  ips=$(echo "$report" | awk '/^Live datanodes/ {f=1; next} /^(Dead|Decommission[a-z]*) datanodes/ {f=0} f && /^Name:/ {split($2, a, ":"); print a[1]}' | sort | tr '\n' ' ')
  want=$(printf '%s\n%s\n' "$NODE2_IP" "$NODE3_IP" | sort | tr '\n' ' ')
  if [ "${live:-0}" = 2 ]; then ok "2 live DataNodes"; else fail "live DataNodes: ${live:-0} (need 2)" "start the missing DataNode: hdfs --daemon start datanode on node2 / node3 (RUNBOOK S20, S21)" dn-missing; fi
  [ "$ips" = "$want" ] && ok "DataNodes are $NODE2_IP and $NODE3_IP" || fail "DataNode addresses are '${ips:-none}', want '$want'" "fix /etc/hosts on that node (RUNBOOK S05), restart its DataNode" dn-ip
  hdfs dfsadmin -safemode get 2>&1 | grep -q 'OFF' && ok "safe mode OFF" || fail "safe mode is ON" "wait 30 s; both DataNodes must report" safemode
  out=$(hdfs dfs -ls -d /search/input/phase-0 /search/input/phase-1 /serving /warehouse /spark-logs 2>&1)
  for d in /search/input/phase-0 /search/input/phase-1 /serving /warehouse /spark-logs; do
    echo "$out" | grep -q " $d\$" && ok "HDFS folder $d" || fail "HDFS folder $d missing" "hdfs dfs -mkdir -p $d (RUNBOOK S19, on node2)" safemode
  done
  f="/search/.check-$NODE_NAME-$$"
  if echo "gate check $(date +%s)" | hdfs dfs -put -f - "$f" 2>/dev/null; then
    got=$(hdfs dfs -cat "$f" 2>/dev/null); repl=$(hdfs fsck "$f" -files -blocks 2>/dev/null | grep -o 'Live_repl=[0-9]*' | head -1)
    hdfs dfs -rm -skipTrash "$f" >/dev/null 2>&1
    [ -n "$got" ] && ok "test file written and read back through HDFS" || fail "test file written but not readable" "check DataNode port 9866 from this node" minreplication
    [ "$repl" = Live_repl=2 ] && ok "test file replication: $repl" || fail "test file replication is '${repl:-unknown}', must be Live_repl=2" "both DataNodes live? dfs.replication=2 in this node's config" minreplication
  else fail "cannot write a test file to HDFS" "DataNodes reachable on 9866 from this node? safe mode?" minreplication; fi
}

# ---------- stage: spark (nodes 1-3) ----------
stage_spark() {
  local json st n hosts apps want
  [ "$(probe "$NODE3_IP" 7077)" = open ] && ok "Spark master answers on node3:7077" || { fail "nothing answers on node3:7077" "on node3: start-master.sh (RUNBOOK S23)" master-down; return; }
  [ "$NODE_NAME" = node3 ] && ss -tln | awk '$4 ~ /:7077$/ {print $4}' | grep -qE '^(127\.|\[?::1|\[::ffff:127)' \
    && fail "the master listens on loopback only" "fix /etc/hosts and SPARK_MASTER_HOST, restart the master" dn-ip
  json=$(curl -s --max-time 5 "http://$NODE3_IP:8080/json/")      # the trailing slash matters
  st=$(echo "$json" | python3 -c '
import json, sys
d = json.load(sys.stdin); w = d["workers"]
print("%s|%d|%s|%d" % (d["status"], len(w), ",".join(sorted(x["host"] for x in w if x["state"] == "ALIVE")), len(d.get("activeapps", []))))' 2>/dev/null)
  if [ -z "$st" ]; then fail "master UI json unreadable at http://$NODE3_IP:8080/json/" "master running? (trailing slash needed)" master-down; return; fi
  IFS='|' read -r st n hosts apps <<<"$st"
  want=$(printf '%s\n%s\n' "$NODE2_IP" "$NODE3_IP" | sort | tr '\n' ',' | sed 's/,$//')
  [ "$st" = ALIVE ] && ok "master status ALIVE" || fail "master status is $st" "restart the master" master-down
  [ "$hosts" = "$want" ] && ok "2 ALIVE workers: $hosts" || fail "ALIVE workers: '${hosts:-none}', want '$want'" "start-worker.sh spark://$NODE3_IP:7077 on node2 / node3 (RUNBOOK S23, S24)" one-worker
  [ "$apps" = 0 ] && ok "no application is running" || warn "$apps Spark application(s) RUNNING: one job at a time (4 cores)"
  [ "$(probe "$NODE3_IP" 18080)" = open ] && ok "history server answers on node3:18080" || warn "history server not running on node3:18080 (optional: start-history-server.sh on node3)"
}

# ---------- stages: ingest0 / ingest1 / index (any node; read Iceberg metadata from HDFS, no Spark job) ----------
need_hdfs() { hdfs_up || { fail "NameNode not reachable" "run the hdfs gate first (RUNBOOK S22)" refused-9000; return 1; }; }
stage_ingest0() {   # phase 0 happened: input in HDFS + the phase-0 snapshots/rejects exist (still true after phase 1)
  need_hdfs || return
  expect "phase-0 input lines in HDFS" "$(lines_of /search/input/phase-0/pages.jsonl)" 1040 "RUNBOOK S29" minreplication
  ever_had docs 1000 && ok "docs table has a snapshot with 1000 rows" || fail "no docs snapshot with 1000 rows" "run RUNBOOK S30 and read state/logs/ingest-p0.log" table-not-found
  ever_had links 499 && ok "links table has a snapshot with 499 rows" || fail "no links snapshot with 499 rows" "RUNBOOK S30" table-not-found
  ever_had rejects 40 && ok "rejects recorded for phase 0 (20 unparseable + 20 duplicate = 40)" || fail "no rejects snapshot with 40 rows" "RUNBOOK S30" table-not-found
}
stage_ingest1() {   # phase 1 happened: this is the state NOW
  need_hdfs || return
  expect "phase-1 input lines in HDFS" "$(lines_of /search/input/phase-1/changes.jsonl)" 100 "RUNBOOK S41" minreplication
  table_is docs 975; table_is links 467; table_is rejects 47
}
stage_index() {     # index + PageRank match the docs table (1000 -> 233643 postings, 975 -> 220622)
  local docs post
  need_hdfs || return
  docs=$(total_of docs); post=$(total_of postings)
  table_is doc_stats "$docs"; table_is pagerank "$docs"
  case $docs in
    1000) expect "postings (phase 0)" "$post" 233643 "run build_index.py --phase 0 (RUNBOOK S33)" table-not-found ;;
    975)  expect "postings (phase 1)" "$post" 220622 "run build_index.py --phase 1 (RUNBOOK S44)" table-not-found ;;
    *)    warn "docs has $docs rows (not a known phase): postings = ${post:-?}" ;;
  esac
}

# ---------- stages: export <vN> (HDFS) and pulled <vN> (node4 disk) ----------
manifest_check() {  # manifest_check <label> <vN> <command that prints manifest.json...>
  local label=$1 v=$2 line st n p c w t exp; shift 2
  line=$("$@" 2>/dev/null | python3 -c '
import json, sys
m = json.load(sys.stdin); c = m["counts"]; k = m["checks"]
print("%s|%d|%d|%s|%s|%s" % (m["status"], len(k), sum(1 for x in k if x["ok"]), c["docs"], c["postings"], c["terms"]))' 2>/dev/null)
  if [ -z "$line" ]; then fail "$label: manifest.json missing or unreadable" "export again (RUNBOOK S35/S46) or pull again (S37/S48)" export-bad; return; fi
  IFS='|' read -r st n p c w t <<<"$line"
  [ "$st" = OK ] && ok "$label: manifest status OK" || fail "$label: manifest status is $st" "read the export log in state/logs/export-$v.log" export-bad
  [ "$n" = 13 ] && [ "$p" = 13 ] && ok "$label: $p/$n checks passed" || fail "$label: $p/$n checks passed (want 13/13)" "export again" export-bad
  case $v in v0) exp="1000|233643|37551" ;; v1) exp="975|220622|36521" ;; *) exp="" ;; esac
  if [ -z "$exp" ]; then note "$label: docs/postings/terms = $c/$w/$t"
  elif [ "$exp" = "$c|$w|$t" ]; then ok "$label: docs/postings/terms = $c/$w/$t"
  else fail "$label: docs/postings/terms = $c/$w/$t, want ${exp//|//}" "export again from the right phase" export-bad; fi
}
stage_export() {
  local v=$1
  need_hdfs || return
  hdfs dfs -test -e "/serving/$v/_READY" && ok "/serving/$v/_READY exists" || fail "/serving/$v/_READY missing" "export did not finish: RUNBOOK S35 / S46" export-bad
  manifest_check "HDFS export $v" "$v" hdfs dfs -cat "/serving/$v/manifest.json"
}
stage_pulled() {
  local v=$1 d="$SERVING_DIR/$1"
  [ -d "$d" ] && ok "$d exists" || { fail "$d does not exist on this node" "pull it: RUNBOOK S37 / S48" serve; return; }
  [ -f "$d/_READY" ] && ok "$v/_READY present" || fail "$v/_READY missing" "pull again" serve
  manifest_check "pulled $v" "$v" cat "$d/manifest.json"
  note "CURRENT = $(cat "$SERVING_DIR/CURRENT" 2>/dev/null || echo none)"
}

# ---------- stage: serve [vN] (HTTP to node4:8000, works from any node) ----------
stage_serve() {
  local want=$1 base="http://$NODE4_IP:8000" h st v docs exp q m title
  h=$(curl -s --max-time 5 "$base/api/health") || h=""
  if [ -z "$h" ]; then fail "no answer from $base/api/health" "on node4: start the service (RUNBOOK S28; cd code/node4_search first)" serve; return; fi
  IFS='|' read -r st v docs <<<"$(echo "$h" | python3 -c 'import json, sys; d = json.load(sys.stdin); print("%s|%s|%s" % (d["status"], d["version"], d["docs"]))' 2>/dev/null)"
  [ "$st" = ok ] && ok "health: serving $v with $docs docs" || { fail "health says '${st:-unreadable}': no version loaded" "pull an export: RUNBOOK S37 / S48" serve; return; }
  [ -n "$want" ] && { [ "$v" = "$want" ] && ok "serving the wanted version $want" || fail "serving $v, wanted $want" "pull $want or switch (RUNBOOK S56)" serve; }
  case $v in v0) exp=1000 ;; v1) exp=975 ;; *) exp="" ;; esac
  [ -z "$exp" ] || expect "docs in $v" "$docs" "$exp" "pull the export again" serve
  q=$(curl -s --max-time 10 "$base/api/search?q=ocean&k=3")
  IFS='|' read -r m title <<<"$(echo "$q" | python3 -c 'import json, sys; d = json.load(sys.stdin); print("%s|%s" % (d["matches"], d["results"][0]["title"]))' 2>/dev/null)"
  [ "${m:-0}" -gt 0 ] 2>/dev/null && ok "query 'ocean': $m matches, top: $title" || fail "query 'ocean' returned no results" "wrong or empty export loaded; pull again" serve
}

# ---------- stage: down (end of the session) ----------
stage_down() {
  local pid name any=0
  while read -r pid name; do
    case $name in NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager|Master|Worker|HistoryServer)
      fail "$name ($pid) is still running" "stop it with its own command (RUNBOOK S67-S69)" port-in-use ;; esac
  done < <(jps 2>/dev/null)
  pgrep -f 'uvicorn app:app' >/dev/null && fail "the search service (uvicorn) is still running" "Ctrl-C in terminal A (RUNBOOK S67)" port-in-use
  [ "$PROBLEMS" = 0 ] && ok "no Hadoop/Spark daemon and no search service running on $NODE_NAME"
}

# ---------- run one stage, print its GATE line ----------
run() {   # run <stage> [arg]
  local st=$1 arg=${2:-}
  PROBLEMS=0; echo; echo "== check $st${arg:+ $arg} on ${NODE_NAME:-?} =="
  "stage_$st" "$arg"
  if [ "$PROBLEMS" = 0 ]; then echo "GATE $st${arg:+ $arg} PASSED"; else echo "GATE $st${arg:+ $arg} FAILED ($PROBLEMS problems)"; echo "      next: bash ~/bd_project/scripts/doctor.sh  and paste the output in the group chat"; TOTAL=$((TOTAL + PROBLEMS)); fi
}
stage_all() {   # whatever applies to this node and to how far the pipeline has come
  local v
  run setup; run network; run hdfs
  has_spark && run spark
  if hdfs_up && hdfs dfs -test -d /warehouse/search/docs 2>/dev/null; then
    run ingest0; [ "$(total_of docs)" = 975 ] && run ingest1
    run index
    for v in v0 v1; do hdfs dfs -test -e "/serving/$v/_READY" 2>/dev/null && run export "$v"; done
  fi
  if [ "$NODE_NAME" = node4 ]; then
    for v in v0 v1; do [ -d "$SERVING_DIR/$v" ] && run pulled "$v"; done
  fi
  [ "$(probe "$NODE4_IP" 8000)" = open ] && run serve
}

# ---------- main ----------
STAGE=${1:-}; ARG=${2:-}
usage() { sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
refuse() { echo "GATE $STAGE NOT RUN: $1"; exit 2; }
case $STAGE in
  setup|network|hdfs|ingest0|ingest1|index|down|all|spark|serve|export|pulled) ;;
  *) usage ;;
esac
[ "$STAGE" = setup ] || [ -n "${NODE_NAME:-}" ] || refuse "cannot tell which node this is (no team.env IP on this machine); run: bash scripts/check.sh setup"
[ "$STAGE" = spark ] && ! has_spark && refuse "node4 has no Spark; check Spark on node1, node2 or node3"
[ "$STAGE" = pulled ] && [ "$NODE_NAME" != node4 ] && refuse "pulled versions live on node4 (state/serving); run it there"
case $STAGE in export|pulled) [ -n "$ARG" ] || refuse "which version? e.g.: bash scripts/check.sh $STAGE v0" ;; esac
if [ "$STAGE" = all ]; then
  stage_all; echo
  if [ "$TOTAL" = 0 ]; then echo "GATE all PASSED"; else echo "GATE all FAILED ($TOTAL problems)"; fi
else
  run "$STAGE" "$ARG"
fi
[ "$TOTAL" = 0 ]
