#!/usr/bin/env bash
# bd setup - prepare THIS machine for the project. Safe to run again at any time.
# Order: prerequisites -> venv -> Iceberg jar -> Hadoop/Spark config -> /etc/hosts (+WSL) -> ufw -> MTU -> leftovers.
set -euo pipefail
BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; export BD_HOME BD_SCRIPT=1
# shellcheck source=env.sh
. "$BD_HOME/scripts/env.sh"
bd_need_detected

NUM="$(bd_nodenum)"
ROLE_SPARK=1; [ "$BD_NODE" = node4 ] && ROLE_SPARK=0
PY310=/usr/bin/python3.10
JAR_NAME=iceberg-spark-runtime-3.3_2.12-1.8.1.jar
JAR_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.8.1/$JAR_NAME"
JAR_SHA1=43d0915f3eeec9b732307aee8334f5a84843e568
BLOCK_BEGIN="# >>> bd_project"; BLOCK_END="# <<< bd_project"
TODO=()                                   # things the user must still do by hand
todo() { TODO+=("$*"); }

# ---------------------------------------------------------------------------------------------
check_prereqs() {
  step "1. Prerequisites on $BD_NODE ($MY_IP)"
  if java -version 2>&1 | head -1 | grep -q '"11\.'; then ok "Java 11"
  else fail "Java 11 missing"; hint "sudo apt install -y openjdk-11-jdk-headless   and set JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"; PREREQ_BAD=1; fi
  if command -v hadoop >/dev/null && hadoop version 2>/dev/null | head -1 | grep -q 'Hadoop 3.3.6'; then ok "Hadoop 3.3.6 (${HADOOP_HOME:-})"
  else fail "Hadoop 3.3.6 not found (needs /opt/hadoop or HADOOP_HOME, and 'hdfs' on PATH)"; hint "install it as in the course Hadoop guide; then open a new terminal"; PREREQ_BAD=1; fi
  command -v hdfs >/dev/null && ok "hdfs on PATH" || { fail "hdfs not on PATH"; hint "add /opt/hadoop/bin to PATH in ~/.bashrc"; PREREQ_BAD=1; }
  if [ "$ROLE_SPARK" = 1 ]; then
    if command -v spark-submit >/dev/null && spark-submit --version 2>&1 | grep -q 'version 3.3.4'; then ok "Spark 3.3.4 (${SPARK_HOME:-})"
    else fail "Spark 3.3.4 not found (needs /opt/spark or SPARK_HOME)"; hint "install it as in the course Spark guide"; PREREQ_BAD=1; fi
    ensure_python310
  else
    command -v python3 >/dev/null && ok "python3 $(python3 -V 2>&1 | cut -d' ' -f2)" || { fail "python3 missing"; PREREQ_BAD=1; }
  fi
  if command -v curl >/dev/null || command -v wget >/dev/null; then ok "curl/wget"
  else fail "neither curl nor wget"; hint "sudo apt install -y curl"; PREREQ_BAD=1; fi
}

ensure_python310() {
  local found
  if [ -x "$PY310" ]; then ok "$PY310 ($($PY310 -V 2>&1))"; return; fi
  found="$(command -v python3.10 || true)"
  [ -z "$found" ] && [ -x /usr/local/bin/python3.10 ] && found=/usr/local/bin/python3.10
  if [ -n "$found" ]; then
    warn "python3.10 is at $found, but executors use the driver's path $PY310 on every node"
    if bd_sudo ln -s "$found" "$PY310"; then ok "linked $PY310 -> $found"; else todo "sudo ln -s $found $PY310"; PREREQ_BAD=1; fi
  else
    fail "Python 3.10 not found"; hint "sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt install -y python3.10   (Spark 3.3.4 needs <= 3.10)"; PREREQ_BAD=1
  fi
}

# ---------------------------------------------------------------------------------------------
make_venv() {
  step "2. Python venv ($BD_HOME/venv)"
  local v="$BD_HOME/venv"
  if [ ! -x "$v/bin/python" ]; then
    rm -rf "$v"
    if [ "$ROLE_SPARK" = 1 ]; then
      [ -x "$PY310" ] && "$PY310" -m venv --without-pip "$v" || { fail "could not create venv"; PREREQ_BAD=1; return; }
    else
      if ! python3 -m venv "$v" 2>/dev/null; then
        rm -rf "$v"; fail "python3 -m venv failed"; hint "sudo apt install -y python3-venv   then run bd setup again"; PREREQ_BAD=1; return
      fi
    fi
  fi
  ok "venv exists"
  if [ "$BD_NODE" = node4 ]; then
    if "$v/bin/pip" install -q -r "$BD_HOME/code/node4_search/requirements.txt"; then
      "$v/bin/python" -c "import fastapi, uvicorn, pandas, pyarrow; print('deps ok')" | sed 's/^/          /'
      ok "node4 requirements installed"
    else fail "pip install failed (internet?)"; hint "re-run bd setup when online"; PREREQ_BAD=1; fi
  fi
  local line="source \"$BD_HOME/scripts/env.sh\"   # bd_project env"
  if ! grep -q "# bd_project env" "$v/bin/activate"; then
    printf '\n%s\n' "$line" >> "$v/bin/activate"; ok "activate now loads scripts/env.sh"
  else ok "activate already loads scripts/env.sh"; fi
}

# ---------------------------------------------------------------------------------------------
get_jar() {
  step "3. Iceberg jar"
  local dst="$BD_STATE/jars/$JAR_NAME"
  mkdir -p "$BD_STATE/jars"
  if [ ! -f "$dst" ]; then
    if [ -f "/opt/iceberg/$JAR_NAME" ]; then cp "/opt/iceberg/$JAR_NAME" "$dst"
    elif command -v curl >/dev/null; then curl -fL --retry 3 -o "$dst" "$JAR_URL" || rm -f "$dst"
    elif command -v wget >/dev/null; then wget -q -O "$dst" "$JAR_URL" || rm -f "$dst"; fi
  fi
  if [ -f "$dst" ] && [ "$(sha1sum "$dst" | cut -d' ' -f1)" = "$JAR_SHA1" ]; then ok "$dst (sha1 ok)"
  else rm -f "$dst"; fail "Iceberg jar missing or wrong checksum"; hint "download $JAR_URL to $dst (needs internet)"; PREREQ_BAD=1; fi
}

# ---------------------------------------------------------------------------------------------
write_hadoop_conf() {
  step "4. Hadoop config -> state/conf/hadoop (the course's /opt/hadoop/etc/hadoop is not touched)"
  local c="$BD_STATE/conf/hadoop" src="${HADOOP_HOME:-/opt/hadoop}/etc/hadoop"
  [ -d "$src" ] || { fail "$src not found"; PREREQ_BAD=1; return; }
  mkdir -p "$c" "$BD_STATE/logs/hadoop" "$BD_STATE/pids/hadoop" "$BD_STATE/hdfs"
  cp -rf "$src/." "$c/"
  cat > "$c/core-site.xml" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://node2:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$BD_STATE/hdfs/tmp</value></property>
</configuration>
XML
  cat > "$c/hdfs-site.xml" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$BD_STATE/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$BD_STATE/hdfs/datanode</value></property>
  <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
  <property><name>dfs.namenode.heartbeat.recheck-interval</name><value>30000</value></property>
  <property><name>dfs.client.block.write.replace-datanode-on-failure.policy</name><value>NEVER</value></property>
</configuration>
XML
  cat >> "$c/hadoop-env.sh" <<ENV

$BLOCK_BEGIN
export JAVA_HOME="\${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HEAPSIZE_MAX=512m
export HADOOP_LOG_DIR="$BD_STATE/logs/hadoop"
export HADOOP_PID_DIR="$BD_STATE/pids/hadoop"
$BLOCK_END
ENV
  ok "core-site.xml: fs.defaultFS = hdfs://node2:9000, replication 2, fresh dirs in state/hdfs"
}

write_spark_conf() {
  step "5. Spark config -> state/conf/spark"
  local c="$BD_STATE/conf/spark" n3 tmpl
  n3="$(bd_ip node3)"
  mkdir -p "$c" "$BD_STATE/logs/spark" "$BD_STATE/pids/spark" "$BD_STATE/derby"
  cat > "$c/spark-env.sh" <<ENV
# generated by bd setup - do not edit (re-run bd setup instead)
export JAVA_HOME="\${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export SPARK_LOCAL_IP=$MY_IP
export SPARK_PUBLIC_DNS=$MY_IP
export SPARK_MASTER_HOST=$n3
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1536m
export SPARK_DAEMON_MEMORY=512m
export SPARK_WORKER_PORT=40020
export PYSPARK_PYTHON=$PY310
export SPARK_LOG_DIR="$BD_STATE/logs/spark"
export SPARK_PID_DIR="$BD_STATE/pids/spark"
ENV
  tmpl="${SPARK_HOME:-/opt/spark}/conf/log4j2.properties.template"
  if [ -f "$tmpl" ]; then sed 's/^rootLogger.level = info/rootLogger.level = warn/' "$tmpl" > "$c/log4j2.properties"; fi
  cat > "$c/spark-defaults.conf" <<CONF
# generated by bd setup - do not edit (re-run bd setup instead)
spark.master                              spark://$n3:7077
spark.driver.host                         $MY_IP
spark.driver.bindAddress                  0.0.0.0
spark.driver.port                         40000
spark.driver.blockManager.port            40001
spark.blockManager.port                   40010
spark.port.maxRetries                     8
spark.driver.memory                       768m
spark.executor.memory                     1g
spark.executor.cores                      2
spark.cores.max                           4
spark.sql.shuffle.partitions              8
spark.default.parallelism                 8
spark.network.timeout                     300s
spark.executor.heartbeatInterval          30s
spark.pyspark.python                      $PY310
spark.pyspark.driver.python               $PY310
spark.hadoop.fs.defaultFS                 hdfs://node2:9000
spark.hadoop.dfs.replication              2
spark.sql.extensions                      org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg_catalog         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type    hadoop
spark.sql.catalog.iceberg_catalog.warehouse  hdfs://node2:9000/warehouse
spark.jars                                $BD_STATE/jars/$JAR_NAME
spark.eventLog.enabled                    true
spark.eventLog.dir                        hdfs://node2:9000/spark-logs
spark.history.fs.logDirectory             hdfs://node2:9000/spark-logs
spark.driver.extraJavaOptions             -Dderby.system.home=$BD_STATE/derby
spark.ui.showConsoleProgress              false
CONF
  ok "spark-defaults.conf: master spark://$n3:7077, driver host $MY_IP (ports 40000/40001), executors 1g x 2 cores"
}

# ---------------------------------------------------------------------------------------------
write_hosts() {
  step "6. /etc/hosts"
  local tmp names out
  tmp="$(mktemp)"; names="node1 node2 node3 node4"
  awk -v host="$(hostname)" -v names="$names" -v b="$BLOCK_BEGIN" -v e="$BLOCK_END" '
    BEGIN { n = split(names, a, " "); for (i = 1; i <= n; i++) bad[a[i]] = 1; if (host != "localhost") bad[host] = 1 }
    $0 ~ "^# >>> (team32|bd_project)" { skip = 1; next }
    $0 ~ "^# <<< (team32|bd_project)" { skip = 0; next }
    skip { next }
    /^[[:space:]]*#/ || NF == 0 { print; next }
    $1 == "127.0.1.1" { print "#" $0; next }
    { out = $1; kept = 0; changed = 0
      for (i = 2; i <= NF; i++) { if ($i in bad) changed = 1; else { out = out " " $i; kept++ } }
      if (!changed) print; else if (kept == 0) print "#" $0; else print out }
  ' /etc/hosts > "$tmp"
  {
    echo "$BLOCK_BEGIN"
    for i in 1 2 3 4; do
      line="$(bd_ip node$i)  node$i"
      if [ "node$i" = "$BD_NODE" ] && [ "$(hostname)" != "$BD_NODE" ] && [ "$(hostname)" != localhost ]; then line="$line $(hostname)"; fi
      echo "$line"
    done
    echo "$BLOCK_END"
  } >> "$tmp"
  if cmp -s "$tmp" /etc/hosts; then ok "/etc/hosts already correct"
  else
    [ -f /etc/hosts.before-bd ] || bd_sudo cp /etc/hosts /etc/hosts.before-bd || true
    if bd_sudo cp "$tmp" /etc/hosts; then ok "/etc/hosts updated (backup: /etc/hosts.before-bd)"
    else todo "sudo cp $tmp /etc/hosts      (then re-run bd setup)"; fi
  fi
  rm -f "$tmp" 2>/dev/null || true
  for i in 1 2 3 4; do
    got="$(getent hosts "node$i" | awk '{print $1; exit}')" || true
    [ "$got" = "$(bd_ip node$i)" ] && ok "node$i -> $got" || { fail "node$i resolves to '${got:-nothing}' (want $(bd_ip node$i))"; PREREQ_BAD=1; }
  done
  got="$(getent hosts "$(hostname)" | awk '{print $1; exit}')" || true
  [ "$got" = "$MY_IP" ] && ok "$(hostname) -> $got (not 127.0.1.1)" || warn "$(hostname) resolves to '${got:-nothing}', expected $MY_IP"
}

wsl_setup() {
  bd_is_wsl || return 0
  step "7. WSL2"
  local conf=/etc/wsl.conf new
  new="$(mktemp)"
  python3 - "$conf" "$new" <<'PY'
import configparser, sys
c = configparser.ConfigParser(); c.optionxform = str
c.read(sys.argv[1])
for sec, key, val in (("boot", "systemd", "true"), ("network", "generateHosts", "false")):
    if not c.has_section(sec): c.add_section(sec)
    c.set(sec, key, val)
with open(sys.argv[2], "w") as f: c.write(f)
PY
  if [ -f "$conf" ] && cmp -s "$new" "$conf"; then ok "/etc/wsl.conf already has systemd=true and generateHosts=false"
  elif bd_sudo cp "$new" "$conf"; then
    ok "wrote /etc/wsl.conf (systemd=true, generateHosts=false)"
    todo "In Windows PowerShell run:  wsl --shutdown   then open Ubuntu again (once, so wsl.conf takes effect), then bd setup again"
  else todo "sudo cp $new $conf"; fi
  rm -f "$new"
  [ -c /dev/net/tun ] && ok "/dev/net/tun exists (ZeroTier can run inside WSL)" || { fail "/dev/net/tun missing"; hint "needs a WSL2 kernel with TUN: wsl --update (PowerShell)"; }
  if command -v zerotier-cli >/dev/null; then ok "zerotier-one installed in this Ubuntu"
  else
    warn "ZeroTier is not installed inside this Ubuntu. node3 on WSL2 needs it HERE, not only on Windows:"
    hint "curl -s https://install.zerotier.com | sudo bash"
    hint "sudo zerotier-cli join <network id>   -> authorize the new member in ZeroTier Central"
    hint "set its IP to $(bd_ip node3) by hand, delete the old Windows member of node3, then: bd setup"
    hint "details: TROUBLESHOOTING.md#wsl2"
  fi
}

# ---------------------------------------------------------------------------------------------
firewall() {
  step "8. Firewall (ufw)"
  if ! command -v ufw >/dev/null; then ok "ufw not installed: nothing to open"; return; fi
  local st
  st="$( { if [ "$(id -u)" = 0 ]; then ufw status; else sudo ufw status; fi; } 2>/dev/null | head -1 )" || true
  case "$st" in
    *inactive*) ok "ufw is inactive: nothing to open"; return ;;
    "") warn "cannot read ufw status (no sudo?)"; hint "if ufw is active run: sudo ufw allow from <each other node's IP>"; return ;;
  esac
  local i
  for i in 1 2 3 4; do
    [ "node$i" = "$BD_NODE" ] && continue
    bd_sudo ufw allow from "$(bd_ip node$i)" >/dev/null && ok "ufw allow from node$i ($(bd_ip node$i))" || todo "sudo ufw allow from $(bd_ip node$i)"
  done
}

set_mtu() {
  [ -n "${ZT_MTU:-}" ] || return 0
  step "9. ZeroTier MTU"
  local ifc; ifc="$(bd_iface_of "$MY_IP")"
  [ -n "$ifc" ] || { warn "no interface holds $MY_IP"; return; }
  if bd_sudo ip link set dev "$ifc" mtu "$ZT_MTU"; then ok "MTU of $ifc = $ZT_MTU (not kept after reboot: run bd setup again)"; else todo "sudo ip link set dev $ifc mtu $ZT_MTU"; fi
}

leftovers() {
  step "10. Leftovers from the course / old guides"
  [ -d "$HOME/solo" ] && { warn "~/solo exists (old solo practice)"; hint "rm -rf ~/solo"; }
  local f
  for f in ~/.bashrc ~/.profile ~/.bash_profile; do
    if [ -f "$f" ] && grep -qE '^[[:space:]]*export[[:space:]]+SPARK_LOCAL_(IP|HOSTNAME)' "$f"; then
      warn "$f exports SPARK_LOCAL_IP/HOSTNAME (old guides). It would bind Spark to the wrong address"
      hint "remove those lines:  sed -i '/SPARK_LOCAL_\\(IP\\|HOSTNAME\\)/d' $f   then open a new terminal"
    fi
  done
  [ -d "$HOME/hadoop-data" ] && warn "~/hadoop-data (course HDFS) exists: ignored, we use state/hdfs. Delete it later if you need space."
  if [ -n "$(bd_old_daemons)" ]; then
    warn "course daemons are running (not started by bd): $(bd_old_daemons | tr '\n' ' ')"
    hint "bd stop --old      (bd start does this for you too)"
  else ok "no course daemons running"; fi
}

# ---------------------------------------------------------------------------------------------
PREREQ_BAD=0
check_prereqs
make_venv
[ "$ROLE_SPARK" = 1 ] && get_jar
write_hadoop_conf
[ "$ROLE_SPARK" = 1 ] && write_spark_conf
[ "$BD_NODE" = node4 ] && { mkdir -p "$BD_STATE/serving"; ok "serving dir $BD_STATE/serving"; }
write_hosts
wsl_setup
firewall
set_mtu
leftovers

step "Summary for $BD_NODE"
if [ "${#TODO[@]}" -gt 0 ]; then
  warn "do these by hand, then run bd setup again:"
  for t in "${TODO[@]}"; do hint "$t"; done
fi
if [ "$PREREQ_BAD" = 0 ] && [ "${#TODO[@]}" = 0 ]; then
  ok "setup finished"
  echo "  Next (RUNBOOK B1):  source $BD_HOME/venv/bin/activate   then   bd doctor"
else
  fail "setup is not complete: fix the lines marked FAIL/WARN above, then run bd setup again"
  exit 1
fi
