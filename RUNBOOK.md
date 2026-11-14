# RUNBOOK: the call script

One ordered timeline. Read top to bottom; do the step whose heading names your node, in order. Nothing is explained here: what each step does and what you must say about it is in [VIVA.md](VIVA.md) (per member, by step id). Stuck: run `bash ~/bd_project/scripts/doctor.sh` and paste the output in the chat, then [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

- **Terminal opener** (first thing in every terminal, on every node; works after S04):
  ```bash
  source ~/bd_project/venv/bin/activate
  cd ~/bd_project
  ```
- **GATE** steps: run the named `check.sh <stage>` on the named nodes. It is read-only. Nobody continues until every named node prints `GATE <stage> PASSED`. If FAIL, it prints the fix and the TROUBLESHOOTING anchor.
- "say in chat" = write that sentence in the group chat; the next person waits for it. `(parallel with Sxx)` = different nodes do these at the same time.
- 📸 = screenshot for the report. Job logs: `~/bd_project/state/logs/`.

## Quick index: my steps

| Member | Node | My step ids (gates in **bold**) |
|---|---|---|
| M1 | node1 | S01, S03, S04, S05, S06, S07, S08, S09, S10, **S12**, S12a, S14, S15, **S17**, **S22**, **S25**, S26, S29, S30, S31, **S32**, S41, S42, **S43**, **S66**, **S70** |
| M2 | node2 | S01, S03, S04, S05, S06, S07, S08, S09, S10, S11, **S12**, S12a, S14, S15, **S17**, S18, S19, S20, **S22**, S24, **S25**, **S32**, **S36**, S39, **S43**, **S47**, S49, S52, S57, S59, S62, S64, **S66**, S69, **S70** |
| M3 | node3 | S01, S02, S03, S04, S05, S06, S07, S08, S09, S10, S11, **S12**, S12a, S13, S16, **S17**, S21, **S22**, S23, **S25**, S27, S33, S34, S35, **S36**, S44, S45, S46, **S47**, S51, S53, S54, S60, S61, S63, S65, **S66**, S68, **S70** |
| M4 | node4 | S01, S03, S04, S05, S06, S07, S09, **S12**, S12a, S14, **S17**, **S22**, S28, S37, S38, **S40**, S48, **S50**, S55, S56, S58, **S66**, S67, **S70** |

---

## STAGE 0 · setup (before the call, needs internet)

### S01 · all nodes · Clone the repo, check the course installs
```bash
git clone https://github.com/Greninja34/bd_project.git ~/bd_project
java -version 2>&1 | head -1                         # openjdk version "11.x"
hadoop version | head -1                             # Hadoop 3.3.6
spark-submit --version 2>&1 | grep -m1 "version"     # 3.3.4         (nodes 1-3 only)
/usr/bin/python3.10 --version                        # Python 3.10.x (nodes 1-3 only)
sudo apt install -y git python3-venv netcat-openbsd  # node4 needs git + python3-venv; everyone needs nc
```
✅ expect: the four versions above. If python3.10 is at `/usr/local/bin`: `sudo ln -s /usr/local/bin/python3.10 /usr/bin/python3.10`.

### S02 · node3 (Member 3) · WSL2: ZeroTier inside Ubuntu
```bash
cat /etc/wsl.conf        # if it has other settings, add the two sections below by hand instead of overwriting
sudo tee /etc/wsl.conf >/dev/null <<'EOF'
[boot]
systemd=true
[network]
generateHosts=false
EOF
```
Windows PowerShell: `wsl --shutdown`, then open Ubuntu again, and in Ubuntu:
```bash
ls -l /dev/net/tun                                   # must exist (else: wsl --update in PowerShell)
curl -s https://install.zerotier.com | sudo bash
sudo zerotier-cli join <network id>
```
ZeroTier Central: authorize the new member, set its IP by hand to `172.23.10.141`, delete the old Windows member of node3. Keep one Ubuntu terminal open the whole session.
✅ expect: `ip -4 -br a` shows `172.23.10.141`. ❌ [TROUBLESHOOTING#wsl2](TROUBLESHOOTING.md#wsl2)

### S03 · all nodes · ZeroTier is up
```bash
ip -4 -br a                                          # your ZeroTier IP from team.env must be listed
sudo zerotier-cli info
sudo zerotier-cli peers | grep LEAF
cat ~/bd_project/team.env
```
✅ expect: your IP listed; peers `DIRECT` (`RELAY` works but is slow); the 10-digit node IDs of the 4 machines all differ. ❌ [zt-ip](TROUBLESHOOTING.md#zt-ip) · [ping](TROUBLESHOOTING.md#ping) · [dup-id](TROUBLESHOOTING.md#dup-id)

### S04 · all nodes · Environment file and venv
```bash
source ~/bd_project/scripts/env.sh
echo "I am $NODE_NAME with $MY_IP"
# nodes 1, 2, 3:
/usr/bin/python3.10 -m venv --without-pip ~/bd_project/venv
# node 4 instead:
python3 -m venv ~/bd_project/venv
~/bd_project/venv/bin/pip install -r ~/bd_project/code/node4_search/requirements.txt
~/bd_project/venv/bin/python -c "import fastapi, uvicorn, pandas, pyarrow; print('deps ok')"
# everyone, ONCE:
echo 'source ~/bd_project/scripts/env.sh' >> ~/bd_project/venv/bin/activate
```
✅ expect: `I am node<n> with 172.23.x.x`; on node4 also `deps ok` (nodes 1-3 run only the first lines: no pip step). ❌ [zt-ip](TROUBLESHOOTING.md#zt-ip) · [path-venv](TROUBLESHOOTING.md#path-venv)

### S05 · all nodes · /etc/hosts: the names node1-node4
```bash
sudo cp /etc/hosts /etc/hosts.before-bd_project
sudo sed -i '/# >>> bd_project/,/# <<< bd_project/d' /etc/hosts     # remove an older copy of this block
sudo sed -i 's/^127\.0\.1\.1/#127.0.1.1/' /etc/hosts                # comment out Ubuntu's 127.0.1.1 line
sudo tee -a /etc/hosts >/dev/null <<EOF
# >>> bd_project
$NODE1_IP  node1
$NODE2_IP  node2
$NODE3_IP  node3
$NODE4_IP  node4
# <<< bd_project
EOF
sudo sed -i "/^$MY_IP[[:space:]]/ s/\$/ $(hostname)/" /etc/hosts    # your own line also gets your hostname
cat /etc/hosts
getent hosts $(hostname)                                           # your ZeroTier IP, not 127.x
grep -n "$(hostname)" /etc/hosts                                   # only your own ZeroTier line (comment any other by hand)
for n in node1 node2 node3 node4; do ping -c1 -W2 $n >/dev/null && echo "$n ok" || echo "$n FAIL"; done
```
✅ expect: your hostname resolves to your ZeroTier IP; 4× `ok`. ❌ [ping](TROUBLESHOOTING.md#ping) · [sudo-host](TROUBLESHOOTING.md#sudo-host)

### S06 · all nodes · Firewall: let the other 3 nodes in
```bash
sudo ufw status | head -1
for ip in $NODE1_IP $NODE2_IP $NODE3_IP $NODE4_IP; do [ "$ip" = "$MY_IP" ] || sudo ufw allow from $ip; done
sudo ufw status
```
✅ expect: `inactive`, or `ALLOW  <the 3 other IPs>`. ❌ [ufw](TROUBLESHOOTING.md#ufw)

### S07 · all nodes · OPTIONAL: ZeroTier MTU (only if bulk transfers stall)
```bash
ZT_IF=$(ip -o -4 addr show | awk -v ip=$MY_IP '$4 ~ "^"ip"/" {print $2}'); echo $ZT_IF
sudo ip link set dev $ZT_IF mtu 1400
```
✅ expect: skip unless [TROUBLESHOOTING#mtu](TROUBLESHOOTING.md#mtu) says so (lost after reboot).

### S08 · nodes 1-3 (Members 1, 2, 3) · Iceberg jar
```bash
mkdir -p $STATE_DIR/jars
JAR=iceberg-spark-runtime-3.3_2.12-1.8.1.jar
[ -f /opt/iceberg/$JAR ] && cp /opt/iceberg/$JAR $STATE_DIR/jars/ || wget -P $STATE_DIR/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.8.1/$JAR
echo "43d0915f3eeec9b732307aee8334f5a84843e568  $STATE_DIR/jars/$JAR" | sha1sum -c
```
✅ expect: `...jar: OK`.

### S09 · all nodes · Hadoop config in state/conf/hadoop
```bash
mkdir -p $STATE_DIR/logs/hadoop $STATE_DIR/pids/hadoop $STATE_DIR/hdfs $STATE_DIR/conf
rm -rf $STATE_DIR/conf/hadoop && cp -r $HADOOP_HOME/etc/hadoop $STATE_DIR/conf/hadoop
cd $STATE_DIR/conf/hadoop
cat > core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://node2:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$STATE_DIR/hdfs/tmp</value></property>
</configuration>
EOF
cat > hdfs-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$STATE_DIR/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$STATE_DIR/hdfs/datanode</value></property>
  <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
  <property><name>dfs.namenode.heartbeat.recheck-interval</name><value>30000</value></property>
  <property><name>dfs.client.block.write.replace-datanode-on-failure.policy</name><value>NEVER</value></property>
</configuration>
EOF
cat >> hadoop-env.sh <<EOF
export JAVA_HOME="\${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HEAPSIZE_MAX=512m
EOF
cd ~/bd_project
source scripts/env.sh                                 # now HADOOP_CONF_DIR points at state/conf/hadoop
hdfs getconf -confKey fs.defaultFS; hdfs getconf -confKey dfs.replication
```
✅ expect: `hdfs://node2:9000` and `2`.

### S10 · nodes 1-3 · Spark config in state/conf/spark  ·  node4 · serving folder
```bash
mkdir -p $STATE_DIR/logs/spark $STATE_DIR/pids/spark $STATE_DIR/derby
rm -rf $STATE_DIR/conf/spark && cp -r $SPARK_HOME/conf $STATE_DIR/conf/spark
cd $STATE_DIR/conf/spark
cat > spark-env.sh <<EOF
export JAVA_HOME="\${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export SPARK_LOCAL_IP=$MY_IP
export SPARK_PUBLIC_DNS=$MY_IP
export SPARK_MASTER_HOST=$NODE3_IP
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1536m
export SPARK_DAEMON_MEMORY=512m
export SPARK_WORKER_PORT=40020
export PYSPARK_PYTHON=/usr/bin/python3.10
EOF
sed 's/^rootLogger.level = info/rootLogger.level = warn/' $SPARK_HOME/conf/log4j2.properties.template > log4j2.properties
cat > spark-defaults.conf <<EOF
spark.master                              spark://$NODE3_IP:7077
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
spark.pyspark.python                      /usr/bin/python3.10
spark.pyspark.driver.python               /usr/bin/python3.10
spark.hadoop.fs.defaultFS                 hdfs://node2:9000
spark.hadoop.dfs.replication              2
spark.sql.extensions                      org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg_catalog         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.type    hadoop
spark.sql.catalog.iceberg_catalog.warehouse  hdfs://node2:9000/warehouse
spark.jars                                $STATE_DIR/jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar
spark.eventLog.enabled                    true
spark.eventLog.dir                        hdfs://node2:9000/spark-logs
spark.history.fs.logDirectory             hdfs://node2:9000/spark-logs
spark.driver.extraJavaOptions             -Dderby.system.home=$STATE_DIR/derby
spark.ui.showConsoleProgress              false
EOF
cd ~/bd_project
source scripts/env.sh                                 # now SPARK_CONF_DIR points at state/conf/spark
grep -E "spark.master|spark.driver.host" $SPARK_CONF_DIR/spark-defaults.conf
# node 4 only (no Spark there):
mkdir -p $STATE_DIR/serving
```
✅ expect: `spark.master  spark://172.23.10.141:7077` and `spark.driver.host <your IP>`.

### S11 · node2 + node3 · Stop the old course daemons
```bash
jps
(
  unset HADOOP_CONF_DIR HADOOP_LOG_DIR HADOOP_PID_DIR SPARK_CONF_DIR SPARK_LOG_DIR SPARK_PID_DIR
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  yarn --daemon stop resourcemanager; yarn --daemon stop nodemanager
  hdfs --daemon stop secondarynamenode; hdfs --daemon stop datanode; hdfs --daemon stop namenode
  stop-history-server.sh; stop-worker.sh; stop-master.sh
)
jps
```
Last resort: `kill <pid>`. Never run `start-dfs.sh`, `start-yarn.sh`, `start-all.sh`.
✅ expect: `jps` lists only `Jps`. ❌ [old-daemons](TROUBLESHOOTING.md#old-daemons)

### S12 · GATE 0 · all nodes · setup
```bash
bash ~/bd_project/scripts/check.sh setup
```
✅ expect: `GATE setup PASSED` on all 4 nodes. If FAIL → [TROUBLESHOOTING](TROUBLESHOOTING.md) at the anchor it prints (usually [conf](TROUBLESHOOTING.md#conf), [versions](TROUBLESHOOTING.md#versions), [dn-ip](TROUBLESHOOTING.md#dn-ip)).
say in chat: "setup OK" (each member).

### S12a · all nodes · Try doctor.sh once (and whenever something breaks)
```bash
bash ~/bd_project/scripts/doctor.sh
```
✅ expect: `doctor: no FAIL` (WARN lines for daemons not started yet are normal now). Whenever any step or GATE fails: run it and paste the whole output in the group chat.

---

## STAGE 1 · network

### S13 · node3 (Member 3) · Open a test listener on the driver port
```bash
nc -l 40000          # waits; if your nc complains: nc -l -p 40000
```
say in chat: "node3 listening".

### S14 · node2, node1, node4 (Members 2, 1, 4; one after another or parallel) · Can I reach node3:40000?
```bash
nc -zv -w 3 node3 40000
```
✅ expect: `Connection to node3 40000 port [tcp/*] succeeded!` 📸. Then node3: Ctrl-C. say in chat: "reached node3".

### S15 · node2 (Member 2), then node1 · Open the listener
```bash
nc -l 40000
```
say in chat: "node2 listening" (then "node1 listening"; Ctrl-C after node3 reports).

### S16 · node3 (Member 3) · Can I reach node2:40000, then node1:40000?
```bash
nc -zv -w 3 node2 40000
nc -zv -w 3 node1 40000
```
✅ expect: `succeeded!` both 📸. Do not go on until all directions work. say in chat: "ports OK".

### S17 · GATE 1 · all nodes · network
```bash
bash ~/bd_project/scripts/check.sh network
```
✅ expect: `GATE network PASSED` on all 4 nodes (ports without a listener are "skipped", only timeouts fail). If FAIL → [ufw](TROUBLESHOOTING.md#ufw) · [wsl2](TROUBLESHOOTING.md#wsl2) · [ping](TROUBLESHOOTING.md#ping) · [mtu](TROUBLESHOOTING.md#mtu). Optional bigger test on all 4 at once: `python3 scripts/netcheck.py`.

---

## STAGE 2 · HDFS up

### S18 · node2 (Member 2) · Format HDFS (once, ever)
```bash
jps
ls state/hdfs/namenode/current/VERSION
```
If `VERSION` exists: HDFS is already formatted, do NOT format; go to S19. Otherwise:
```bash
hdfs namenode -format -nonInteractive -force 2>&1 | grep -E "successfully formatted|ERROR|Exception"
ls state/hdfs/namenode/current/VERSION
```
✅ expect: `successfully formatted` and the `VERSION` file exists. ❌ [clusterid](TROUBLESHOOTING.md#clusterid) · [port-in-use](TROUBLESHOOTING.md#port-in-use)

### S19 · node2 (Member 2) · Start the NameNode, create the five HDFS folders
```bash
hdfs --daemon start namenode
ss -tln | grep :9000
hdfs dfsadmin -safemode get
hdfs dfs -mkdir -p /search/input/phase-0 /search/input/phase-1 /search/exports /warehouse /spark-logs
hdfs dfs -ls -R /
```
✅ expect: `0.0.0.0` or `172.23.194.191:9000` listening, `Safe mode is OFF`, the folder listing 📸. (If `-mkdir` complains about safe mode: wait 30 s and repeat.) ❌ [safemode](TROUBLESHOOTING.md#safemode) · [port-in-use](TROUBLESHOOTING.md#port-in-use)
say in chat: "NameNode up".

### S20 · node2 (Member 2) · Start the DataNode (parallel with S21)
```bash
hdfs --daemon start datanode
jps
```
✅ expect: `NameNode`, `DataNode` in `jps`. ❌ [clusterid](TROUBLESHOOTING.md#clusterid) · [old-daemons](TROUBLESHOOTING.md#old-daemons)

### S21 · node3 (Member 3) · Start the DataNode (parallel with S20)
```bash
hdfs --daemon start datanode
ss -tln | grep :9866
```
✅ expect: port 9866 listens on `172.23.10.141`. ❌ [clusterid](TROUBLESHOOTING.md#clusterid) · [port-in-use](TROUBLESHOOTING.md#port-in-use)
say in chat: "DataNodes up".

### S22 · GATE 2 · node2, node3, node1, node4 · hdfs
```bash
bash ~/bd_project/scripts/check.sh hdfs
hdfs dfsadmin -report | grep -E "^Live datanodes|^Dead datanodes|^Name:"        # 📸 (node2)
```
✅ expect: `GATE hdfs PASSED` on all 4 (2 live DataNodes with the node2 and node3 IPs, safe mode OFF, 5 folders, test file `Live_repl=2`); 📸 `http://node2:9870` → Datanodes (2 in service). If FAIL → [dn-missing](TROUBLESHOOTING.md#dn-missing) · [dn-ip](TROUBLESHOOTING.md#dn-ip) · [safemode](TROUBLESHOOTING.md#safemode) · [minreplication](TROUBLESHOOTING.md#minreplication) · [refused-9000](TROUBLESHOOTING.md#refused-9000)

---

## STAGE 3 · Spark up

### S23 · node3 (Member 3) · Start the Spark master, worker, history server (parallel with S24)
```bash
start-master.sh
start-worker.sh spark://172.23.10.141:7077
hdfs dfs -test -d /spark-logs && start-history-server.sh
jps
```
✅ expect: `jps`: `DataNode`, `Master`, `Worker`, `HistoryServer`. ❌ [port-in-use](TROUBLESHOOTING.md#port-in-use) · [one-worker](TROUBLESHOOTING.md#one-worker)

### S24 · node2 (Member 2) · Start the Spark worker (parallel with S23)
```bash
start-worker.sh spark://172.23.10.141:7077
jps
```
✅ expect: `NameNode`, `DataNode`, `Worker` (the worker retries until the master is up).
say in chat: "daemons up".

### S25 · GATE 3 · node3, node2, node1 · spark
```bash
bash ~/bd_project/scripts/check.sh spark
curl -s http://172.23.10.141:8080/json/ | python3 -m json.tool | grep -E '"(host|state)"'     # 📸 (node2)
```
✅ expect: `GATE spark PASSED` (master port 7077, 2 ALIVE workers with the node2 and node3 IPs); 📸 `http://node3:8080` (2 workers). The trailing slash in `/json/` matters. If FAIL → [master-down](TROUBLESHOOTING.md#master-down) · [one-worker](TROUBLESHOOTING.md#one-worker) · [no-resources](TROUBLESHOOTING.md#no-resources)
say in chat: "cluster healthy".

### S26 · node1 (Member 1) · Cluster smoke test (parallel with S28)
```bash
spark-submit code/tools/cluster_check.py 2> state/logs/check.log
```
✅ expect: `task ran on node2 ... python 3.10`, `task ran on node3 ... python 3.10`, `workers used: node2, node3 -> OK`; table `check.hello` has 1 row 📸. ❌ [one-worker](TROUBLESHOOTING.md#one-worker) · [no-resources](TROUBLESHOOTING.md#no-resources) · [py-version](TROUBLESHOOTING.md#py-version)
say in chat: "node1 check OK".

### S27 · node3 (Member 3) · Cluster smoke test, then clean up (after S26)
```bash
spark-submit code/tools/cluster_check.py 2> state/logs/check.log
spark-submit code/tools/cluster_check.py --drop 2> state/logs/check-drop.log
```
✅ expect: same three lines; `check.hello` has 2 rows (`node1`, `node3`) 📸 before `--drop`. ❌ same as S26.
say in chat: "check OK".

### S28 · node4 (Member 4) · Start the search service, test HDFS access (parallel with S26)
Terminal A:
```bash
source ~/bd_project/venv/bin/activate
cd ~/bd_project/code/node4_search
uvicorn app:app --host 0.0.0.0 --port 8000 2>&1 | tee ~/bd_project/state/logs/serve.log
```
Terminal B (opener first):
```bash
curl -s localhost:8000/api/health; echo
hdfs dfs -ls /search
```
✅ expect: `{"status":"no-version", ... "last_error":null}`; `http://node4:8000` shows *no version loaded*; `hdfs dfs -ls /search` lists `exports` and `input`. ❌ [path-venv](TROUBLESHOOTING.md#path-venv) · [refused-9000](TROUBLESHOOTING.md#refused-9000)
say in chat: "node4 ready".

---

## STAGE 4 · Phase 0 ingest

### S29 · node1 (Member 1) · Upload Phase 0 to HDFS, look at the replication
```bash
hdfs dfs -put -f data/phase-0/pages.jsonl /search/input/phase-0/pages.jsonl
hdfs dfs -ls /search/input/phase-0
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations | grep -E "Live_repl|Average block replication"
```
✅ expect 📸: `Live_repl=2` with the node2 AND node3 IPs, `Average block replication: 2.0`. ❌ [minreplication](TROUBLESHOOTING.md#minreplication) · [refused-9000](TROUBLESHOOTING.md#refused-9000)

### S30 · node1 (Member 1) · Ingest Phase 0
```bash
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
✅ expect 📸: lines read 1040 · rejected 20 (`unparseable_json`) · duplicates superseded 20 · accepted 1000 (20 without `links`) · changes merged 1000 · ADDED/UPDATED/DELETED 1000/0/0 · links kept 499 · dangling 0 · docs 0 → 1000. ❌ [no-resources](TROUBLESHOOTING.md#no-resources) · [iceberg-class](TROUBLESHOOTING.md#iceberg-class) · [permission](TROUBLESHOOTING.md#permission)
say in chat: the `docs snapshot ... after` id (it is the Phase 0 snapshot; node2 tags it in S39).

### S31 · node1 (Member 1) · Look at the tables, run Phase 0 again (idempotence)
```bash
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT phase, reason, count(*) FROM iceberg_catalog.search.rejects GROUP BY phase, reason" 2> state/logs/sql.log
$SQL -e "SELECT line_no, doc_id, reason, detail, substr(raw,1,60) FROM iceberg_catalog.search.rejects ORDER BY line_no LIMIT 25" 2> state/logs/sql.log
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
✅ expect 📸: 1000 · `unparseable_json 20` and `duplicate_superseded 20` · re-run: unchanged 1000, changes merged 0, links table rewritten False, docs snapshot id before = after. ❌ [table-not-found](TROUBLESHOOTING.md#table-not-found)
say in chat: "Phase 0 ingested".

### S32 · GATE 4 · node1, node2 · ingest0
```bash
bash ~/bd_project/scripts/check.sh ingest0
```
✅ expect: `GATE ingest0 PASSED` (input 1040 lines, docs snapshot with 1000 rows, links 499, rejects 40). If FAIL → [table-not-found](TROUBLESHOOTING.md#table-not-found) · [no-resources](TROUBLESHOOTING.md#no-resources)

---

## STAGE 5 · Phase 0 index, PageRank, export

### S33 · node3 (Member 3) · Build the inverted index, Phase 0
```bash
spark-submit code/node3_compute/build_index.py --phase 0 2> state/logs/index-p0.log
```
✅ expect 📸: `mode: full`, docs 1000, distinct terms 37551, postings 233643, avg/max doc length 461.0 / 13903. ❌ [table-not-found](TROUBLESHOOTING.md#table-not-found) · [no-resources](TROUBLESHOOTING.md#no-resources)

### S34 · node3 (Member 3) · PageRank, Phase 0
```bash
spark-submit code/node3_compute/pagerank.py --phase 0 2> state/logs/pagerank-p0.log
```
✅ expect 📸: `N=1000 docs, 499 links`, 501 dangling, sum of ranks 1.000000, max/min 0.001752 / 0.000264 (6.65), tied at minimum 501, fingerprint `60a6221350e7`.

### S35 · node3 (Member 3) · Validate and export v0
```bash
spark-submit code/node3_compute/export.py --version v0 --phase 0 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v0.log
hdfs dfs -ls -R hdfs://node2:9000/search/exports/v0 | head -12
```
✅ expect 📸: 13 × `PASS`, `EXPORT v0: OK`, docs / postings / terms = 1000 / 233643 / 37551, listing with `_SUCCESS`. ❌ [stale-export](TROUBLESHOOTING.md#stale-export) · [exists](TROUBLESHOOTING.md#exists) · [no-resources](TROUBLESHOOTING.md#no-resources)

### S36 · GATE 5 · node3, node2 · index and export v0
```bash
bash ~/bd_project/scripts/check.sh index
bash ~/bd_project/scripts/check.sh export v0
```
✅ expect: `GATE index PASSED` and `GATE export v0 PASSED` (postings 233643, `manifest.json` status OK, `_SUCCESS`, 13/13 checks). If FAIL → [table-not-found](TROUBLESHOOTING.md#table-not-found) · [export-bad](TROUBLESHOOTING.md#export-bad) · [stale-export](TROUBLESHOOTING.md#stale-export)
say in chat: "v0 exported".

---

## STAGE 6 · Phase 0 serve

### S37 · node4 (Member 4) · Pull v0 and search it (terminal B; parallel with S39)
```bash
python code/node4_search/pull_export.py --version v0 --source hdfs://node2:9000/search/exports
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py april
python code/node4_search/q.py ocean
python code/node4_search/q.py "computer science"
python code/node4_search/q.py zzzqqq
```
✅ expect 📸: `verified v0: 1000 docs, 233643 postings, 13/13 checks passed` · `CURRENT: None -> v0` · `service: {'switched': True, 'version': 'v0'}`. Top results: bubonic plague → Bubonic plague (100) 1.000, Flea (3523) 0.622, London (460) 0.495 · alan turing → Alan Turing (13) 0.907 · april → March (468) 0.870, May (469) 0.807, ... April (1) is 5th · ocean → Indian Ocean (1978) 1.000 · computer science → Computer science (110) 1.000 · zzzqqq → 0 matches. ❌ [path-venv](TROUBLESHOOTING.md#path-venv) · [minreplication](TROUBLESHOOTING.md#minreplication) · [serve](TROUBLESHOOTING.md#serve)

### S38 · node4 (Member 4) · The UI
Open `http://node4:8000`, search two queries. 📸 with the score bars visible (blue BM25, orange PageRank).

### S39 · node2 (Member 2) · Report and tag Phase 0, BEFORE Phase 1 is ingested (parallel with S37)
```bash
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
hdfs dfs -ls -R /warehouse/search/docs
hdfs fsck /warehouse -files -blocks | grep -E "Total files|Average block replication|Under-replicated"
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs" 2> state/logs/sql.log
```
✅ expect 📸: `docs 1000 rows | 1 snapshots`, links 499, rejects 40, doc_stats 1000, postings 233643, pagerank 1000 · `Average block replication: 2.0`, `Under-replicated blocks: 0` · tag row `phase0  TAG  <the snapshot id Member 1 posted>`. ❌ [table-not-found](TROUBLESHOOTING.md#table-not-found) · [exists](TROUBLESHOOTING.md#exists) (tag name taken)
say in chat: "go for Phase 1".

### S40 · GATE 6 · node4 · pulled v0 and serve v0
```bash
bash ~/bd_project/scripts/check.sh pulled v0
bash ~/bd_project/scripts/check.sh serve v0
```
✅ expect: `GATE pulled v0 PASSED` and `GATE serve v0 PASSED` (manifest OK 13/13, health serving v0 with 1000 docs, query `ocean` has results). If FAIL → [serve](TROUBLESHOOTING.md#serve) · [export-bad](TROUBLESHOOTING.md#export-bad)

---

## STAGE 7 · Phase 1 ingest

### S41 · node1 (Member 1) · Ingest Phase 1 (after "go for Phase 1")
```bash
hdfs dfs -put -f data/phase-1/changes.json /search/input/phase-1/changes.jsonl
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
✅ expect 📸: lines read 100 · accepted 100 (50 DELETE) · ADDED/UPDATED/DELETED 25/25/50 · docs 1000 → 975 · links kept 467 · dangling dropped 7.

### S42 · node1 (Member 1) · Look, then run Phase 1 again
```bash
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
$SQL -e "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type ORDER BY phase, change_type" 2> state/logs/sql.log
$SQL -e "SELECT doc_id, reason, detail FROM iceberg_catalog.search.rejects WHERE phase = 1" 2> state/logs/sql.log
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
✅ expect 📸: 25 ADDED, 25 UPDATED, 50 DELETED · 7 `dangling_link_dropped` rows · re-run: unchanged 50, DELETE of a doc not in the table 50, changes merged 0, same snapshot.

### S43 · GATE 7 · node1, node2 · ingest1
```bash
bash ~/bd_project/scripts/check.sh ingest1
```
✅ expect: `GATE ingest1 PASSED` (input 100 lines, docs 975, links 467, rejects 47). If FAIL → [table-not-found](TROUBLESHOOTING.md#table-not-found) · [no-resources](TROUBLESHOOTING.md#no-resources)
say in chat: "Phase 1 ingested".

---

## STAGE 8 · Phase 1 index, PageRank, export

### S44 · node3 (Member 3) · Incremental index, twice
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
```
✅ expect 📸: first run `Iceberg says 100 docs changed ... {'UPDATED': 25, 'DELETED': 50, 'ADDED': 25}`, `mode: incremental`, docs 975, terms 36521, postings 220622 · second run `mode: up-to-date`.

### S45 · node3 (Member 3) · PageRank, Phase 1
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1.log
```
✅ expect 📸: `N=975 docs, 467 links`, 508 dangling, sum 1.000000, max/min 0.003926 / 0.000279 (14.08), tied at minimum 548, fingerprint `54ee091e74b5`.

### S46 · node3 (Member 3) · Export v1
```bash
spark-submit code/node3_compute/export.py --version v1 --phase 1 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1.log
hdfs dfs -ls -R hdfs://node2:9000/search/exports/v1 | head -12
```
✅ expect 📸: 13 × `PASS`, `EXPORT v1: OK`, docs / postings / terms = 975 / 220622 / 36521.

### S47 · GATE 8 · node3, node2 · index and export v1
```bash
bash ~/bd_project/scripts/check.sh index
bash ~/bd_project/scripts/check.sh export v1
```
✅ expect: `GATE index PASSED` (postings 220622) and `GATE export v1 PASSED`. If FAIL → [stale-export](TROUBLESHOOTING.md#stale-export) · [export-bad](TROUBLESHOOTING.md#export-bad)
say in chat: "v1 exported".

---

## STAGE 9 · Phase 1 serve

### S48 · node4 (Member 4) · v0 still served, then pull v1 (parallel with S49)
```bash
python code/node4_search/q.py april
python code/node4_search/q.py "alan turing"
python code/node4_search/pull_export.py --version v1 --source hdfs://node2:9000/search/exports
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py april
python code/node4_search/q.py phase
python code/node4_search/q.py "computer science"
```
✅ expect 📸: `verified v1: 975 docs, 220622 postings, 13/13 checks passed` · `CURRENT: v0 -> v1` · `service: {'switched': True, 'version': 'v1'}` · bubonic plague → "Bubonic plague (Phase 1)" (100) · alan turing gone, top is List of mathematicians (3433) · april 66 matches (was 71), April gone · phase 69 matches (was 20) · computer science 174 matches, "Computer science (Phase 1)" (110).

### S49 · node2 (Member 2) · Tag Phase 1, report, time travel (parallel with S48)
```bash
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase1" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase1" 2> state/logs/sql.log
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs VERSION AS OF 'phase0'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT doc_id, title FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' WHERE doc_id IN ('1','100')" 2> state/logs/sql.log
$SQL -e "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id IN ('1','100')" 2> state/logs/sql.log
$SQL -e "SELECT count(*) AS deleted_in_phase1 FROM (SELECT doc_id FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' EXCEPT SELECT doc_id FROM iceberg_catalog.search.docs)" 2> state/logs/sql.log
```
✅ expect 📸: `docs 975 rows | 2 snapshots` (history: `append 1000`, then `overwrite added 975, deleted 1000`) · 1000 vs 975 · phase0 has `1 April` and `100 Bubonic plague`, now only `100 Bubonic plague (Phase 1)` · `deleted_in_phase1 = 50`.

### S50 · GATE 9 · node4 · pulled v1 and serve v1
```bash
bash ~/bd_project/scripts/check.sh pulled v1
bash ~/bd_project/scripts/check.sh serve v1
```
✅ expect: `GATE pulled v1 PASSED` and `GATE serve v1 PASSED` (975 docs). If FAIL → [serve](TROUBLESHOOTING.md#serve) · [export-bad](TROUBLESHOOTING.md#export-bad)
say in chat: "Phase 1 served; demos next".

---

## STAGE 10 · demos

### S51 · node3 (Member 3) · Slow PageRank, to be killed in the middle (parallel with S52)
Terminal 1:
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 --slow-seconds 3 2> state/logs/pagerank-p1-slow.log
```
say in chat: "running" (node2 kills when the screen shows `iter 2` or `iter 3`).

### S52 · node2 (Member 2) · Kill a Spark executor (parallel with S51)
```bash
jps | grep CoarseGrainedExecutorBackend
kill -9 <that pid>
```
✅ expect: a pid; the job on node3 continues and finishes. Stronger variant: `stop-worker.sh` during the job, then afterwards `start-worker.sh spark://172.23.10.141:7077`.

### S53 · node3 (Member 3) · Count the failure messages after the job finished
Terminal 2:
```bash
grep -oE "Lost executor|ExecutorLostFailure|No more replicas|FetchFailed" state/logs/pagerank-p1-slow.log | sort | uniq -c
```
✅ expect 📸 (job output, the grep, `http://node3:8080` or `:18080`): the killed iteration is much slower; `Lost executor`, `ExecutorLostFailure`, `FetchFailed` in the log; `sum of ranks: 1.000000` and fingerprint `54ee091e74b5` (same as S45). If the fingerprint differs: tell the team before the demo.

### S54 · node3 (Member 3) · A broken export is refused
```bash
spark-submit code/node3_compute/export.py --version v1-broken --phase 1 --simulate-failure --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1-broken.log
echo "exit code $?"
hdfs dfs -ls /search/exports
```
✅ expect 📸: 3 `FAIL` lines (`every doc has a PageRank: 5 missing`, `PageRank sums to 1: 0.998...`, `every posting points at an exported doc: 1 orphan postings`), `VALIDATION FAILED - nothing published`, `exit code 1`, in HDFS only `_failed_v1-broken_<time>`.
say in chat: "broken export made".

### S55 · node4 (Member 4) · The broken export is refused
```bash
python code/node4_search/pull_export.py --version v1-broken --source hdfs://node2:9000/search/exports; echo "exit code $?"
curl -s localhost:8000/api/health; echo
```
✅ expect 📸: `REFUSED: ... _SUCCESS not found ... Still serving v1.`, `exit code 1`, health still `"version":"v1"`.

### S56 · node4 (Member 4) · Switch between v0 and v1
```bash
curl -s -X POST localhost:8000/api/admin/switch/v0; echo
python code/node4_search/q.py "alan turing"
curl -s -X POST localhost:8000/api/admin/switch/v1; echo
python code/node4_search/q.py "alan turing"
curl -s localhost:8000/api/versions | python3 -m json.tool
```
✅ expect 📸: Alan Turing back on v0, gone on v1; `/api/versions` lists v0 and v1, valid, with their Iceberg snapshot ids.

### S57 · node2 (Member 2) · A bad DELETE
```bash
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
$SQL -e "DELETE FROM iceberg_catalog.search.docs WHERE title LIKE 'C%'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history" 2> state/logs/sql.log
```
✅ expect: 878 rows and a 3rd snapshot. say in chat: "bad delete done".

### S58 · node4 (Member 4) · Search is unaffected
```bash
python code/node4_search/q.py "computer science"
```
✅ expect: still returns results (node4 serves exported Parquet, never Iceberg).

### S59 · node2 (Member 2) · Roll back
```bash
GOOD=$($SQL -e "SELECT snapshot_id FROM iceberg_catalog.search.docs.refs WHERE name = 'phase1'" 2> state/logs/sql.log); echo "GOOD=$GOOD"
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.docs', $GOOD)" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history" 2> state/logs/sql.log
```
(`SQL=...` as in S57.) ✅ expect 📸: `GOOD=` is a number · the procedure prints `<bad id>  <good id>` · count back to 975 · history has a 4th row with the good id; the bad snapshot has `is_current_ancestor = false`.
say in chat: "rolled back".

### S60 · node3 (Member 3) · Index again after the rollback
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
```
✅ expect: `mode: up-to-date`.

### S61 · node3 (Member 3) · Stop the DataNode (parallel with S62)
```bash
hdfs --daemon stop datanode
```
say in chat: "DataNode stopped".

### S62 · node2 (Member 2) · Reads still work, later the DataNode is dead (parallel with S61)
```bash
hdfs dfs -cat /search/input/phase-0/pages.jsonl | wc -l
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
# about 90 seconds later:
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication|Status"
```
✅ expect 📸: 1040 and 975 · later `Live datanodes (1)`, `Dead datanodes (1)`, `Under-replicated blocks: <n> (100 %)`, `Status: HEALTHY` · `http://node2:9870` → node3 dead.
say in chat: "node3 dead, start it again".

### S63 · node3 (Member 3) · Start the DataNode again
```bash
hdfs --daemon start datanode
ss -tln | grep :9866
```
✅ expect: port 9866 listens again.

### S64 · node2 (Member 2) · Recovered, about 30 s after S63
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication|Status"
```
✅ expect 📸: `Live datanodes (2)`, `Under-replicated blocks: 0`, `Average block replication: 2.0`.

### S65 · node3 (Member 3) · Where the time and traffic go
```bash
ZT_IF=$(ip -o -4 addr show | awk -v ip=$MY_IP '$4 ~ "^"ip"/" {print $2}')
rx0=$(cat /sys/class/net/$ZT_IF/statistics/rx_bytes); tx0=$(cat /sys/class/net/$ZT_IF/statistics/tx_bytes)
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1-meter.log
echo "received $(( ($(cat /sys/class/net/$ZT_IF/statistics/rx_bytes) - rx0) / 1048576 )) MB, sent $(( ($(cat /sys/class/net/$ZT_IF/statistics/tx_bytes) - tx0) / 1048576 )) MB on $ZT_IF"
```
✅ expect 📸: the `received ... MB, sent ... MB` line; `http://node3:18080` → the application → *Stages*, sort by *Duration*, *Shuffle Read/Write*.

### S66 · GATE 10 · all nodes · everything healthy after the demos
```bash
bash ~/bd_project/scripts/check.sh all
```
✅ expect: `GATE all PASSED` on every node (HDFS 2 DataNodes, Spark 2 workers, tables, exports v0/v1, node4 serving v1). If FAIL → the anchor it prints.

---

## STAGE 11 · shutdown

### S67 · node4 (Member 4) · Stop the service
Ctrl-C in terminal A.

### S68 · node3 (Member 3) · Stop the daemons (before node2 stops the NameNode)
```bash
stop-history-server.sh
stop-worker.sh
stop-master.sh
hdfs --daemon stop datanode
```

### S69 · node2 (Member 2) · Stop the daemons (NameNode last)
```bash
stop-worker.sh
hdfs --daemon stop datanode
hdfs --daemon stop namenode
```

### S70 · GATE 11 · all nodes · down
```bash
bash ~/bd_project/scripts/check.sh down
```
✅ expect: `GATE down PASSED` (no daemon, no search service left). If FAIL → [port-in-use](TROUBLESHOOTING.md#port-in-use)

---

## Another day / start over (commands only)

| Want | Node | Commands |
|---|---|---|
| Start another day (**no format**) | node2 | `hdfs --daemon start namenode` · `hdfs --daemon start datanode` · `start-worker.sh spark://172.23.10.141:7077` |
| | node3 | `hdfs --daemon start datanode` · `start-master.sh` · `start-worker.sh spark://172.23.10.141:7077` · `start-history-server.sh` |
| | node4 | S28 (it loads the last version, `CURRENT`) |
| | then | `check.sh hdfs` and `check.sh spark` (GATE 2 and 3) |
| Rehearse from zero (keep HDFS, delete the lake) | node2 | `hdfs dfs -rm -r -skipTrash /warehouse/search /warehouse/check '/search/exports/*'` · `hdfs dfs -ls -R /warehouse /search/exports` |
| | node4 | Ctrl-C the service; `rm -rf ~/bd_project/state/serving && mkdir -p ~/bd_project/state/serving`; start it again (S28) |
| | then | continue at S29 |
| Delete everything of ours on a node | that node | stop its daemons, `rm -rf ~/bd_project/state`, redo S08-S10, then format again (S18; the DataNode folder on node3 is gone with `state/`) |
| Re-format an existing HDFS (rare) | node2 | stop everything; `rm -rf ~/bd_project/state/hdfs/namenode ~/bd_project/state/hdfs/datanode`; S18 |
| | node3 | `rm -rf ~/bd_project/state/hdfs/datanode` before its DataNode starts ([clusterid](TROUBLESHOOTING.md#clusterid)) |
| IPs changed | all | one person edits `team.env` and pushes; everyone `git pull`, redo S05-S10, restart the daemons |

## Evidence checklist (📸)

- **Member 1:** `fsck` replication of pages.jsonl (S29) · Phase 0 summary (S30) · rejects query and re-run with the same snapshot (S31) · Phase 1 summary (S41) · `doc_changes` query and re-run (S42) · `check.hello` after node1 (S26).
- **Member 2:** port test (S14) · `dfsadmin -report` + NameNode UI with 2 DataNodes (S22) · report and tag after Phase 0 (S39) · report after Phase 1 and the time-travel queries (S49) · bad delete and rollback history (S57, S59) · DataNode dead and back (S62, S64).
- **Member 3:** port test (S13, S16) · master UI with 2 workers (S25) · `check.hello` with 2 rows (S27) · index full, incremental, up-to-date (S33, S44) · PageRank Phase 0 and 1 (S34, S45) · export 13 PASS (S35, S46) · executor kill: output, grep, UI (S51-S53) · broken export (S54) · history server stages and the received/sent MB line (S65). Save `state/logs/*.log`.
- **Member 4:** UI with v0 results (S37, S38) · `pull_export.py` v0 output (S37) · v1 pull and the same queries (S48) · REFUSED pull (S55) · version switch and `/api/versions` (S56).
- **Everyone:** every `GATE ... PASSED` line (S12, S17, S22, S25, S32, S36, S40, S43, S47, S50, S66, S70).
