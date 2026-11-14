# RUNBOOK

The one guide. Each member types the **real** HDFS / Spark / Iceberg / uvicorn commands (no wrapper hides them), because those commands are what you must be able to explain in the viva.

Stuck? [TROUBLESHOOTING.md](TROUBLESHOOTING.md). Expected numbers and viva answers: [VIVA.md](VIVA.md).

## How it works (read once)

- The repo is cloned to `~/bd_project` on every VM.
- **First thing in every terminal** (the second line makes the relative paths `code/...` and `data/...` work):
  ```bash
  source ~/bd_project/venv/bin/activate
  cd ~/bd_project
  ```
  The prompt then shows `(node3)`. Activating loads `scripts/env.sh`, which points `HADOOP_CONF_DIR` and `SPARK_CONF_DIR` at our own configs in `~/bd_project/state/conf/` and sets `SPARK_LOCAL_IP` to your ZeroTier IP. The Spark/Iceberg settings (master `spark://172.23.10.141:7077`, `hdfs://node2:9000`, Iceberg catalog `iceberg_catalog`, event log) are in `state/conf/spark/spark-defaults.conf` (written in P0-10), so **plain `spark-submit` and `spark-sql` need no `--master` or `--properties-file`**. The course's own config in `/opt` is never touched.
- Spark jobs print their summary on screen. The noisy driver log goes to a file with `2> state/logs/<name>.log` (the `2>` sends only stderr to the file).
- There is no wrapper program: every command in this guide is a real command you type and can explain. The only helper file is `scripts/env.sh` (loaded by `activate`).
- 📸 = take a screenshot (evidence for the report). Logs live in `~/bd_project/state/logs/`; The known errors and their fixes are in the pattern table in [TROUBLESHOOTING](TROUBLESHOOTING.md#log-patterns).

| Member | Node | Role |
|---|---|---|
| M1 | node1 (172.23.59.75) | ingestion: Spark driver + HDFS client. No daemons |
| M2 | node2 (172.23.194.191) | storage: HDFS NameNode + DataNode, Spark worker, the Iceberg tables |
| M3 | node3 (172.23.10.141) | compute: Spark master + worker, DataNode, history server. Drivers for index, PageRank, export |
| M4 | node4 (172.23.204.87) | search: FastAPI service. HDFS client only, no Spark |

---

## Part 0 · everyone: get the machine ready (before the call, needs internet)

You already did the course guides (Java 11, Hadoop 3.3.6, Spark 3.3.4 + Python 3.10, ZeroTier). Here you turn those single-machine installs into one 4-node cluster. **Nothing is reinstalled, only configured.** Everything we write goes into `~/bd_project/state/` (configs, HDFS data, logs, jars, serving folder) or `/etc/hosts`; the course's own config in `/opt` is not touched. Delete `state/` for a clean start.

Tags: **[all]** every member, **[1-3]** nodes 1, 2, 3 only (the ones with Spark), **[3]**, **[4]** one node. Do not go on until each ✅ passes.

**P0-1 · [all] clone the repo and check the course installs**
```bash
git clone https://github.com/Greninja34/bd_project.git ~/bd_project
java -version 2>&1 | head -1                         # openjdk version "11.x"
hadoop version | head -1                             # Hadoop 3.3.6
spark-submit --version 2>&1 | grep -m1 "version"     # 3.3.4         (nodes 1-3 only)
/usr/bin/python3.10 --version                        # Python 3.10.x (nodes 1-3 only)
sudo apt install -y git python3-venv netcat-openbsd  # node4 needs git + python3-venv; everyone needs nc for the port tests
```
**What it does.** Clones the code and shows the installed versions. Spark 3.3.4 breaks on Python 3.11+, so the executors use `/usr/bin/python3.10` on every node. If your Python 3.10 lives at `/usr/local/bin/python3.10`: `sudo ln -s /usr/local/bin/python3.10 /usr/bin/python3.10`.
✅ You should see the four versions above. ❌ [wrong path](TROUBLESHOOTING.md#path-venv).

**P0-2 · [3] only (M3): WSL2 and ZeroTier inside Ubuntu**
node3 runs in **WSL2 Ubuntu**. ZeroTier must run **inside** that Ubuntu, not only on Windows. Why: Spark executors on node2 must connect *into* the driver on node3 (port 40000); WSL2's NAT blocks that. With ZeroTier inside Ubuntu the traffic lands on Ubuntu's own interface and Windows never sees it. In Ubuntu:
```bash
cat /etc/wsl.conf        # if it has other settings, add the two sections below to it by hand instead of overwriting
sudo tee /etc/wsl.conf >/dev/null <<'EOF'
[boot]
systemd=true
[network]
generateHosts=false
EOF
```
Then in **Windows PowerShell**: `wsl --shutdown`, and open Ubuntu again. In Ubuntu:
```bash
ls -l /dev/net/tun                                   # must exist (else: wsl --update in PowerShell)
curl -s https://install.zerotier.com | sudo bash
sudo zerotier-cli join <network id>
```
In ZeroTier Central: authorize the **new** member, set its IP by hand to `172.23.10.141`, delete the **old Windows** member of node3. Keep one Ubuntu terminal open the whole session (WSL stops when the last terminal closes).
**What it does.** `systemd=true` lets ZeroTier run as a service; `generateHosts=false` stops WSL from rewriting `/etc/hosts` at every start (we write it in P0-5).
✅ You should see `ip -4 -br a` showing `172.23.10.141`. ❌ [node3 on WSL2](TROUBLESHOOTING.md#wsl2).

**P0-3 · [all] ZeroTier**
```bash
ip -4 -br a                                          # your ZeroTier IP from team.env must be listed
sudo zerotier-cli info
sudo zerotier-cli peers | grep LEAF
cat ~/bd_project/team.env
```
✅ Your IP is listed; peers say `DIRECT` (`RELAY` works but is slow); write down your 10-digit ZeroTier node ID from `info`: all 4 must differ. ❌ [ZeroTier IP not on this machine](TROUBLESHOOTING.md#zt-ip) · [ping fails](TROUBLESHOOTING.md#ping) · [duplicate node IDs](TROUBLESHOOTING.md#dup-id). If an IP ever changes: one person edits `team.env` and pushes; everyone `git pull` and redoes P0-5 to P0-10.

**P0-4 · [all] the environment file and the venv**
```bash
source ~/bd_project/scripts/env.sh
echo "I am $NODE_NAME with $MY_IP"                   # node detected from team.env + your interfaces
# nodes 1, 2, 3:
/usr/bin/python3.10 -m venv --without-pip ~/bd_project/venv
# node 4 instead:
python3 -m venv ~/bd_project/venv
~/bd_project/venv/bin/pip install -r ~/bd_project/code/node4_search/requirements.txt
~/bd_project/venv/bin/python -c "import fastapi, uvicorn, pandas, pyarrow; print('deps ok')"
# everyone, ONCE:
echo 'source ~/bd_project/scripts/env.sh' >> ~/bd_project/venv/bin/activate
```
**What it does.** `scripts/env.sh` finds which of the four team IPs is on this machine (so it knows if it is node1..node4) and exports `HADOOP_CONF_DIR` / `SPARK_CONF_DIR` (our own copies in `state/conf/`), the log and pid folders under `state/`, `SPARK_LOCAL_IP`, and `SERVING_DIR` on node4. The last line makes `source ~/bd_project/venv/bin/activate` load it in every new terminal. Nodes 1-3 get a venv without pip (Spark runs on the system Python 3.10; the venv is only for the activate hook); node4 installs the web service dependencies.
✅ You should see `I am node<n> with 172.23.x.x` and, on node4, `deps ok`. From now on every terminal starts with the opener shown in "How it works". ❌ [ZeroTier IP not on this machine](TROUBLESHOOTING.md#zt-ip) · [venv or dependencies](TROUBLESHOOTING.md#path-venv).

**P0-5 · [all] `/etc/hosts`: the names node1-node4**
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
**What it does.** From now on every config uses the **names** `node1`..`node4`. Why disable `127.0.1.1 <hostname>`: with it, Java resolves this machine's own name to a loopback address, so the NameNode or Spark master listens only on loopback and no other VM can connect. The last `sed` adds your hostname (your SRN) to your own line so `sudo` and Java still resolve it, now to the ZeroTier IP. Nothing else may map your hostname to 127.x.
✅ You should see your hostname resolving to your ZeroTier IP and 4× `ok`. ❌ [ping](TROUBLESHOOTING.md#ping) · [sudo: unable to resolve host](TROUBLESHOOTING.md#sudo-host).

**P0-6 · [all] firewall: let the other 3 nodes in**
```bash
sudo ufw status | head -1
for ip in $NODE1_IP $NODE2_IP $NODE3_IP $NODE4_IP; do [ "$ip" = "$MY_IP" ] || sudo ufw allow from $ip; done
sudo ufw status
```
**What it does.** HDFS and Spark use many ports (some chosen at random), so instead of a port list we trust the 3 teammates' ZeroTier IPs. If `ufw` is `inactive` the loop is harmless.
✅ `ufw status` is `inactive`, or it lists `ALLOW  <the 3 other IPs>`. ❌ [ufw](TROUBLESHOOTING.md#ufw).

**P0-7 · [all] optional: ZeroTier MTU (only if bulk transfers stall, see TROUBLESHOOTING)**
```bash
ZT_IF=$(ip -o -4 addr show | awk -v ip=$MY_IP '$4 ~ "^"ip"/" {print $2}'); echo $ZT_IF
sudo ip link set dev $ZT_IF mtu 1400
```
Not kept after a reboot. Skip unless [MTU](TROUBLESHOOTING.md#mtu) says so.

**P0-8 · [1-3] Iceberg jar**
```bash
mkdir -p $STATE_DIR/jars
JAR=iceberg-spark-runtime-3.3_2.12-1.8.1.jar
[ -f /opt/iceberg/$JAR ] && cp /opt/iceberg/$JAR $STATE_DIR/jars/ || wget -P $STATE_DIR/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.8.1/$JAR
echo "43d0915f3eeec9b732307aee8334f5a84843e568  $STATE_DIR/jars/$JAR" | sha1sum -c
```
**What it does.** Iceberg is a library for Spark. The jar is needed where a **driver** runs (node1, node3, and node2 for its `spark-sql`); Spark ships it to the executors itself (`spark.jars`). The checksum proves the download is intact.
✅ You should see `...jar: OK`.

**P0-9 · [all] Hadoop config in `state/conf/hadoop`**
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
**What it does.** Copies the course's default config, then overwrites the two files that matter. What each setting is for (you will be asked):

| Setting | Why |
|---|---|
| `fs.defaultFS = hdfs://node2:9000` | the NameNode's address. Every node, even the clients node1 and node4, needs it (it is why `hdfs dfs -ls /search` needs no `hdfs://`) |
| `hadoop.tmp.dir`, `namenode.name.dir`, `datanode.data.dir` | fresh folders in `state/hdfs`, not the course's `~/hadoop-data` |
| `dfs.replication = 2` | every block is stored on 2 DataNodes (node2 + node3). **The writer's** setting decides this, so it must be on every node |
| `rpc-bind-host = 0.0.0.0` | the NameNode listens on all interfaces, so loopback mistakes cannot hide it |
| `permissions.enabled = false` | our 4 VMs have 4 different usernames. Node 1 creates the Iceberg tables and node 3 writes into the same folders; without this node 3 gets "Permission denied" |
| `ip-hostname-check = false` | the NameNode accepts DataNodes even if reverse DNS of the ZeroTier IP is odd |
| `heartbeat.recheck-interval = 30000` | a stopped DataNode is marked dead after about 90 s instead of 10.5 min (for the failure demo) |
| `replace-datanode-on-failure = NEVER` | with only 2 DataNodes there is no spare to swap into a write pipeline; without this, writes fail while one DataNode is down |
| `HADOOP_HEAPSIZE_MAX = 512m` | the HDFS daemons get 512 MB instead of 1/4 of RAM each |

✅ You should see `hdfs://node2:9000` and `2`.

**P0-10 · [1-3] Spark config in `state/conf/spark`  ·  [4] the serving folder**
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
**What it does.** `spark-env.sh` is read by the daemons (master and worker); `spark-defaults.conf` is read by every `spark-submit` / `spark-sql` (so no `--master` or `--properties-file` is ever needed). What the settings are for:

| Setting | Why |
|---|---|
| `SPARK_LOCAL_IP`, `SPARK_PUBLIC_DNS` = your ZeroTier IP | without them Spark advertises the VM's **NAT** address (10.0.2.15, 192.168.x.x), which no other VM can reach. Symptom: *Initial job has not accepted any resources* |
| `SPARK_MASTER_HOST` = node3's IP | only used by node 3's `start-master.sh`; harmless elsewhere |
| `SPARK_WORKER_CORES/MEMORY/PORT` | the workers on node2 and node3 each offer 2 cores, 1.5 GB, and use port 40020 |
| `spark.master = spark://<node3 IP>:7077` | every job connects to the master on node3 |
| `spark.driver.host` = your IP, `bindAddress` 0.0.0.0 | executors connect **back** to the driver, so it must advertise a reachable address but listen on all |
| ports 40000 / 40001 / 40010 (+ `maxRetries`) | fixed ports make connection errors predictable and the firewall list exact |
| `shuffle.partitions = 8` | 200 tiny tasks over a ZeroTier link is what makes jobs crawl |
| `cores.max = 4` | one job takes all 4 cores, so **run one job at a time** |
| `pyspark.python = /usr/bin/python3.10` | driver and executors must run the same Python (3.10) |
| `hadoop.fs.defaultFS`, `dfs.replication` | the Spark side of the HDFS settings |
| `sql.extensions` + `catalog.iceberg_catalog.*` | turns on Iceberg: the catalog `iceberg_catalog`, type `hadoop`, tables stored under `hdfs://node2:9000/warehouse` |
| `spark.jars` | the Iceberg jar from P0-8 |
| `eventLog.*`, `history.fs.logDirectory` | finished jobs are written to HDFS `/spark-logs`; the history server on node3 reads them |
| `derby.system.home` | stops `spark-sql` from leaving `metastore_db/` folders wherever you run it |
| `log4j2.properties` level `warn` | Spark's log goes to stderr (we send it to a file); the job summaries on stdout become readable |

✅ You should see `spark.master  spark://172.23.10.141:7077` and `spark.driver.host <your IP>`.

**P0-11 · [2, 3] old course daemons must be stopped**
```bash
jps
```
We have not started anything of ours yet, so **any** `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`, `Master`, `Worker` or `HistoryServer` here is left over from the course. They hold the ports and are configured for `localhost`. Stop them with the **course** config:
```bash
(
  unset HADOOP_CONF_DIR HADOOP_LOG_DIR HADOOP_PID_DIR SPARK_CONF_DIR SPARK_LOG_DIR SPARK_PID_DIR
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
  yarn --daemon stop resourcemanager; yarn --daemon stop nodemanager
  hdfs --daemon stop secondarynamenode; hdfs --daemon stop datanode; hdfs --daemon stop namenode
  stop-history-server.sh; stop-worker.sh; stop-master.sh
)
jps
```
Last resort: `kill <pid>` from the `jps` output. **Never run `start-dfs.sh`, `start-yarn.sh` or `start-all.sh` in this project**: they SSH into the other VMs or start a second NameNode; we start each daemon by hand so you see what runs where.
✅ `jps` lists only `Jps`. ❌ [old daemons](TROUBLESHOOTING.md#old-daemons).

**P0-12 · [all] port test before any daemon runs: can node2 reach node3's driver port?**
This is the WSL2 failure (executors on node2 must reach the driver on node3, port 40000), so test it now with `nc` instead of finding out in the middle of a Spark job. On **node3**:
```bash
nc -l 40000          # waits; if your nc complains: nc -l -p 40000
```
On **node2** (and then on node1 and node4 too):
```bash
nc -zv -w 3 node3 40000
```
Then swap roles (node2 runs `nc -l 40000`, node3 runs `nc -zv -w 3 node2 40000`), and repeat for node1 ↔ node3. Stop a listener with Ctrl-C.
**What it does.** `nc -l` opens a throwaway listener; `nc -zv` only tries to connect. `succeeded` = reachable. `timed out` = a firewall or NAT drops it ([ufw](TROUBLESHOOTING.md#ufw), [WSL2](TROUBLESHOOTING.md#wsl2)). `refused` = the route works but nobody listens, which is the normal answer before the daemons run.
✅ You should see `Connection to node3 40000 port [tcp/*] succeeded!` in both directions 📸. **Do not go on until it does.** Optional bigger test (all ports + 20 MB bulk, run on all 4 at once): `python3 scripts/netcheck.py` (see [MTU](TROUBLESHOOTING.md#mtu)).

**P0-13 · [all] the checks you can repeat any time**

| Question | Command |
|---|---|
| Is my ZeroTier IP up? | `ip -4 -br a` · `sudo zerotier-cli peers` |
| Do the names resolve? | `getent hosts $(hostname)` · `getent hosts node2` |
| Can I reach everyone? | `for n in node1 node2 node3 node4; do ping -c1 -W2 $n >/dev/null && echo "$n ok" \|\| echo "$n FAIL"; done` |
| Which daemons run here? | `jps` |
| Which ports listen, and on which address? | `ss -tlnp \| grep -E ':9000\|:9866\|:7077\|:8080\|:8081\|:18080\|:8000'` (must NOT be `127.0.0.1`) |
| Can I reach the daemon ports? (after they run) | `for p in 9000 9870 9866; do nc -zv -w 3 node2 $p; done; for p in 7077 8080 9866 18080; do nc -zv -w 3 node3 $p; done` |
| Config right? | `hdfs getconf -confKey fs.defaultFS` · `grep spark.master $SPARK_CONF_DIR/spark-defaults.conf` |
| Why did a job fail? | `tail -30 state/logs/<name>.log` · the pattern table in [TROUBLESHOOTING](TROUBLESHOOTING.md#log-patterns) |

---

## Part 1 · who does what, when

"Say" = write it in the group chat; the next person waits for it.

| # | Call | Who | Tasks | Then say |
|---|---|---|---|---|
| 1 | Call 1, cluster | all | P0-1..P0-12 (port test between the nodes) | "ports OK" |
| 2 | | M2 | M2-1 format, M2-2 NameNode + folders | "NameNode up" |
| 3 | | M2 + M3 (parallel) | M2-3 DataNode + worker, M3-1 DataNode, M3-2 master + worker + history | "daemons up" |
| 4 | | M2 | M2-4 verify the cluster (2 DataNodes, 2 workers) | "cluster healthy" |
| 5 | | M1, then M3 | M1-1, M3-3 cluster_check | "check OK" |
| 6 | | M4 | M4-1 start the search service | "node4 ready" |
| 7 | Call 2, Phase 0 | M1 | M1-2 upload, M1-3 ingest, M1-4 look + re-run | "Phase 0 ingested" |
| 8 | | M3 | M3-4 index, M3-5 PageRank, M3-6 export v0 | "v0 exported" |
| 9 | | M4 | M4-2 pull v0, M4-3 queries | |
| 10 | | M2 | M2-5 report + tag phase0 (**before** Phase 1) | "go for Phase 1" |
| 11 | Call 2, Phase 1 | M1 | M1-5 ingest changes, M1-6 look + re-run | "Phase 1 ingested" |
| 12 | | M3 | M3-7 index (twice), M3-8 PageRank, M3-9 export v1 | "v1 exported" |
| 13 | | M4 | M4-4 pull v1 + queries | |
| 14 | | M2 | M2-6 tag phase1 + time travel | |
| 15 | Demos | M3 + M2 | M3-10 / M2-8 kill an executor | |
| 16 | | M3, M4 | M3-11 / M4-5 broken export refused | |
| 17 | | M4 | M4-6 switch v0 / v1 | |
| 18 | | M2 (+M3, M4) | M2-7 bad delete + rollback | |
| 19 | | M3 + M2 | M3-12 / M2-9 DataNode dies and returns | |
| 20 | | M3 | M3-13 metering | |
| 21 | End | each | M1-7, M2-10, M3-14, M4-7 stop / restart / start over | |

---

## Member 1 · node1 · Ingestion

**Role.** You are the front door of the data. You upload the raw files to HDFS, then run a Spark job that validates every line, removes duplicates, and merges the clean rows into the Iceberg `docs` table. node1 runs **no daemons**: it is an HDFS client and a Spark driver.

**You must be able to explain**
- What a Spark driver and an executor are, and why executors on node2/node3 connect *into* your driver (ports 40000/40001).
- HDFS: block, replication 2 (`Live_repl=2` in fsck), NameNode (metadata) vs DataNode (blocks), why the client writes blocks straight to DataNodes.
- Why bad lines are stored in `rejects` with a reason instead of being dropped (20 `unparseable_json`, 20 `duplicate_superseded`).
- `MERGE INTO` and idempotence: the second run finds 0 changes and the Iceberg snapshot id does not change.
- How Phase 1 changes are detected *from Iceberg* (time-travel diff between two snapshots): ADDED / UPDATED / DELETED.
- Why `changes.json` is stored in HDFS as `changes.jsonl`.

**Terminal opener** (every terminal on node1):
```bash
source ~/bd_project/venv/bin/activate
cd ~/bd_project
```

### M1-1 · Cluster smoke test (Call 1, step 5; after M2-4 said "cluster healthy")
```bash
spark-submit code/tools/cluster_check.py 2> state/logs/check.log
```
**What it does.** Starts a small Spark job with node1 as the driver. It proves: tasks run on BOTH workers, executors use Python 3.10, executors can import our shared code, and Iceberg on HDFS works (it writes one row into the table `check.hello`).
✅ You should see: `task ran on node2 ... python 3.10`, `task ran on node3 ... python 3.10`, `workers used: node2, node3 -> OK`. Table `check.hello` has **1 row** after node1 (M3 adds the 2nd) 📸.
❌ if not: [ONLY ONE WORKER](TROUBLESHOOTING.md#one-worker) · [Initial job has not accepted any resources](TROUBLESHOOTING.md#no-resources) · [Python versions](TROUBLESHOOTING.md#py-version). Log: `state/logs/check.log`.

### M1-2 · Upload Phase 0 to HDFS and look at the replication
```bash
hdfs dfs -put -f data/phase-0/pages.jsonl /search/input/phase-0/pages.jsonl
hdfs dfs -ls /search/input/phase-0
hdfs fsck /search/input/phase-0/pages.jsonl -files -blocks -locations | grep -E "Live_repl|Average block replication"
```
**What it does.** `-put` copies the local file into HDFS (`-f` overwrites an earlier copy). The path has no `hdfs://` because `fs.defaultFS` in our config is `hdfs://node2:9000`. `fsck -blocks -locations` shows where every block is stored.
✅ You should see `Live_repl=2` with the node2 AND node3 IPs, `Average block replication: 2.0` 📸. ❌ [minReplication / connection refused](TROUBLESHOOTING.md#minreplication) · [refused :9000](TROUBLESHOOTING.md#refused-9000).

### M1-3 · Ingest Phase 0
```bash
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
**What it does.** The job reads the file line by line, parses and validates each line, keeps the last copy of duplicate `doc_id`s, compares with the current table (content hash) and runs `MERGE INTO search.docs` with only the real changes. Then it finds what changed from the Iceberg snapshots, rebuilds the link graph (drops links to docs that do not exist) and stores the rejects. The summary is on screen, the Spark log in `state/logs/ingest-p0.log`.
✅ You should see 📸: lines read 1040 · rejected 20 (`unparseable_json`) · duplicates superseded 20 · accepted 1000 (20 without `links`) · changes merged 1000 · ADDED/UPDATED/DELETED 1000/0/0 · links kept 499 · dangling 0 · docs 0 → 1000.
Post the `docs snapshot ... after` id in the group chat: it is **the Phase 0 snapshot** (M2 tags it).
❌ if not: [no resources](TROUBLESHOOTING.md#no-resources) · [ClassNotFound iceberg](TROUBLESHOOTING.md#iceberg-class) · [Permission denied](TROUBLESHOOTING.md#permission).

### M1-4 · Look at the table, then run it again (idempotence)
```bash
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT phase, reason, count(*) FROM iceberg_catalog.search.rejects GROUP BY phase, reason" 2> state/logs/sql.log
$SQL -e "SELECT line_no, doc_id, reason, detail, substr(raw,1,60) FROM iceberg_catalog.search.rejects ORDER BY line_no LIMIT 25" 2> state/logs/sql.log
spark-submit code/node1_ingestion/ingest.py --phase 0 --input hdfs://node2:9000/search/input/phase-0/pages.jsonl 2> state/logs/ingest-p0.log
```
**What it does.** `SQL=...` is just a shortcut variable for this terminal. `spark-sql -e` runs one SQL statement; `--master local[2]` runs it on this machine only (a read does not need the cluster). `--conf spark.eventLog.enabled=false` stops it from writing an event-log file into HDFS `/spark-logs` (matters while a DataNode is down: such a file would have only 1 replica). The last command is Phase 0 again, on the same input.
✅ You should see 1000 · `unparseable_json 20` and `duplicate_superseded 20` 📸 · on the re-run: unchanged 1000, changes merged 0, links table rewritten False, and the docs snapshot id **before = after** 📸. Tell M3: **"Phase 0 ingested"**.
❌ [Table or view not found](TROUBLESHOOTING.md#table-not-found).

### M1-5 · Ingest Phase 1 (after M2 said "go for Phase 1")
```bash
hdfs dfs -put -f data/phase-1/changes.json /search/input/phase-1/changes.jsonl
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
**What it does.** Uploads the 100 change lines (the local file is `changes.json`, HDFS name `changes.jsonl`) and runs the same job for phase 1: `UPSERT` rows insert or replace, `DELETE` rows remove. Then the job rebuilds the links.
✅ You should see 📸: lines read 100 · accepted 100 (50 DELETE) · ADDED/UPDATED/DELETED 25/25/50 · docs 1000 → 975 · links kept 467 · dangling dropped 7. Tell M3: **"Phase 1 ingested"**.

### M1-6 · Look, then run Phase 1 again
```bash
$SQL -e "SELECT phase, change_type, count(*) FROM iceberg_catalog.search.doc_changes GROUP BY phase, change_type ORDER BY phase, change_type" 2> state/logs/sql.log
$SQL -e "SELECT doc_id, reason, detail FROM iceberg_catalog.search.rejects WHERE phase = 1" 2> state/logs/sql.log
spark-submit code/node1_ingestion/ingest.py --phase 1 --input hdfs://node2:9000/search/input/phase-1/changes.jsonl 2> state/logs/ingest-p1.log
```
(If this is a new terminal, set `SQL=...` again as in M1-4.) **What it does.** `doc_changes` is the change log that was derived from Iceberg; `rejects` for phase 1 holds the links dropped because their target is gone.
✅ You should see 📸: 25 ADDED, 25 UPDATED, 50 DELETED · 7 `dangling_link_dropped` rows · on the re-run: unchanged 50, DELETE of a doc that is not in the table 50, changes merged 0, same snapshot.

### M1-7 · Stop / another day
node1 has no daemons: nothing to stop. Another day: open a terminal (opener above) and continue. Start over from zero: see M2-10 (M2 resets the lake, then you redo M1-2 onwards).

**M1 evidence checklist 📸:** `fsck` replication of pages.jsonl · Phase 0 summary · rejects query · Phase 0 re-run (same snapshot) · Phase 1 summary · `doc_changes` query · Phase 1 re-run · `check.hello` after node1.

---

## Member 2 · node2 · Storage

**Role.** You own HDFS and the Iceberg tables. node2 runs the HDFS NameNode (the metadata master), a DataNode, and a Spark worker. You format and start HDFS, watch replication, tag the Iceberg snapshots, and show time travel and rollback.

**You must be able to explain**
- NameNode vs DataNode; what `hdfs namenode -format` creates and why doing it twice destroys HDFS (cluster ID).
- Replication 2: what `fsck` shows, under-replicated blocks when a DataNode dies, the ~90 s before the NameNode declares it dead, why nothing is re-copied with only 2 DataNodes.
- Iceberg layout in HDFS: `data/` (Parquet), `metadata/` (JSON + Avro), `version-hint.text`; snapshots; copy-on-write (why a 100-row change rewrites files and the old ones stay).
- Tags, time travel (`VERSION AS OF`), rollback (`rollback_to_snapshot`): reading old vs moving the "current" pointer.
- Why safe mode exists, and why the course's old daemons must not run next to ours.

**Terminal opener** (every terminal on node2):
```bash
source ~/bd_project/venv/bin/activate
cd ~/bd_project
```

### M2-1 · Format HDFS (once, ever)
```bash
jps
ls state/hdfs/namenode/current/VERSION
```
If `jps` shows old course daemons (ResourceManager, NodeManager, SecondaryNameNode, a NameNode that is not ours), stop them as in P0-11 first. If `ls` finds `VERSION`, HDFS is **already formatted: do not format again** (go to M2-2). Otherwise:
```bash
hdfs namenode -format -nonInteractive -force 2>&1 | grep -E "successfully formatted|ERROR|Exception"
ls state/hdfs/namenode/current/VERSION
```
**What it does.** Formatting writes an empty NameNode metadata directory (`state/hdfs/namenode`, set in our `hdfs-site.xml`) and gives the filesystem a new cluster ID. Every DataNode later remembers that ID; formatting again would make existing DataNodes refuse to join (and erase all HDFS metadata).
✅ You should see `successfully formatted` and the `VERSION` file exists. ❌ [clusterID](TROUBLESHOOTING.md#clusterid) · [port in use / old daemons](TROUBLESHOOTING.md#port-in-use).

### M2-2 · Start the NameNode and create the HDFS folders
```bash
hdfs --daemon start namenode
ss -tln | grep :9000
hdfs dfsadmin -safemode get
hdfs dfs -mkdir -p /search/input/phase-0 /search/input/phase-1 /search/exports /warehouse /spark-logs
hdfs dfs -ls -R /
```
**What it does.** Starts the NameNode in the background (its log is in `state/logs/hadoop/`). It listens on `172.23.194.191:9000`. Right after start it is in safe mode (read-only) until DataNodes report; for an empty new filesystem it leaves it by itself within ~30 s. If `-mkdir` complains about safe mode, wait 30 s and repeat, or run `hdfs dfsadmin -safemode leave`. The folders: `/search/input/...` raw files, `/search/exports` exports for node4, `/warehouse` Iceberg tables, `/spark-logs` Spark event logs (the history server reads it).
✅ You should see `0.0.0.0` or `172.23.194.191:9000` listening, `Safe mode is OFF`, the folder listing 📸. Say "NameNode up". ❌ [safe mode](TROUBLESHOOTING.md#safemode) · [port in use](TROUBLESHOOTING.md#port-in-use).

### M2-3 · Start the DataNode and the Spark worker (parallel with M3-1, M3-2)
```bash
hdfs --daemon start datanode
start-worker.sh spark://172.23.10.141:7077
jps
```
**What it does.** The DataNode stores the block files (`state/hdfs/datanode`) and reports to the NameNode at `node2:9000` (port 9866 for data). The worker registers at the Spark master on node3 (`spark://172.23.10.141:7077`); it keeps retrying until node3's master is up, so the order between M2 and M3 does not matter.
✅ You should see `jps`: `NameNode`, `DataNode`, `Worker`. ❌ [clusterID](TROUBLESHOOTING.md#clusterid) · [port in use](TROUBLESHOOTING.md#port-in-use) · [old daemons](TROUBLESHOOTING.md#old-daemons). Logs: `state/logs/hadoop/`, `state/logs/spark/`.

### M2-4 · Check that the whole cluster is healthy
```bash
hdfs dfsadmin -report | grep -E "^Live datanodes|^Dead datanodes|^Name:"
curl -s http://172.23.10.141:8080/json/ | python3 -m json.tool | grep -E '"(host|state)"'
hdfs dfsadmin -safemode get
hdfs dfs -test -d /spark-logs && echo "/spark-logs exists"
```
**What it does.** Asks the NameNode which DataNodes are alive, asks the Spark master (its status page as JSON; **the trailing slash in `/json/` matters**) which workers are ALIVE, and checks safe mode and the log folder. If a check is not ready yet, wait 30 s and run it again (DataNodes and workers need a few seconds to register).
✅ You should see 📸: `Live datanodes (2)` with the node2 and node3 IPs · 2 workers with `"state": "ALIVE"` · `Safe mode is OFF` · `/spark-logs exists`. Also 📸 `http://node3:8080` (2 workers) and `http://node2:9870` → Datanodes (2 in service). Say "cluster healthy".
❌ [DataNode shows 127.x](TROUBLESHOOTING.md#dn-ip) · [safe mode](TROUBLESHOOTING.md#safemode) · [only one worker](TROUBLESHOOTING.md#one-worker).

### M2-5 · After Phase 0 (do this BEFORE Phase 1 is ingested): look, then tag
```bash
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
hdfs dfs -ls -R /warehouse/search/docs
hdfs fsck /warehouse -files -blocks | grep -E "Total files|Average block replication|Under-replicated"
SQL="spark-sql --master local[2] --conf spark.eventLog.enabled=false"
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase0" 2> state/logs/sql.log
$SQL -e "SELECT name, type, snapshot_id FROM iceberg_catalog.search.docs.refs" 2> state/logs/sql.log
```
**What it does.** `lake_report.py` prints every table, its row count and its snapshots. `ls -R` shows the Iceberg files in HDFS (`data/` = Parquet rows, `metadata/` = JSON + Avro; `version-hint.text` says which `vN` is current). `fsck` shows replication of those files. `CREATE TAG` gives a snapshot a permanent name so we can come back to it by name.
✅ You should see 📸: `docs 1000 rows | 1 snapshots`, links 499, rejects 40, doc_stats 1000, postings 233643, pagerank 1000 (tables from M3 must exist: do this after M3-6) · `Average block replication: 2.0`, `Under-replicated blocks: 0` · tag row `phase0  TAG  <the Phase 0 snapshot id that M1 posted>`. Say "go for Phase 1".
❌ [Table or view not found](TROUBLESHOOTING.md#table-not-found) · `Table ... already exists` for a tag: the name is taken, use another ([stale export / exists](TROUBLESHOOTING.md#exists)).

### M2-6 · After Phase 1: tag, report, time travel
```bash
$SQL -e "ALTER TABLE iceberg_catalog.search.docs CREATE TAG phase1" 2> state/logs/sql.log
$SQL -e "ALTER TABLE iceberg_catalog.search.links CREATE TAG phase1" 2> state/logs/sql.log
spark-submit code/node2_storage/lake_report.py 2> state/logs/report.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs VERSION AS OF 'phase0'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT doc_id, title FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' WHERE doc_id IN ('1','100')" 2> state/logs/sql.log
$SQL -e "SELECT doc_id, title FROM iceberg_catalog.search.docs WHERE doc_id IN ('1','100')" 2> state/logs/sql.log
$SQL -e "SELECT count(*) AS deleted_in_phase1 FROM (SELECT doc_id FROM iceberg_catalog.search.docs VERSION AS OF 'phase0' EXCEPT SELECT doc_id FROM iceberg_catalog.search.docs)" 2> state/logs/sql.log
```
(`SQL=...` as in M2-5.) **What it does.** Same report, now with 2 snapshots; then the same table read at the tag `phase0` and at "now".
✅ You should see 📸: `docs 975 rows | 2 snapshots` (history: `append 1000`, then `overwrite added 975, deleted 1000`) · 1000 vs 975 · Phase 0 has `1 April` and `100 Bubonic plague`, now only `100 Bubonic plague (Phase 1)` · `deleted_in_phase1 = 50`.
Why "delete 1000, add 975" for a 100-row change? Copy-on-write: the data files (4 Parquet files here) are rewritten; the old files stay because the `phase0` snapshot points at them.

### M2-7 · Demo: a bad DELETE, then rollback (with M3 and M4)
```bash
$SQL -e "DELETE FROM iceberg_catalog.search.docs WHERE title LIKE 'C%'" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history" 2> state/logs/sql.log
```
✅ **878** rows and a 3rd snapshot. Search on node4 is unaffected (`python code/node4_search/q.py "computer science"` still works): it serves exported Parquet, never Iceberg. Now undo it:
```bash
GOOD=$($SQL -e "SELECT snapshot_id FROM iceberg_catalog.search.docs.refs WHERE name = 'phase1'" 2> state/logs/sql.log); echo "GOOD=$GOOD"
$SQL -e "CALL iceberg_catalog.system.rollback_to_snapshot('search.docs', $GOOD)" 2> state/logs/sql.log
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
$SQL -e "SELECT made_current_at, snapshot_id, is_current_ancestor FROM iceberg_catalog.search.docs.history" 2> state/logs/sql.log
```
**What it does.** The first line asks Iceberg for the snapshot id behind the tag `phase1` and keeps it in `GOOD`. The `CALL` moves the table's "current" pointer back to it. No data files are copied or deleted (time travel only reads an old snapshot; rollback moves the pointer).
✅ You should see 📸: `GOOD=` is a number · the procedure prints `<bad id>  <good id>` · count back to **975** · history has a 4th row with the good id; the bad snapshot has `is_current_ancestor = false`. Then M3 runs `build_index.py --phase 1` again (M3-7) → `mode: up-to-date`.

### M2-8 · Demo: kill a Spark executor on your node (while M3 runs M3-10)
When node3's PageRank shows `iter 2` or `iter 3`:
```bash
jps | grep CoarseGrainedExecutorBackend
kill -9 <that pid>
```
**What it does.** `CoarseGrainedExecutorBackend` is the Spark executor process of the running job on your worker. `kill -9` ends it at once, like a crashed machine. Spark notices, starts a new executor and recomputes the lost partitions from lineage.
✅ You should see a pid, then the job on node3 continues and finishes (details in M3-10) 📸. Stronger variant: `stop-worker.sh` on node2 during the job (the whole worker goes away); the job finishes on node3 alone with the same fingerprint. Then `start-worker.sh spark://172.23.10.141:7077` again.

### M2-9 · Demo: a DataNode dies (M3 stops its DataNode in M3-12)
While node3's DataNode is stopped:
```bash
hdfs dfs -cat /search/input/phase-0/pages.jsonl | wc -l
$SQL -e "SELECT count(*) FROM iceberg_catalog.search.docs" 2> state/logs/sql.log
# about 90 seconds later:
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs fsck / | grep -E "Under-replicated blocks|Average block replication|Status"
```
**What it does.** Reads must still work: HDFS reads the second copy from node2's own DataNode. About 90 s later the NameNode stops hearing heartbeats from node3 and marks it dead; the blocks that had a copy there become under-replicated.
✅ You should see 📸: 1040 and 975 · later `Live datanodes (1)`, `Dead datanodes (1)`, `Under-replicated blocks: <n> (100 %)`, `Status: HEALTHY` · `http://node2:9870` → Datanodes shows node3 dead.
After M3 starts the DataNode again, about 30 s later run the last two commands again: ✅ `Live datanodes (2)`, `Under-replicated blocks: 0`, `Average block replication: 2.0`.
(Under-replicated, not re-replicated: with 2 DataNodes and one down there is no third node to copy to. When node3 returns, its block report fixes the counts.)

### M2-10 · Stop, restart another day, start over
| Want | Run on node2 |
|---|---|
| Stop for today | `stop-worker.sh` · `hdfs --daemon stop datanode` · `hdfs --daemon stop namenode` (NameNode last; M3 stops node3 first) |
| Start another day (**no format!**) | `hdfs --daemon start namenode` · `hdfs --daemon start datanode` · `start-worker.sh spark://172.23.10.141:7077` · then M2-4 |
| Rehearse again from zero (keep HDFS, delete the lake) | `hdfs dfs -rm -r -skipTrash /warehouse/search /warehouse/check '/search/exports/*'` then `hdfs dfs -ls -R /warehouse /search/exports`; M4 empties its serving folder (M4-7); then M1-2 again |
| Delete everything of ours on a node | stop the daemons, `rm -rf ~/bd_project/state`, redo P0-8 to P0-10 (then HDFS must be formatted again: M2-1; the DataNode folder on node3 is gone with `state/` too) |
| Re-format an existing HDFS (rare) | stop everything; node2: `rm -rf ~/bd_project/state/hdfs/namenode ~/bd_project/state/hdfs/datanode` then M2-1; **node3 must also run `rm -rf ~/bd_project/state/hdfs/datanode`** before it starts (old cluster ID, see [clusterID](TROUBLESHOOTING.md#clusterid)) |

**M2 evidence checklist 📸:** M2-4 outputs + NameNode UI (2 DataNodes) · M2-5 report after Phase 0 · M2-6 report after Phase 1 and the time-travel queries · bad delete + rollback history · DataNode dead and back.

---

## Member 3 · node3 · Compute

**Role.** You run the Spark master and the heavy jobs. node3 runs the Spark master, a worker, a DataNode and the history server; you are the driver of the inverted index, PageRank and the validated export.

**You must be able to explain**
- Spark standalone: master (7077, UI 8080), workers (2 here), driver, executors; why the driver's ports are pinned (40000/40001) and why node3 runs in WSL2 with ZeroTier inside.
- The inverted index (term → doc, tf), BM25, and full vs **incremental** index (Iceberg tells which docs changed between two snapshots).
- PageRank (d = 0.85, 10 iterations, dangling-node mass spread evenly, sum of ranks = 1) written with RDDs; why the fingerprint stays the same after an executor crash (lineage recomputation).
- The export: 13 validation checks, `_SUCCESS` file, rename to `_failed_...` on failure, why a broken export is never served.
- Where time and network go (the metering demo); why 8 shuffle partitions.

**Terminal opener** (every terminal on node3):
```bash
source ~/bd_project/venv/bin/activate
cd ~/bd_project
```

### M3-1 · Start the DataNode (Call 1, parallel with M2-3)
```bash
hdfs --daemon start datanode
ss -tln | grep :9866
```
**What it does.** Starts node3's DataNode. It stores block copies and reports to the NameNode at `node2:9000`; it keeps retrying until the NameNode is up.
✅ Port 9866 listens on `172.23.10.141`. ❌ [clusterID](TROUBLESHOOTING.md#clusterid) · [port in use](TROUBLESHOOTING.md#port-in-use) · [old daemons](TROUBLESHOOTING.md#old-daemons).

### M3-2 · Start the Spark master, the worker and the history server
```bash
start-master.sh
start-worker.sh spark://172.23.10.141:7077
hdfs dfs -test -d /spark-logs && start-history-server.sh
jps
```
**What it does.** `start-master.sh` starts the cluster manager on `spark://172.23.10.141:7077` (UI `http://node3:8080`; `SPARK_MASTER_HOST` is set in our `spark-env.sh`). `start-worker.sh <master url>` starts a worker that registers there. The history server (UI `http://node3:18080`) shows finished jobs from the event logs in HDFS `/spark-logs` (set in `spark-defaults.conf`); it needs that folder, so M2-2 must be done first, otherwise repeat this line later.
✅ You should see `jps`: `DataNode`, `Master`, `Worker`, `HistoryServer`. ❌ [port in use](TROUBLESHOOTING.md#port-in-use) · [only one worker](TROUBLESHOOTING.md#one-worker). Say "daemons up". Check with M2-4: 2 workers ALIVE at `http://node3:8080`.

### M3-3 · Cluster smoke test from node3 (after M1-1), then clean up
```bash
spark-submit code/tools/cluster_check.py 2> state/logs/check.log
spark-submit code/tools/cluster_check.py --drop 2> state/logs/check-drop.log
```
**What it does.** The same test as M1-1, now with node3 as the driver. The table `check.hello` now has a second row: two drivers on two machines share one Iceberg table on HDFS. `--drop` (run it last) deletes the test table.
✅ You should see `task ran on node2 ... python 3.10`, `task ran on node3 ... python 3.10`, `workers used: node2, node3 -> OK`, and the table with **2 rows** (`node1`, `node3`) 📸 before `--drop`. ❌ [ONLY ONE WORKER](TROUBLESHOOTING.md#one-worker) · [no resources](TROUBLESHOOTING.md#no-resources) · [Python versions](TROUBLESHOOTING.md#py-version).

### M3-4 · Phase 0: build the inverted index (after "Phase 0 ingested")
```bash
spark-submit code/node3_compute/build_index.py --phase 0 2> state/logs/index-p0.log
```
**What it does.** Reads the `docs` table, turns wiki markup into terms, counts term frequency per doc and writes the tables `postings` and `doc_stats` to Iceberg. The first time it builds everything (`full`).
✅ You should see 📸: `mode: full`, docs 1000, distinct terms 37551, postings 233643, avg/max doc length 461.0 / 13903. ❌ [Table not found](TROUBLESHOOTING.md#table-not-found) · [no resources](TROUBLESHOOTING.md#no-resources).

### M3-5 · Phase 0: PageRank
```bash
spark-submit code/node3_compute/pagerank.py --phase 0 2> state/logs/pagerank-p0.log
```
**What it does.** Reads the `links` table, runs 10 PageRank iterations with RDD joins and `reduceByKey`, spreads the rank of docs without out-links evenly, writes the `pagerank` table. `--phase` is only for the run log.
✅ You should see 📸: `N=1000 docs, 499 links`, 501 dangling, sum of ranks 1.000000, max/min 0.001752 / 0.000264 (6.65), tied at minimum 501, **fingerprint `60a6221350e7`**.

### M3-6 · Phase 0: validate and export v0
```bash
spark-submit code/node3_compute/export.py --version v0 --phase 0 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v0.log
hdfs dfs -ls -R hdfs://node2:9000/search/exports/v0 | head -12
```
**What it does.** Runs 13 checks (index built from the current docs snapshot, every doc has a PageRank, ranks sum to 1, no orphan postings, ...). Only if all pass does it write plain Parquet (docs, postings) plus `manifest.json` and `_SUCCESS` to `/search/exports/v0`. If a check fails the folder is renamed `_failed_v0_<time>` and the job exits 1.
✅ You should see 📸: 13 × `PASS`, `EXPORT v0: OK`, docs / postings / terms = 1000 / 233643 / 37551, and the listing with `_SUCCESS`. Tell M4: **"v0 exported"**.
❌ [stale export](TROUBLESHOOTING.md#stale-export) · [already exists](TROUBLESHOOTING.md#exists) · [no resources](TROUBLESHOOTING.md#no-resources).

### M3-7 · Phase 1: incremental index (after "Phase 1 ingested"), twice
```bash
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
spark-submit code/node3_compute/build_index.py --phase 1 2> state/logs/index-p1.log
```
**What it does.** The first run asks Iceberg which docs differ between the snapshot of the last index and now, and re-indexes only those. The second run finds nothing to do. (`--full` would force a rebuild from scratch.)
✅ You should see 📸: first run `Iceberg says 100 docs changed ... {'UPDATED': 25, 'DELETED': 50, 'ADDED': 25}`, `mode: incremental`, docs 975, terms 36521, postings 220622 · second run `mode: up-to-date`.

### M3-8 · Phase 1: PageRank
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1.log
```
**What it does.** PageRank is global (one changed link changes everything), so it is fully recomputed.
✅ You should see 📸: `N=975 docs, 467 links`, 508 dangling, sum 1.000000, max/min 0.003926 / 0.000279 (14.08), tied at minimum 548, **fingerprint `54ee091e74b5`**.

### M3-9 · Phase 1: export v1
```bash
spark-submit code/node3_compute/export.py --version v1 --phase 1 --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1.log
hdfs dfs -ls -R hdfs://node2:9000/search/exports/v1 | head -12
```
✅ You should see 📸: 13 × `PASS`, `EXPORT v1: OK`, docs / postings / terms = 975 / 220622 / 36521. Tell M4: **"v1 exported"**.

### M3-10 · Demo: kill an executor in the middle of PageRank (with M2-8)
Terminal 1 on node3:
```bash
spark-submit code/node3_compute/pagerank.py --phase 1 --slow-seconds 3 2> state/logs/pagerank-p1-slow.log
```
M2 kills an executor on node2 when your screen shows `iter 2` or `iter 3` (M2-8). Terminal 2, after the job finished:
```bash
grep -oE "Lost executor|ExecutorLostFailure|No more replicas|FetchFailed" state/logs/pagerank-p1-slow.log | sort | uniq -c
```
**What it does.** `--slow-seconds 3` makes every task sleep 3 s so the job lasts long enough to kill something. The `grep -oE ... | sort | uniq -c` counts the failure messages in the driver log.
✅ You should see 📸 (job output, the grep, `http://node3:8080` or `:18080`): the killed iteration takes much longer, then the others are normal · the log shows `Lost executor`, `ExecutorLostFailure`, `FetchFailed` · `sum of ranks: 1.000000` and **fingerprint `54ee091e74b5`** (same as the clean run). Spark recomputed the lost partitions from lineage.
❌ if the fingerprint differs: tell the team before the demo. [TROUBLESHOOTING](TROUBLESHOOTING.md).

### M3-11 · Demo: a broken export is refused (then M4-5)
```bash
spark-submit code/node3_compute/export.py --version v1-broken --phase 1 --simulate-failure --export-root hdfs://node2:9000/search/exports 2> state/logs/export-v1-broken.log
echo "exit code $?"
hdfs dfs -ls /search/exports
```
**What it does.** `--simulate-failure` corrupts the export on purpose; the validation must catch it.
✅ You should see 📸: 3 `FAIL` lines (`every doc has a PageRank: 5 missing`, `PageRank sums to 1: 0.998...`, `every posting points at an exported doc: 1 orphan postings`), `VALIDATION FAILED - nothing published`, `exit code 1`, and in HDFS only `_failed_v1-broken_<time>` (no `v1-broken` folder). Tell M4.

### M3-12 · Demo: a DataNode dies and returns (with M2-9)
```bash
hdfs --daemon stop datanode
```
M2 runs the commands of M2-9 (reads still work; about 90 s later node3 is dead, blocks under-replicated). Then:
```bash
hdfs --daemon start datanode
ss -tln | grep :9866
```
✅ Port 9866 listens again; M2 sees `Live datanodes (2)` and `Under-replicated blocks: 0`.

### M3-13 · Demo: where the time and traffic go
```bash
ZT_IF=$(ip -o -4 addr show | awk -v ip=$MY_IP '$4 ~ "^"ip"/" {print $2}')
rx0=$(cat /sys/class/net/$ZT_IF/statistics/rx_bytes); tx0=$(cat /sys/class/net/$ZT_IF/statistics/tx_bytes)
spark-submit code/node3_compute/pagerank.py --phase 1 2> state/logs/pagerank-p1-meter.log
echo "received $(( ($(cat /sys/class/net/$ZT_IF/statistics/rx_bytes) - rx0) / 1048576 )) MB, sent $(( ($(cat /sys/class/net/$ZT_IF/statistics/tx_bytes) - tx0) / 1048576 )) MB on $ZT_IF"
```
**What it does.** Reads the byte counters of the ZeroTier interface before and after the job and prints the difference (everything that crossed that interface during the job). Then open `http://node3:18080` -> the application -> *Stages*: sort by *Duration*, look at *Shuffle Read/Write* 📸.
✅ You should see the `received ... MB, sent ... MB` line. What to say: the data is tiny, bandwidth is never the problem; PageRank is latency-bound (10 iterations, each a few small jobs with a shuffle); the index is CPU-bound (regex over 10 MB of wiki text). That is why we use 8 shuffle partitions, not 200.

### M3-14 · Stop, restart another day
| Want | Run on node3 |
|---|---|
| Stop for today (before M2 stops the NameNode) | `stop-history-server.sh` · `stop-worker.sh` · `stop-master.sh` · `hdfs --daemon stop datanode` |
| Start another day | M3-1 then M3-2 (no format) |

**M3 evidence checklist 📸:** port test between node2 and node3 (P0-12) · master UI with 2 workers · `check.hello` with 2 rows · index (full, then incremental, then up-to-date) · PageRank Phase 0 + 1 · export (13 PASS) · executor kill (output, grep, UI) · broken export · history server stages · the received/sent MB line. Save `state/logs/*.log`.

---

## Member 4 · node4 · Search

**Role.** You turn the validated export into a search engine. node4 runs no Hadoop or Spark daemons: it is an HDFS client (it copies an export out of HDFS) and runs the FastAPI service that answers queries from plain Parquet. You also own the safety rule: a bad export is refused and the old version keeps serving.

**You must be able to explain**
- Why the service reads exported Parquet, not Iceberg (Iceberg problems like the bad DELETE cannot reach search).
- The ranking: `score = 0.7 · BM25/max + 0.3 · PageRank/max` (blue and orange bars in the UI); why April is 5th ([VIVA.md](VIVA.md#why-april-is-5th)).
- What `pull_export.py` does in order: look for `_SUCCESS`, `hdfs dfs -get` into `.incoming-<v>`, verify manifest and counts, atomic rename, write `CURRENT`, ask the service to reload.
- Versions on node4 (`state/serving/v0`, `v1`, `CURRENT`) and switching without downtime.
- What happens on `REFUSED`.

**Terminal opener** (every terminal on node4; you need two: A runs the service, B runs everything else):
```bash
source ~/bd_project/venv/bin/activate
cd ~/bd_project
```

### M4-1 · Start the search service (terminal A) and test HDFS access (terminal B)
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
**What it does.** `uvicorn app:app` runs the FastAPI app `app` from `app.py`, so you must `cd` into `code/node4_search` first (that is why this terminal does not stay in `~/bd_project`). `--host 0.0.0.0` makes it reachable from the other nodes on port 8000. It stays in the foreground: stop it with Ctrl-C. The health call shows which version is loaded; the `hdfs dfs -ls` proves node4 can reach the NameNode.
✅ You should see `{"status":"no-version", ... "last_error":null}` (before the first pull); the browser at `http://node4:8000` shows the badge *no version loaded*; `hdfs dfs -ls /search` lists `exports` and `input`. Say "node4 ready".
❌ [venv or dependencies](TROUBLESHOOTING.md#path-venv) · [refused :9000](TROUBLESHOOTING.md#refused-9000). Log: `state/logs/serve.log`.

### M4-2 · Pull v0 (after "v0 exported") and search it (terminal B)
```bash
python code/node4_search/pull_export.py --version v0 --source hdfs://node2:9000/search/exports
python code/node4_search/q.py "bubonic plague"
python code/node4_search/q.py "alan turing"
python code/node4_search/q.py april
python code/node4_search/q.py ocean
python code/node4_search/q.py "computer science"
python code/node4_search/q.py zzzqqq
```
**What it does.** `pull_export.py` copies the export from HDFS into `state/serving/` (the venv sets `SERVING_DIR`), verifies it (manifest, 13 checks, file sizes, row counts), makes it the `CURRENT` version and tells the running service to reload; any failure stops it and the old version keeps serving. `q.py "words" [k]` calls the service's `/api/search` and prints the top `k` (default 5) results.
✅ You should see `verified v0: 1000 docs, 233643 postings, 13/13 checks passed` · `CURRENT: None -> v0` · `service: {'switched': True, 'version': 'v0'}` 📸. Queries:

| query | v0 top results |
|---|---|
| bubonic plague | Bubonic plague (100) 1.000 · Flea (3523) 0.622 · London (460) 0.495 |
| alan turing | Alan Turing (13) 0.907 · Mathematics (467) · List of mathematicians (3433) |
| april | March (468) 0.870 · May (469) 0.807 · Time limit (828) · ... April (1) is 5th |
| ocean | Indian Ocean (1978) 1.000 · Arctic Ocean (1980) · Pacific Ocean (1975) |
| computer science | Computer science (110) 1.000 · 1 (number) (571) · Computer (112) |
| zzzqqq | 0 matches |

❌ [venv or dependencies](TROUBLESHOOTING.md#path-venv) · [connection refused / minReplication](TROUBLESHOOTING.md#minreplication). If the service was not running, the pull still works and says so: start it (M4-1) and it loads `CURRENT`.

### M4-3 · The UI
Open `http://node4:8000`, search two queries. 📸 with the score bars visible (blue BM25, orange PageRank). Why April is 5th: [VIVA.md](VIVA.md#why-april-is-5th).

### M4-4 · Phase 1: v0 is still served, then pull v1 (after "v1 exported")
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
✅ You should see (before the pull the v0 answers; after) `verified v1: 975 docs, 220622 postings, 13/13 checks passed` · `CURRENT: v0 -> v1` · `service: {'switched': True, 'version': 'v1'}` 📸, and:

| query | v1 |
|---|---|
| bubonic plague | **Bubonic plague (Phase 1)** (100), the updated doc |
| alan turing | Alan Turing (13) is **gone** (deleted); top is List of mathematicians (3433) |
| april | 66 matches (was 71); April (1) is gone |
| phase | 69 matches (was 20): the 25 added and 25 updated pages |
| computer science | 174 matches; "Computer science (Phase 1)" (110) |

### M4-5 · Demo: a broken export is refused (after M3-11)
```bash
python code/node4_search/pull_export.py --version v1-broken --source hdfs://node2:9000/search/exports; echo "exit code $?"
curl -s localhost:8000/api/health; echo
```
**What it does.** There is no `_SUCCESS` for `v1-broken` (the failed export was renamed), so the pull refuses at step 1 and nothing changes.
✅ You should see 📸: `REFUSED: ... _SUCCESS not found ... Still serving v1.`, `exit code 1`, health still `"version":"v1"`.

### M4-6 · Demo: switch between v0 and v1
```bash
curl -s -X POST localhost:8000/api/admin/switch/v0; echo
python code/node4_search/q.py "alan turing"
curl -s -X POST localhost:8000/api/admin/switch/v1; echo
python code/node4_search/q.py "alan turing"
curl -s localhost:8000/api/versions | python3 -m json.tool
```
(Or in the UI: *Versions on this node* → *serve this*.) **What it does.** Both versions sit side by side in `state/serving/`; the POST makes the service load another one in memory.
✅ You should see Alan Turing back on v0 and gone on v1; `/api/versions` lists v0 and v1, valid, with their Iceberg snapshot ids 📸.

### M4-7 · Stop, restart another day, start over
| Want | Run on node4 |
|---|---|
| Stop for today | Ctrl-C in terminal A (the service) |
| Start another day | M4-1 (it loads the last version, `CURRENT`) |
| Rehearse again from zero | Ctrl-C the service; `rm -rf ~/bd_project/state/serving && mkdir -p ~/bd_project/state/serving`; start it again (M4-1) |

**M4 evidence checklist 📸:** UI with v0 results · `pull_export.py` v0 output · v1 pull and the same queries · REFUSED pull · version switch · `/api/versions`.

---

## Everyone: logs and the optional shortcuts

- Logs: `~/bd_project/state/logs/` (`ingest-p0.log`, `index-p1.log`, `pagerank-p1-slow.log`, `export-v0.log`, ...). Daemon logs: `state/logs/hadoop/`, `state/logs/spark/`. Read the end with `tail -30 <file>`; the error patterns and fixes are in [TROUBLESHOOTING](TROUBLESHOOTING.md#log-patterns). Save the logs for the report.
- The checks you can repeat any time are in P0-13.
- IPs changed: edit `team.env`, push, everyone `git pull`, redo P0-5 to P0-10, then restart the daemons.
