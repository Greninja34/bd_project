# 01: Setup for the cluster (every member, on their own VM)

You already did the course guides up to Spark (Java 11, SSH, Hadoop 3.3.6 single-node, Spark 3.3.4 +
Python 3.10, ZeroTier). This page turns those single-machine installs into **one 4-node cluster**.
Nothing gets reinstalled; we only change configuration.

Tags: **[all]** everyone · **[1–3]** nodes 1, 2, 3 only · **[2]** node 2 only, and so on.
Each section ends with a ✅ check. Don't go on until it passes.

---

## Fill-in sheet (write this down on the call; you'll type it a lot)

| | Value |
|---|---|
| ZeroTier network ID | `________________` |
| node1 ZeroTier IP (Member 1, ingestion) | `___.___.___.___` |
| node2 ZeroTier IP (Member 2, storage) | `___.___.___.___` |
| node3 ZeroTier IP (Member 3, compute) | `___.___.___.___` |
| node4 ZeroTier IP (Member 4, search) | `___.___.___.___` |

From §4 on, every config uses the **names** `node1`–`node4`, never raw IPs. If an IP ever changes,
you edit one line of `/etc/hosts` and nothing else.

---

## §0. Check what's already installed  [all]

```bash
java -version 2>&1 | head -1          # openjdk version "11.x"
echo $JAVA_HOME                       # /usr/lib/jvm/java-11-openjdk-amd64
hadoop version | head -1              # Hadoop 3.3.6
echo $HADOOP_CONF_DIR                 # /opt/hadoop/etc/hadoop
spark-submit --version 2>&1 | grep -m1 "version 3.3.4"     # (nodes 1–3; node 4 doesn't need Spark)
/usr/bin/python3.10 --version         # Python 3.10.x      (nodes 1–3)
ls /opt/iceberg 2>/dev/null           # the Iceberg jar, if you did the Iceberg guide (§8 otherwise)
```
If `python3.10` is at `/usr/local/bin/python3.10` (the course's source-build fallback), use that path
wherever this guide says `/usr/bin/python3.10`.

✅ every line prints what the comment says.

## §1. Put the project folder on every VM  [all]

Clone the repo to **`~/project_search_engine`** on each VM (do it at home; every command in these
guides assumes that path). It contains the code, the data, these guides and the Iceberg jar.
```bash
sudo apt install -y git
git clone git@github.com:Greninja34/bd_project.git ~/project_search_engine
#   no SSH key on GitHub? use HTTPS:  git clone https://github.com/Greninja34/bd_project.git ~/project_search_engine
ls ~/project_search_engine/code ~/project_search_engine/data
chmod +x ~/project_search_engine/code/tools/*.sh
bash ~/project_search_engine/code/tools/check_prereqs.sh node1      # ← your node
```
(Without internet: copy the folder by USB stick / shared folder to the same path.)
`check_prereqs.sh` lists everything from [`../PREREQUISITES.md`](../PREREQUISITES.md) that is
missing on this VM.
✅ you see `common node1_ingestion node2_storage node3_compute node4_search tools` and `phase-0 phase-1 toy`.

## §2. Stop the single-node daemons from the course guides  [all]

```bash
stop-yarn.sh 2>/dev/null; stop-dfs.sh 2>/dev/null
jps
```
- The course guides started a private HDFS + YARN on every VM. From now on there is **one** HDFS
  (NameNode on node2) and **no YARN** (Spark runs in its own standalone mode).
- **Never run `start-dfs.sh`, `start-yarn.sh` or `start-all.sh` again** in this project. They would
  try to SSH into the other VMs (which have different usernames) or start a second NameNode. We
  start each daemon by hand with `hdfs --daemon start ...`, so you can see exactly what runs where.

✅ `jps` lists only `Jps`.

## §3. ZeroTier  [all]

You did this in the ZeroTier guide. Recap:
```bash
sudo zerotier-cli join <NETWORK_ID>                    # → 200 join OK
```
One member authorises all 4 devices in the ZeroTier web console. Then everyone:
```bash
ip -4 -br a | grep zt                                  # your ZeroTier IP → write it in the fill-in sheet
sudo zerotier-cli peers | grep LEAF                    # DIRECT = good, RELAY = works but slower
```
✅ every VM has a `zt...` interface with an IP, and all 4 IPs are in the fill-in sheet.

## §4. `/etc/hosts`: the names node1–node4  [all]

In **every terminal** you open from now on, first set these two shell variables (only for that
terminal; retype them after opening a new one):
```bash
ME=node2                      # ← YOUR node name: node1 / node2 / node3 / node4
ME_IP=$(ip -4 -o addr show | awk '/ zt/ {split($4,a,"/"); print a[1]; exit}'); echo "$ME = $ME_IP"
```
Now write the 4 names into `/etc/hosts` (fill in the real IPs):
```bash
sudo cp /etc/hosts /etc/hosts.before-team32
sudo sed -i '/# >>> team32/,/# <<< team32/d' /etc/hosts        # remove an older copy of this block
sudo sed -i 's/^127\.0\.1\.1/#127.0.1.1/' /etc/hosts             # disable Ubuntu's 127.0.1.1 line
sudo tee -a /etc/hosts > /dev/null << EOF
# >>> team32
<node1 ZT IP>  node1
<node2 ZT IP>  node2
<node3 ZT IP>  node3
<node4 ZT IP>  node4
# <<< team32
EOF
sudo sed -i "/[[:space:]]${ME}\$/ s/\$/ $(hostname)/" /etc/hosts   # your own line also gets your hostname
cat /etc/hosts
```
- **Why disable `127.0.1.1 <hostname>`:** with it, Java resolves this machine's own name to a
  loopback address. The NameNode or Spark master then listens only on loopback, and no other VM
  can connect. This caused most of A2's trouble.
- The last `sed` adds your hostname (your SRN) to **your own** line, so `sudo` and Java can still
  resolve it, now to the ZeroTier IP.

```bash
for n in node1 node2 node3 node4; do ping -c1 -W2 $n >/dev/null && echo "$n ok" || echo "$n FAIL"; done
getent hosts $(hostname)                                         # → your ZeroTier IP, not 127.0.1.1
```
✅ 4× `ok`, and your hostname resolves to your ZeroTier IP.

## §5. Firewall: open everything on the ZeroTier interface only  [all]

The course's prerequisites guide enabled `ufw` with only SSH allowed. HDFS and Spark use many
ports (some random), so we trust the ZeroTier interface as a whole. Only your 3 teammates are on it.
```bash
sudo ufw status | head -1
ZT_IF=$(ip -o link show | awk -F': ' '$2 ~ /^zt/ {print $2; exit}'); echo $ZT_IF
sudo ufw allow in on "$ZT_IF"
sudo ufw status
```
✅ `ufw status` is `inactive`, or it lists `Anywhere on zt...  ALLOW`.

## §6. Hadoop: every node points at the NameNode on node2  [all]

Back up the single-node config, then write the cluster config. The heredocs are unquoted (`<< EOF`),
so `$HOME` is filled in for you.
```bash
cd $HADOOP_CONF_DIR
cp core-site.xml core-site.xml.single-node; cp hdfs-site.xml hdfs-site.xml.single-node

cat > core-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://node2:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$HOME/hadoop-data/tmp</value></property>
</configuration>
EOF

cat > hdfs-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$HOME/hadoop-data/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$HOME/hadoop-data/datanode</value></property>
  <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
  <property><name>dfs.namenode.heartbeat.recheck-interval</name><value>30000</value></property>
  <property><name>dfs.client.block.write.replace-datanode-on-failure.policy</name><value>NEVER</value></property>
</configuration>
EOF
cd ~
hdfs getconf -confKey fs.defaultFS; hdfs getconf -confKey dfs.replication
```
What each setting is for (you'll be asked):

| Setting | Why |
|---|---|
| `fs.defaultFS = hdfs://node2:9000` | the NameNode's address. Every node, even the clients node1 and node4, needs it |
| `dfs.replication = 2` | every block is stored on 2 DataNodes (node2 + node3). **The writer's** setting decides this, so it must be on every node |
| `rpc-bind-host = 0.0.0.0` | the NameNode listens on all interfaces, so loopback mistakes can't hide it |
| `permissions.enabled = false` | our 4 VMs have 4 different usernames (SRNs). Node 1 creates the Iceberg tables and node 3 writes into the same folders. Without this, node 3 gets "Permission denied" |
| `ip-hostname-check = false` | the NameNode accepts DataNodes even if reverse DNS of the ZeroTier IP is odd |
| `heartbeat.recheck-interval = 30000` | a stopped DataNode is marked dead after about 90 s instead of 10.5 min (for the failure demo) |
| `replace-datanode-on-failure = NEVER` | with only 2 DataNodes there's no spare to swap into a write pipeline. Without this, writes fail while one DataNode is down |

**[2, 3] only:** give the HDFS daemons less memory (default is 1/4 of RAM each):
```bash
grep -q HADOOP_HEAPSIZE_MAX $HADOOP_CONF_DIR/hadoop-env.sh || echo 'export HADOOP_HEAPSIZE_MAX=512m' >> $HADOOP_CONF_DIR/hadoop-env.sh
```
✅ `hdfs getconf` prints `hdfs://node2:9000` and `2`.

## §7. Spark: cluster settings  [1–3]

(Node 4 doesn't run Spark. Skip to §9.)

### 7a. `spark-env.sh`: which address this node uses
```bash
sed -i '/# >>> team32/,/# <<< team32/d' $SPARK_HOME/conf/spark-env.sh
cat >> $SPARK_HOME/conf/spark-env.sh << EOF
# >>> team32
export SPARK_LOCAL_IP=\${SPARK_LOCAL_IP:-$ME_IP}
export SPARK_LOCAL_HOSTNAME=\${SPARK_LOCAL_HOSTNAME:-$ME}
export SPARK_MASTER_HOST=node3
export SPARK_DAEMON_MEMORY=512m
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=1536m
export PYSPARK_PYTHON=/usr/bin/python3.10
# <<< team32
EOF
tail -10 $SPARK_HOME/conf/spark-env.sh
```
- `SPARK_LOCAL_IP` / `SPARK_LOCAL_HOSTNAME`: without them Spark advertises the VM's **NAT**
  address (`10.0.2.15`, `192.168.x.x`), which no other VM can reach. The usual symptom is a job
  stuck on *"Initial job has not accepted any resources"*. The `${...:-...}` form means "unless
  already set", so solo practice can override it with `SPARK_LOCAL_IP=127.0.0.1`.
  `tail` must show your real ZeroTier IP inside the `${...}`.
- `SPARK_MASTER_HOST=node3`: only used by node 3's `start-master.sh`; harmless elsewhere.
- `SPARK_WORKER_*`: used by the workers on nodes 2 and 3. Each offers 2 cores and 1.5 GB to Spark.

### 7b. Quieter logs (optional, recommended)
```bash
cp -n $SPARK_HOME/conf/log4j2.properties.template $SPARK_HOME/conf/log4j2.properties
sed -i 's/^rootLogger.level = info/rootLogger.level = warn/' $SPARK_HOME/conf/log4j2.properties
```
Spark's logs go to stderr, so the jobs' own summaries on stdout become readable.

### 7c. `~/spark-config/search.conf`: the one config file every job uses
```bash
mkdir -p ~/spark-config
cat > ~/spark-config/search.conf << EOF
spark.master                              spark://node3:7077
spark.driver.host                         $ME
spark.driver.port                         40000
spark.driver.blockManager.port            40001
spark.blockManager.port                   40010
spark.port.maxRetries                     16
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
spark.jars                                /opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar
spark.eventLog.enabled                    true
spark.eventLog.dir                        hdfs://node2:9000/spark-logs
spark.driver.extraJavaOptions             -Dderby.system.home=/tmp/spark-derby-$USER
spark.ui.showConsoleProgress              false
EOF
cat ~/spark-config/search.conf | head -3
```
- `--properties-file ~/spark-config/search.conf` **replaces** `spark-defaults.conf` (whose
  `spark.master local[*]` is for the course's hands-ons). Your old `~/spark-config/iceberg.conf`
  is left alone.
- `spark.driver.host` differs per node (node1 for ingestion, node3 for index/PageRank/export).
  Executors connect **back** to the driver, so the driver must advertise a reachable name.
- Fixed ports (40000, 40001, 40010): connection errors become predictable.
- Shuffle partitions 8 instead of 200: 200 tiny tasks over a relayed ZeroTier link is what makes jobs crawl.
- `spark.cores.max 4`: one job takes all 4 cores, so **run one job at a time**.
- Event logs go to HDFS, so the history server on node3 can show finished jobs (where the time went).
- The `derby` line stops `spark-sql` from leaving `metastore_db/` folders wherever you run it.

✅ `head -3` shows `spark://node3:7077` and `spark.driver.host  node<you>`.

## §8. Iceberg jar  [1–3]  (at home, needs internet)

Skip if `ls /opt/iceberg` already shows `iceberg-spark-runtime-3.3_2.12-1.8.1.jar`.
```bash
sudo mkdir -p /opt/iceberg && sudo chown -R "$USER":"$USER" /opt/iceberg
wget -P /opt/iceberg https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.8.1/iceberg-spark-runtime-3.3_2.12-1.8.1.jar
echo "43d0915f3eeec9b732307aee8334f5a84843e568  /opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar" | sha1sum -c
```
The jar is needed where a **driver** runs (node1, node3, and node2 for its `spark-sql` demos).
Spark ships it to the executors itself (`spark.jars`).

✅ `sha1sum -c` prints `OK`.

## §8b. numpy for the Word2Vec driver  [3]  (NLP twist; at home, needs internet)

Spark's ML library (`pyspark.ml`, used by `word2vec.py`) needs numpy in the **driver's** Python.
The executors don't, because the training itself runs in the JVM. So only node 3 gets a small
Python 3.10 venv. The system Python stays untouched, and so do the other nodes.
```bash
python3.10 -m venv ~/nlp-venv
~/nlp-venv/bin/pip install numpy==1.26.4
~/nlp-venv/bin/python -c "import numpy, sys; print(sys.version.split()[0], numpy.__version__)"
```
✅ `3.10.x 1.26.4`. (numpy 1.26, not 2.x: Spark 3.3.4's Python code predates numpy 2.)

## §9. The doctor  [all]

```bash
bash ~/project_search_engine/code/tools/doctor.sh
```
Read-only. It prints your ZeroTier IP, `/etc/hosts`, pings, Java daemons and which cluster ports
answer. **Run it first whenever anything breaks.** Before the cluster is started, the port checks
fail; that's expected. After [`06-TEAM-RUN-ORDER.md`](06-TEAM-RUN-ORDER.md) step 6, everything should be OK.

✅ the "this machine", `/etc/hosts` and ping sections are all OK.

---

## If something breaks

| Symptom | Cause → fix |
|---|---|
| `ping node2` fails | ZeroTier not authorised / not joined (`sudo zerotier-cli listnetworks` must say `OK`), or a typo in `/etc/hosts` |
| `sudo: unable to resolve host <srn>` | the last `sed` of §4 didn't match. Add your hostname to your own line by hand: `sudo nano /etc/hosts` |
| `Connection refused node2:9000` | NameNode not running on node2, or it listens on loopback (`ss -tlnp \| grep 9000` on node2 must show `0.0.0.0:9000`) |
| `Initial job has not accepted any resources` | the workers can't reach the driver, or there aren't enough free cores/memory. Check `SPARK_LOCAL_IP`, `spark.driver.host`, ufw, and that no other job is running (node3:8080) |
| `Permission denied: user=...` in HDFS | `dfs.permissions.enabled=false` missing on **node2** (the NameNode decides). Fix it, then restart the NameNode |
| `python3.10: not found` on an executor | Python 3.10 missing on node2 or node3, or at another path. Fix `spark.pyspark.python` |
