# TROUBLESHOOTING

Find your symptom (the exact error text, or what you see). Each entry: **cause**, then the commands.
**Start here:** run `bash ~/bd_project/scripts/doctor.sh` and paste the whole output in the group chat. It is read-only and prints, per section, OK / WARN / FAIL with the fix, the last ERROR/WARN lines of every daemon log annotated from the [log pattern table](#log-patterns), and the HDFS / Spark view from this node. After a failed GATE, `bash ~/bd_project/scripts/check.sh <stage>` (also read-only) tells you which single check failed and which section below applies. `scripts/netcheck.py` is an optional bigger network test.
Other first commands: `jps` (which daemons run), `ss -tlnp` (which ports listen), `tail -30 ~/bd_project/state/logs/<name>.log` (the job or daemon log).

Contents: [path / venv](#path-venv) · [ZeroTier IP not found](#zt-ip) · [ping](#ping) · [duplicate node IDs](#dup-id) · [MTU / RELAY](#mtu) · [ufw](#ufw) · [sudo host](#sudo-host) · [old daemons](#old-daemons) · [port in use](#port-in-use) · [clusterID](#clusterid) · [safe mode](#safemode) · [minReplication](#minreplication) · [DataNode wrong IP](#dn-ip) · [refused :9000](#refused-9000) · [Permission denied](#permission) · [no resources](#no-resources) · [only one worker](#one-worker) · [Python versions](#py-version) · [python3.10 missing](#py-missing) · [Table not found](#table-not-found) · [ClassNotFound iceberg](#iceberg-class) · [stale export](#stale-export) · [already exists](#exists) · [solo leftovers](#solo-leftover) · [node3 on WSL2](#wsl2) · [log patterns](#log-patterns) · [checks](#checks) · [versions](#versions) · [conf](#conf) · [dn-missing](#dn-missing) · [master-down](#master-down) · [export-bad](#export-bad) · [serve](#serve) · [not tested](#not-tested)

---

<a id="path-venv"></a>
## `No such file or directory` for a script, `ModuleNotFoundError: fastapi`, pip fails

**Cause:** the repo is not where you think, you did not activate the venv, or the node4 dependencies are not installed.
```bash
ls ~/bd_project                                      # clone: git clone https://github.com/Greninja34/bd_project.git ~/bd_project
source ~/bd_project/venv/bin/activate                # in EVERY new terminal; prompt shows (nodeN)
# no venv yet: RUNBOOK S04 (creates the venv, installs the node4 deps)
```
node4: `python3 -m venv` failing means `sudo apt install -y python3-venv`, then redo RUNBOOK S04. `pip` failing means no internet: redo S04 when online. Test: `~/bd_project/venv/bin/python -c "import fastapi, uvicorn, pandas, pyarrow"`.
`No space left on device`: `df -h`.

<a id="zt-ip"></a>
## `ZeroTier IP not on this machine`

**Cause:** none of the 4 IPs in `team.env` is on any interface here.
```bash
ip -4 -br a                                          # is your 172.23.x.x there?
sudo zerotier-cli listnetworks                       # status must be OK; ACCESS_DENIED = not authorized in Central
sudo systemctl status zerotier-one
cat ~/bd_project/team.env                            # same file on every node? git pull
```
In ZeroTier Central the member needs its IP set by hand and "Auth" ticked. On WSL2 see [node3 on WSL2](#wsl2).

<a id="ping"></a>
## `ping node2` fails / `nothing answers` in netcheck.py

**Cause:** ZeroTier not joined or not authorized, wrong `/etc/hosts`, or the other node is off.
```bash
sudo zerotier-cli listnetworks                       # OK?
ping -c2 172.23.194.191                              # by IP: works? then /etc/hosts is wrong -> RUNBOOK S05
getent hosts node2                                   # must print node2's ZeroTier IP
sudo zerotier-cli peers | grep LEAF                  # UDP 9993 must not be blocked by your network
```

<a id="dup-id"></a>
## Two machines have the same ZeroTier node ID (cloned VMs)

**Cause:** a VM cloned from another one copied `/var/lib/zerotier-one/identity.*`. Both machines fight over one identity: random timeouts, wrong IPs. `sudo zerotier-cli info` prints the 10-digit node ID of each machine; compare them in the group chat.
On the clone:
```bash
sudo systemctl stop zerotier-one
sudo rm /var/lib/zerotier-one/identity.public /var/lib/zerotier-one/identity.secret
sudo systemctl start zerotier-one
sudo zerotier-cli info                               # new ID
sudo zerotier-cli join <network id>
```
Then authorize the new member in Central, set its IP by hand, delete the old duplicate member, then check `ip -4 -br a`.

<a id="mtu"></a>
## Pings work but HDFS/Spark hang; `bulk transfer stalls`; `peers` shows RELAY

**Cause:** ZeroTier's default MTU is 2800. Small packets pass, big ones get lost: connections open but data transfers freeze. Or traffic goes through a RELAY (slow) because UDP 9993 is blocked.
```bash
sudo zerotier-cli peers | grep LEAF                  # DIRECT is good, RELAY is slow
ip link show | grep -A1 zt                           # current mtu
```
Fix: on **every** node set the interface MTU (RUNBOOK S07: `sudo ip link set dev $ZT_IF mtu 1400`; run it again after a reboot). Then test again with `python3 scripts/netcheck.py` on all 4 nodes at the same time (optional diagnostic: opens test listeners on every port, probes the other nodes and sends 20 MB to each; it ends with a matrix and a hint per failure).
Still RELAY: another network or a phone hotspot usually gives DIRECT. The university wifi often blocks UDP.

<a id="ufw"></a>
## Some ports blocked, others open (`nc -zv` times out, netcheck.py shows a few ❌); ufw

**Cause:** `ufw` is enabled with only SSH allowed.
```bash
sudo ufw status
for ip in $NODE1_IP $NODE2_IP $NODE3_IP $NODE4_IP; do [ "$ip" = "$MY_IP" ] || sudo ufw allow from $ip; done   # RUNBOOK S06
nc -zv -w 3 node3 40000                              # from another node: timed out = still blocked
```
Both directions matter: node A can reach node B does not prove B can reach A. `netcheck.py` tests both.

<a id="sudo-host"></a>
## `sudo: unable to resolve host <srn>`, or `sudo` cannot be used

**Cause:** the hostname is not in `/etc/hosts` (after our edit it is on your own node line). Redo RUNBOOK S05, or by hand:
```bash
echo "$(hostname -I | awk '{print $1}') $(hostname)" | sudo tee -a /etc/hosts
```
If your user has no sudo, ask a teammate or your admin to run the `sudo` commands of the RUNBOOK for you.

<a id="old-daemons"></a>
## Old course daemons (ResourceManager, NodeManager, SecondaryNameNode, a 2nd NameNode) still running

**Cause:** the course's single-node setup started HDFS and YARN on this VM. They hold ports and are configured for `localhost`.
```bash
jps                                                  # before our daemons run: ANY NameNode/DataNode/Master/Worker/... is an old one
(
  unset HADOOP_CONF_DIR HADOOP_LOG_DIR HADOOP_PID_DIR SPARK_CONF_DIR SPARK_LOG_DIR SPARK_PID_DIR
  export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop      # the COURSE config, so the stop commands find the old pid files
  yarn --daemon stop resourcemanager; yarn --daemon stop nodemanager
  hdfs --daemon stop secondarynamenode; hdfs --daemon stop datanode; hdfs --daemon stop namenode
  stop-history-server.sh; stop-worker.sh; stop-master.sh
)
jps
kill <pid>                                           # last resort, from the jps output
```
Never run `start-dfs.sh`, `start-yarn.sh`, `start-all.sh` in this project.

<a id="port-in-use"></a>
## `BindException`, `Address already in use`

**Cause:** another process owns the port (usually an old daemon, or our own daemon started twice after a crash).
```bash
ss -tlnp | grep -E ':9000|:9866|:7077|:8080|:8081|:18080'
jps
# old course daemon: see "Old course daemons" above. Ours: stop it with its own command (hdfs --daemon stop ..., stop-worker.sh, ...), then start it again
```

<a id="clusterid"></a>
## `Incompatible clusterIDs` (DataNode does not start or never appears)

**Cause:** the DataNode's data dir belongs to a different (older) NameNode. Happens after HDFS was formatted again (RUNBOOK S18), or if an old dir is reused. Our dirs are fresh (`state/hdfs`), so it means HDFS was formatted twice.
On the node that shows it (it holds no precious data; the blocks are copied again):
```bash
hdfs --daemon stop datanode
rm -rf ~/bd_project/state/hdfs/datanode              # this DataNode's block copies only; HDFS copies them back
hdfs --daemon start datanode
hdfs dfsadmin -report | grep -E "^Live datanodes|^Name:"
```

<a id="safemode"></a>
## `Name node is in safe mode` / `SafeModeException`

**Cause:** normal for about 30 s after the NameNode starts (and until DataNodes report). 
```bash
hdfs dfsadmin -safemode get
# wait 30 s and ask again; it needs both DataNodes to report
```
If it stays ON with both DataNodes live: `hdfs dfsadmin -safemode leave` (last resort).

<a id="minreplication"></a>
## `could only be written to 0 of the 1 minReplication nodes` (also `put` hangs)

**Cause:** the client reached the NameNode (port 9000) but cannot reach any DataNode on **port 9866**.
```bash
for n in node2 node3; do nc -zv -w 3 $n 9866; done   # from the failing node: succeeded? timed out = firewall
hdfs dfsadmin -report | grep -E "^Live datanodes|^Name:"   # both DataNodes live?
sudo ufw status
```
On node3/WSL2: [node3 on WSL2](#wsl2). Big writes stalling while small things work: [MTU](#mtu).

<a id="dn-ip"></a>
## A DataNode is listed with `127.x.x.x` or a NAT address (10.0.2.x, 192.168.x.x)

**Cause:** `/etc/hosts` on that node maps its hostname to 127.0.1.1 (or a container/WSL address), or the NameNode sees a connection from the wrong interface.
```bash
getent hosts $(hostname)                             # must be the ZeroTier IP
# fix /etc/hosts first (RUNBOOK S05), then restart the DataNode on that node:
hdfs --daemon stop datanode ; hdfs --daemon start datanode
hdfs dfsadmin -report | grep -E "^Live datanodes|^Name:"
```
On WSL, `generateHosts=false` in `/etc/wsl.conf` must be set (RUNBOOK S02) and `wsl --shutdown` run once.

<a id="refused-9000"></a>
## `Connection refused` to `node2:9000`

**Cause:** the NameNode is not running, or listens only on loopback, or the firewall blocks 9000.
```bash
# on node2
jps | grep NameNode ; ss -tlnp | grep 9000           # must show 172.23.194.191:9000 or 0.0.0.0:9000, not 127.x
hdfs --daemon start namenode                         # if it was not running
# on the failing node
getent hosts node2 ; nc -zv -w 3 node2 9000
```

<a id="permission"></a>
## `Permission denied: user=...` in HDFS

**Cause:** our 4 VMs have 4 different usernames. HDFS permission checking must be off **on node2** (the NameNode decides).
```bash
grep -A1 permissions ~/bd_project/state/conf/hadoop/hdfs-site.xml     # on node2: false
# if it says true: set it to false in hdfs-site.xml (RUNBOOK S09), then on node2:
hdfs --daemon stop namenode ; hdfs --daemon stop datanode
hdfs --daemon start namenode ; hdfs --daemon start datanode
```

<a id="no-resources"></a>
## `Initial job has not accepted any resources`

**Cause:** no executor started or no executor could connect back to the driver, or another job holds all 4 cores.
```bash
curl -s http://172.23.10.141:8080/json/ | python3 -m json.tool | grep -E '"(host|state)"'   # 2 ALIVE workers?
# open http://172.23.10.141:8080 : is another application RUNNING? kill/finish it
nc -zv -w 3 node3 40000                              # run on node2 while node3 does: nc -l 40000   (executors connect INTO the driver port 40000)
tail -30 ~/bd_project/state/logs/<the job>.log
```
Most often a firewall on the **driver** node (ufw, or WSL2: see [node3 on WSL2](#wsl2)).

<a id="one-worker"></a>
## `workers used: node3 -> ONLY ONE WORKER` although `node3:8080` lists 2 workers

**Cause:** the master (7077/8080) is reachable, but the other worker's executor cannot connect **back to the driver** (port 40000/40001) or its block manager port. The master UI cannot tell you that. This is exactly the WSL2 NAT problem.
```bash
nc -l 40000                                          # on node3 (the driver) ...
nc -zv -w 3 node3 40000                              # ... and this on node2: must say succeeded
# bigger test: python3 scripts/netcheck.py on all 4 nodes together. Look for "node2 -> node3: never reached my ports [40000 ...]"
```
- Driver on node3/WSL2: [node3 on WSL2](#wsl2) (ZeroTier must run inside Ubuntu).
- Otherwise: [ufw](#ufw) on the driver node, then run the cluster_check again (RUNBOOK S26).
- The check waits 120 s because executors start one after the other. If it prints the diagnosis, a retry will not help.

<a id="py-version"></a>
## `Python in worker has different version 3.x than that in driver 3.10`

**Cause:** an executor started another Python (Spark 3.3.4 needs 3.10, Ubuntu 24.04's default is 3.12). Executors use the **driver's** path `/usr/bin/python3.10`.
```bash
/usr/bin/python3.10 --version                        # on node1, node2, node3
sudo ln -s /usr/local/bin/python3.10 /usr/bin/python3.10   # if python3.10 lives elsewhere
grep python ~/bd_project/state/conf/spark/spark-env.sh ~/bd_project/state/conf/spark/spark-defaults.conf
```
Restart the worker on that node: `stop-worker.sh`, then `start-worker.sh spark://172.23.10.141:7077`.

<a id="py-missing"></a>
## `python3.10: not found` / `Cannot run program "/usr/bin/python3.10"` on an executor

**Cause:** Python 3.10 is missing on node2/node3, or only at `/usr/local/bin/python3.10`.
```bash
ls -l /usr/bin/python3.10 /usr/local/bin/python3.10
sudo ln -s /usr/local/bin/python3.10 /usr/bin/python3.10
```

<a id="table-not-found"></a>
## `Table or view not found` / `TABLE_OR_VIEW_NOT_FOUND`

**Cause:** the step that creates the table failed or has not run. `docs/links/rejects` come from `ingest.py`, `postings/doc_stats` from `build_index.py`, `pagerank` from `pagerank.py`.
```bash
tail -30 ~/bd_project/state/logs/ingest-p0.log       # read the earlier job's log
spark-sql --master local[2] --conf spark.eventLog.enabled=false -e "SHOW TABLES IN iceberg_catalog.search" 2> state/logs/sql.log
```
Run the earlier step again.

<a id="iceberg-class"></a>
## `ClassNotFoundException: org.apache.iceberg...` / `Cannot find catalog plugin class`

**Cause:** the Iceberg jar is missing in `state/jars/`.
```bash
ls ~/bd_project/state/jars/                          # iceberg-spark-runtime-3.3_2.12-1.8.1.jar
# missing: redo RUNBOOK S08 (copy from /opt/iceberg or download, then sha1sum -c)
```

<a id="stale-export"></a>
## export: `FAIL index built from the current docs snapshot`

**Cause:** the `docs` table changed after the index/PageRank ran (a new ingest, or a rollback). The export refuses stale inputs on purpose.
```bash
spark-submit code/node3_compute/build_index.py --phase N 2> state/logs/index-pN.log
spark-submit code/node3_compute/pagerank.py --phase N 2> state/logs/pagerank-pN.log
spark-submit code/node3_compute/export.py --version vN --phase N --export-root hdfs://node2:9000/serving 2> state/logs/export-vN.log
```

<a id="exists"></a>
## `.../v0 already exists` / `already exists`

**Cause:** that export version is in HDFS already.
```bash
spark-submit code/node3_compute/export.py --version v0 --phase 0 --overwrite --export-root hdfs://node2:9000/serving 2> state/logs/export-v0.log   # old one is kept as _replaced_v0_<time>
# or use a new name, e.g. v2
```
`Table ... already exists` from `ALTER TABLE ... CREATE TAG`: the tag name is taken; use another name.

<a id="solo-leftover"></a>
## Spark binds to `127.0.0.1`, or `~/solo` exists (old solo-practice guide)

**Cause:** the old guide exported `SPARK_LOCAL_IP=127.0.0.1` and left `~/solo`. `source .../activate` removes the variable in the terminal, but not from `~/.bashrc`.
```bash
grep -n SPARK_LOCAL ~/.bashrc ~/.profile
sed -i '/SPARK_LOCAL_\(IP\|HOSTNAME\)/d' ~/.bashrc ; rm -rf ~/solo
# open a NEW terminal, then:
source ~/bd_project/venv/bin/activate ; echo $SPARK_LOCAL_IP        # your ZeroTier IP
```

---

<a id="checks"></a>
## The checks you can repeat any time

| Question | Command |
|---|---|
| Everything on this node, one paste for the chat | `bash ~/bd_project/scripts/doctor.sh` |
| Did this RUNBOOK stage work? | `bash ~/bd_project/scripts/check.sh <setup\|network\|hdfs\|spark\|ingest0\|ingest1\|index\|export vN\|serve\|pulled vN\|down\|all>` |
| Is my ZeroTier IP up? | `ip -4 -br a` · `sudo zerotier-cli peers` |
| Do the names resolve? | `getent hosts $(hostname)` · `getent hosts node2` |
| Can I reach everyone? | `for n in node1 node2 node3 node4; do ping -c1 -W2 $n >/dev/null && echo "$n ok" \|\| echo "$n FAIL"; done` |
| Which daemons run here? | `jps` |
| Which ports listen, and on which address? | `ss -tlnp \| grep -E ':9000\|:9866\|:7077\|:8080\|:8081\|:18080\|:8000'` (must NOT be `127.0.0.1`) |
| Can I reach the daemon ports? | `for p in 9000 9870 9866; do nc -zv -w 3 node2 $p; done; for p in 7077 8080 9866 18080; do nc -zv -w 3 node3 $p; done` |
| Config right? | `hdfs getconf -confKey fs.defaultFS` · `grep spark.master $SPARK_CONF_DIR/spark-defaults.conf` |
| Why did a job fail? | `tail -30 state/logs/<name>.log` · the [pattern table](#log-patterns) |

<a id="versions"></a>
## `check.sh setup`: wrong Java / Hadoop / Spark version, or a missing tool

**Cause:** the course install is not where `env.sh` expects it (`/usr/lib/jvm/java-11-openjdk-amd64`, `/opt/hadoop`, `/opt/spark`), or it is another version.
```bash
java -version 2>&1 | head -1 ; hadoop version | head -1 ; spark-submit --version 2>&1 | grep -m1 version
echo $JAVA_HOME $HADOOP_HOME $SPARK_HOME
sudo apt install -y git python3-venv netcat-openbsd curl
```
Pinned: Java 11, Hadoop 3.3.6, Spark 3.3.4 (nodes 1-3), Python 3.10 at `/usr/bin/python3.10` (nodes 1-3).

<a id="conf"></a>
## `check.sh setup`: a generated config value is wrong or the config folder is missing

**Cause:** RUNBOOK S09 / S10 were skipped, run on another node, or `team.env` changed afterwards.
```bash
source ~/bd_project/scripts/env.sh ; echo $HADOOP_CONF_DIR $SPARK_CONF_DIR
# redo RUNBOOK S09 (all nodes) and S10 (nodes 1-3: Spark config; node4: serving folder), then open a new terminal
```

<a id="dn-missing"></a>
## `check.sh hdfs`: fewer than 2 live DataNodes

**Cause:** a DataNode is not running, still registering (wait 30 s), has a wrong cluster ID, or the NameNode sees it under a wrong address.
```bash
jps | grep DataNode                                  # on node2 and node3
hdfs --daemon start datanode                         # on the node without one
tail -30 ~/bd_project/state/logs/hadoop/*datanode*.log
```
Then [clusterID](#clusterid), [DataNode wrong IP](#dn-ip), [minReplication](#minreplication).

<a id="master-down"></a>
## `check.sh spark`: the master does not answer on node3:7077 / its UI json is unreadable

**Cause:** the master is not running on node3, listens on loopback, or the firewall blocks 7077/8080.
```bash
# on node3
jps | grep Master ; ss -tlnp | grep -E ':7077|:8080'      # 172.23.10.141, not 127.x
start-master.sh
# from another node
nc -zv -w 3 node3 7077 ; curl -s http://172.23.10.141:8080/json/ | head -c 200     # the trailing slash matters
```

<a id="export-bad"></a>
## `check.sh export vN` / `pulled vN`: manifest not OK, `_READY` missing, counts wrong

**Cause:** the export job failed (the folder is then renamed `_failed_vN_<time>` and there is no `_READY`), is still running, or was made from the wrong phase.
```bash
hdfs dfs -ls /serving
tail -40 ~/bd_project/state/logs/export-vN.log       # the 13 checks: which one is FAIL?
```
Fix the cause (usually [stale export](#stale-export)), export again (RUNBOOK S35 / S46), pull again (S37 / S48). The v1-broken demo folder is supposed to look like this.

<a id="serve"></a>
## `check.sh serve` / `pulled`: the search service does not answer, `no-version`, or the pull was REFUSED

**Cause:** uvicorn is not running on node4 (it must be started from `code/node4_search`), no export pulled yet, or `pull_export.py` refused (read its last line).
```bash
# on node4
curl -s localhost:8000/api/health ; echo
ss -tlnp | grep :8000                                 # must be 0.0.0.0:8000
cd ~/bd_project/code/node4_search && uvicorn app:app --host 0.0.0.0 --port 8000 2>&1 | tee ~/bd_project/state/logs/serve.log
python code/node4_search/pull_export.py --version v1 --source hdfs://node2:9000/serving     # from ~/bd_project
```
`REFUSED: ... _READY not found`: the export is not finished or failed ([export-bad](#export-bad)). The old version keeps serving.

---

<a id="wsl2"></a>
## node3 on WSL2

WSL2 runs Ubuntu in a small VM behind a NAT. Spark executors on node2 must connect **into** node3's driver (port 40000) and block manager. WSL2's NAT and the Windows firewall block that. Symptom: the cluster_check says ONLY ONE WORKER while `node3:8080` shows 2 workers; the `nc` test shows node2 never reaching node3's 40000.

**The fix: run ZeroTier inside Ubuntu, not (only) on Windows.** Then the overlay traffic ends in Ubuntu's own `zt` interface and never crosses the Windows NAT.

1. In Ubuntu write `/etc/wsl.conf` (RUNBOOK S02):
   ```
   [boot]
   systemd=true
   [network]
   generateHosts=false
   ```
   (`systemd=true` lets zerotier-one start as a service. `generateHosts=false` stops WSL from rewriting `/etc/hosts`, which would remove our node names.)
2. In Windows PowerShell: `wsl --shutdown`. Open Ubuntu again. Check `ls -l /dev/net/tun` exists (if not: `wsl --update`).
3. In Ubuntu:
   ```bash
   curl -s https://install.zerotier.com | sudo bash
   sudo zerotier-cli join <network id>
   ```
4. ZeroTier Central: authorize the **new** member, set its IP by hand to `172.23.10.141`, **delete the old Windows member** of node3 (two members must not share the IP). You may leave ZeroTier running on Windows, but its network membership for this network must go.
5. `ip -4 -br a` in Ubuntu shows `172.23.10.141`. Redo RUNBOOK S05 and S06 (hosts, ufw). Then the port test of S13-S16: `nc -l 40000` on node3, `nc -zv -w 3 node3 40000` on node2 must say `succeeded`. (Optional, all ports: `python3 scripts/netcheck.py` on all 4 nodes at once: node3 must be `ALL OK` including "node2 -> node3 ✅ reached all my ports".)
6. **Keep one Ubuntu terminal open** for the whole session. WSL shuts its VM down (and every daemon in it) when the last terminal closes. Optional: `%UserProfile%\.wslconfig`
   ```
   [wsl2]
   memory=6GB
   ```
   then `wsl --shutdown`.
7. After a Windows reboot: open Ubuntu, `source ~/bd_project/venv/bin/activate`, `jps`, then start the daemons again (RUNBOOK S21, S23).

<a id="wsl2-windows"></a>
### Fallback: ZeroTier on Windows

Only if ZeroTier will not run inside WSL. Then Windows owns the overlay IP and must forward traffic into WSL. This is slower to set up and less certain. In `%UserProfile%\.wslconfig`:
```
[wsl2]
networkingMode=mirrored
```
Then an **administrator PowerShell**:
```powershell
Set-NetFirewallHyperVVMSetting -Name '{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}' -DefaultInboundAction Allow
New-NetFirewallRule -DisplayName bd_project -Direction Inbound -Action Allow -Protocol TCP -LocalPort 7077,8080,8081,4040,18080,9864,9866,9867,40000-40031 -RemoteAddress 172.23.0.0/16
wsl --shutdown
```
In mirrored mode the ZeroTier adapter appears in Ubuntu (maybe as `eth1`); `env.sh` finds the IP on any interface. Verify with the `nc` test of S13-S16.

---

<a id="log-patterns"></a>
## Which error text in a log means what

Find it with `grep -E "Error|Exception|ERROR|FAIL" ~/bd_project/state/logs/<name>.log | tail`, or search all newest logs: `grep -lE "<pattern>" ~/bd_project/state/logs/*.log ~/bd_project/state/logs/hadoop/*.log ~/bd_project/state/logs/spark/*.out`.

| grep for | Cause | Section |
|---|---|---|
| `Initial job has not accepted any resources` | executors cannot reach the driver, or no free cores | [no resources](#no-resources) |
| `Master removed our application`, `Failed to connect to master`, `Could not connect to spark://` | cannot reach the Spark master on node3:7077 | [no resources](#no-resources) |
| `Python in worker has different version` | executor Python is not 3.10 | [py-version](#py-version) |
| `python3.10: (not found\|No such file)`, `Cannot run program "/usr/bin/python3.10"` | Python 3.10 missing on a worker node | [py-missing](#py-missing) |
| `Incompatible clusterIDs` | old DataNode folder after a second format | [clusterID](#clusterid) |
| `Name node is in safe mode`, `SafeModeException` | normal for ~30 s after NameNode start | [safe mode](#safemode) |
| `could only be written to [0-9]+ of the [0-9]+ minReplication` | client cannot reach a DataNode on 9866 | [minReplication](#minreplication) |
| `Connection refused.*:9000`, `Call From .* to node2:9000 failed` | NameNode not running or not reachable | [refused :9000](#refused-9000) |
| `Permission denied` | `dfs.permissions.enabled` is not false on node2 | [Permission denied](#permission) |
| `TABLE_OR_VIEW_NOT_FOUND`, `Table or view not found`, `NoSuchTableException` | an earlier step did not finish | [Table not found](#table-not-found) |
| `ClassNotFoundException.*iceberg` | Iceberg jar missing | [ClassNotFound iceberg](#iceberg-class) |
| `FAIL index built from the current docs snapshot`, `FAIL PageRank computed from the current docs snapshot` | stale index/PageRank | [stale export](#stale-export) |
| `already exists` | export version or tag name taken | [already exists](#exists) |
| `BindException`, `Address already in use` | port held by another process | [port in use](#port-in-use) |
| `UnknownHostException` | a node name does not resolve (`/etc/hosts`) | [ping](#ping) |
| `No space left on device` | disk full (`df -h`) | |

<a id="not-tested"></a>
## What we could not test

The scripts and the pipeline were tested on a 4-container podman cluster on one laptop (see the status line in [README.md](README.md)). Not tested, because they need the real machines:

- real ZeroTier (DIRECT/RELAY, MTU behaviour, UDP 9993, Central authorization),
- `ufw` on the real VMs (containers have no ufw),
- WSL2 and Windows itself (`wsl.conf`, `wsl --shutdown`, the Windows firewall, mirrored mode).

The port test of RUNBOOK S13-S16 (and, optionally, `python3 scripts/netcheck.py` on all 4 machines) is the proof step for all three. If `nc -zv` succeeds in both directions on 40000 and the daemon ports, HDFS and Spark will work. If not, the sections above say why.
