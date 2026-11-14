# TROUBLESHOOTING

Find your symptom (the exact error text, or what you see). Each entry: **cause**, then the commands.
First two commands for almost everything: `bd doctor` and `bd logs`.

Contents: [path / venv](#path-venv) · [ZeroTier IP not found](#zt-ip) · [ping](#ping) · [duplicate node IDs](#dup-id) · [MTU / RELAY](#mtu) · [ufw](#ufw) · [sudo host](#sudo-host) · [old daemons](#old-daemons) · [port in use](#port-in-use) · [clusterID](#clusterid) · [safe mode](#safemode) · [minReplication](#minreplication) · [DataNode wrong IP](#dn-ip) · [refused :9000](#refused-9000) · [Permission denied](#permission) · [no resources](#no-resources) · [only one worker](#one-worker) · [Python versions](#py-version) · [python3.10 missing](#py-missing) · [Table not found](#table-not-found) · [ClassNotFound iceberg](#iceberg-class) · [stale export](#stale-export) · [already exists](#exists) · [solo leftovers](#solo-leftover) · [node3 on WSL2](#wsl2) · [not tested](#not-tested)

---

<a id="path-venv"></a>
## `No such file or directory` for a script, `bd: command not found`, `ModuleNotFoundError: fastapi`, pip fails

**Cause:** the repo is not where you think, you did not activate the venv, or the node4 dependencies are not installed.
```bash
ls ~/bd_project/bd                                   # clone: git clone https://github.com/Greninja34/bd_project.git ~/bd_project
source ~/bd_project/venv/bin/activate                # in EVERY new terminal; prompt shows (bd nodeN)
bash ~/bd_project/bd setup                           # creates the venv, installs node4 deps
```
node4: `python3 -m venv` failing means `sudo apt install -y python3-venv`, then `bd setup` again. `pip` failing means no internet: run `bd setup` when online. Test: `~/bd_project/venv/bin/python -c "import fastapi, uvicorn, pandas, pyarrow"`.
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
## `ping node2` fails / `nothing answers` in netcheck

**Cause:** ZeroTier not joined or not authorized, wrong `/etc/hosts`, or the other node is off.
```bash
sudo zerotier-cli listnetworks                       # OK?
ping -c2 172.23.194.191                              # by IP: works? then /etc/hosts is wrong -> bd setup
getent hosts node2                                   # must print node2's ZeroTier IP
sudo zerotier-cli peers | grep LEAF                  # UDP 9993 must not be blocked by your network
```

<a id="dup-id"></a>
## Two machines have the same ZeroTier node ID (cloned VMs)

**Cause:** a VM cloned from another one copied `/var/lib/zerotier-one/identity.*`. Both machines fight over one identity: random timeouts, wrong IPs. `bd doctor` prints the 10-digit node ID of each machine; compare them in the group chat.
On the clone:
```bash
sudo systemctl stop zerotier-one
sudo rm /var/lib/zerotier-one/identity.public /var/lib/zerotier-one/identity.secret
sudo systemctl start zerotier-one
sudo zerotier-cli info                               # new ID
sudo zerotier-cli join <network id>
```
Then authorize the new member in Central, set its IP by hand, delete the old duplicate member, `bd setup`.

<a id="mtu"></a>
## Pings work but HDFS/Spark hang; `bulk transfer stalls`; `peers` shows RELAY

**Cause:** ZeroTier's default MTU is 2800. Small packets pass, big ones get lost: connections open but data transfers freeze. Or traffic goes through a RELAY (slow) because UDP 9993 is blocked.
```bash
sudo zerotier-cli peers | grep LEAF                  # DIRECT is good, RELAY is slow
ip link show | grep -A1 zt                           # current mtu
```
Fix: put `ZT_MTU=1400` in `team.env`, push, everyone `git pull` and `bd setup` (sets the interface MTU; run `bd setup` again after a reboot). Then `bd netcheck` on all 4 again.
Still RELAY: another network or a phone hotspot usually gives DIRECT. The university wifi often blocks UDP.

<a id="ufw"></a>
## Some ports blocked, others open (netcheck shows a few ❌); ufw

**Cause:** `ufw` is enabled with only SSH allowed.
```bash
sudo ufw status
bash ~/bd_project/bd setup                           # adds: ufw allow from <each other node's IP>
# by hand:  sudo ufw allow from 172.23.59.75   (and the other two nodes)
```
Both directions matter: node A can reach node B does not prove B can reach A. netcheck prints both.

<a id="sudo-host"></a>
## `sudo: unable to resolve host <srn>`, or `bd setup` cannot use sudo

**Cause:** the hostname is not in `/etc/hosts` (after our edit it is on your own node line). Run `bd setup` once more (it adds it). By hand:
```bash
echo "$(hostname -I | awk '{print $1}') $(hostname)" | sudo tee -a /etc/hosts
```
If your user has no sudo, `bd setup` prints each command it could not run: ask a teammate or your admin to run it.

<a id="old-daemons"></a>
## Old course daemons (ResourceManager, NodeManager, SecondaryNameNode, a 2nd NameNode) still running

**Cause:** the course's single-node setup started HDFS and YARN on this VM. They hold ports and are configured for `localhost`.
```bash
jps
bd stop --old                                        # stops them with the course config, kills as last resort
```
`bd start` and `bd hdfs-format` do this for you. Never run `start-dfs.sh`, `start-yarn.sh`, `start-all.sh` in this project.

<a id="port-in-use"></a>
## `BindException`, `Address already in use`

**Cause:** another process owns the port (usually an old daemon, or a second `bd start` after a crash).
```bash
ss -tlnp | grep -E ':9000|:9866|:7077|:8080|:8081|:18080'
jps
bd stop --old ; bd stop ; bd start
```

<a id="clusterid"></a>
## `Incompatible clusterIDs` (DataNode does not start or never appears)

**Cause:** the DataNode's data dir belongs to a different (older) NameNode. Happens after `bd hdfs-format --force`, or if an old dir is reused. Our dirs are fresh (`state/hdfs`), so it means HDFS was formatted twice.
On the node that shows it (it holds no precious data; the blocks are copied again):
```bash
bd stop
bd datanode wipe
bd start
bd status
```

<a id="safemode"></a>
## `Name node is in safe mode` / `SafeModeException`

**Cause:** normal for about 30 s after the NameNode starts (and until DataNodes report). 
```bash
hdfs dfsadmin -safemode get
bd status                                            # waits for it
```
If it stays ON with both DataNodes live: `hdfs dfsadmin -safemode leave` (last resort).

<a id="minreplication"></a>
## `could only be written to 0 of the 1 minReplication nodes` (also `put` hangs)

**Cause:** the client reached the NameNode (port 9000) but cannot reach any DataNode on **port 9866**.
```bash
bd netcheck                                          # all 4 nodes: is 9866 on node2 and node3 ✅ ?
bd status                                            # both DataNodes live?
sudo ufw status
```
On node3/WSL2: [node3 on WSL2](#wsl2). Big writes stalling while small things work: [MTU](#mtu).

<a id="dn-ip"></a>
## A DataNode is listed with `127.x.x.x` or a NAT address (10.0.2.x, 192.168.x.x)

**Cause:** `/etc/hosts` on that node maps its hostname to 127.0.1.1 (or a container/WSL address), or the NameNode sees a connection from the wrong interface.
```bash
getent hosts $(hostname)                             # must be the ZeroTier IP
bash ~/bd_project/bd setup ; bd stop ; bd start      # on that node
bd status
```
On WSL, `generateHosts=false` in `/etc/wsl.conf` must be set (setup does it) and `wsl --shutdown` run once.

<a id="refused-9000"></a>
## `Connection refused` to `node2:9000`

**Cause:** the NameNode is not running, or listens only on loopback, or the firewall blocks 9000.
```bash
# on node2
jps | grep NameNode ; ss -tlnp | grep 9000           # must show 172.23.194.191:9000 or 0.0.0.0:9000, not 127.x
bd start
# on the failing node
getent hosts node2 ; bd netcheck
```

<a id="permission"></a>
## `Permission denied: user=...` in HDFS

**Cause:** our 4 VMs have 4 different usernames. HDFS permission checking must be off **on node2** (the NameNode decides).
```bash
grep -A1 permissions ~/bd_project/state/conf/hadoop/hdfs-site.xml     # on node2: false
bd setup ; bd stop ; bd start                        # on node2
```

<a id="no-resources"></a>
## `Initial job has not accepted any resources`

**Cause:** no executor started or no executor could connect back to the driver, or another job holds all 4 cores.
```bash
bd status                                            # 2 ALIVE workers?
# open http://172.23.10.141:8080 : is another application RUNNING? kill/finish it
bd netcheck                                          # executors connect INTO the driver port 40000
bd logs
```
Most often a firewall on the **driver** node (ufw, or WSL2: see [node3 on WSL2](#wsl2)).

<a id="one-worker"></a>
## `workers used: node3 -> ONLY ONE WORKER` although `node3:8080` lists 2 workers

**Cause:** the master (7077/8080) is reachable, but the other worker's executor cannot connect **back to the driver** (port 40000/40001) or its block manager port. The master UI cannot tell you that. This is exactly the WSL2 NAT problem.
```bash
bd netcheck            # on all 4 nodes together. Look for "node2 -> node3: never reached my ports [40000 ...]"
```
- Driver on node3/WSL2: [node3 on WSL2](#wsl2) (ZeroTier must run inside Ubuntu).
- Otherwise: [ufw](#ufw) on the driver node, then `bd check` again.
- The check waits 120 s because executors start one after the other. If it prints the diagnosis, a retry will not help.

<a id="py-version"></a>
## `Python in worker has different version 3.x than that in driver 3.10`

**Cause:** an executor started another Python (Spark 3.3.4 needs 3.10, Ubuntu 24.04's default is 3.12). Executors use the **driver's** path `/usr/bin/python3.10`.
```bash
/usr/bin/python3.10 --version                        # on node1, node2, node3
bd setup                                             # links it if python3.10 lives elsewhere
grep python ~/bd_project/state/conf/spark/spark-env.sh ~/bd_project/state/conf/spark/spark-defaults.conf
```
Restart the worker on that node: `bd stop`; `bd start`.

<a id="py-missing"></a>
## `python3.10: not found` / `Cannot run program "/usr/bin/python3.10"` on an executor

**Cause:** Python 3.10 is missing on node2/node3, or only at `/usr/local/bin/python3.10`.
```bash
ls -l /usr/bin/python3.10 /usr/local/bin/python3.10
sudo ln -s /usr/local/bin/python3.10 /usr/bin/python3.10      # or run: bd setup
```

<a id="table-not-found"></a>
## `Table or view not found` / `TABLE_OR_VIEW_NOT_FOUND`

**Cause:** the step that creates the table failed or has not run. `docs/links/rejects` come from `bd ingest`, `postings/doc_stats` from `bd index`, `pagerank` from `bd pagerank`.
```bash
bd logs ingest                                       # read the earlier job's log
bd sql "SHOW TABLES IN iceberg_catalog.search"
```
Run the earlier step again.

<a id="iceberg-class"></a>
## `ClassNotFoundException: org.apache.iceberg...` / `Cannot find catalog plugin class`

**Cause:** the Iceberg jar is missing in `state/jars/`.
```bash
ls ~/bd_project/state/jars/                          # iceberg-spark-runtime-3.3_2.12-1.8.1.jar
bd setup                                             # copies from /opt/iceberg or downloads + sha1 check
```

<a id="stale-export"></a>
## export: `FAIL index built from the current docs snapshot`

**Cause:** the `docs` table changed after the index/PageRank ran (a new ingest, or a rollback). The export refuses stale inputs on purpose.
```bash
bd index N ; bd pagerank N ; bd export vN
```

<a id="exists"></a>
## `.../v0 already exists` / `already exists`

**Cause:** that export version is in HDFS already.
```bash
bd export v0 --overwrite                             # old one is kept as _replaced_v0_<time>
# or use a new name, e.g. v2
```
`Table ... already exists` from `bd tag`: the tag name is taken; use another name.

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

<a id="wsl2"></a>
## node3 on WSL2

WSL2 runs Ubuntu in a small VM behind a NAT. Spark executors on node2 must connect **into** node3's driver (port 40000) and block manager. WSL2's NAT and the Windows firewall block that. Symptom: `bd check` says ONLY ONE WORKER while `node3:8080` shows 2 workers; `bd netcheck` shows node2 never reaching node3's 40000.

**The fix: run ZeroTier inside Ubuntu, not (only) on Windows.** Then the overlay traffic ends in Ubuntu's own `zt` interface and never crosses the Windows NAT.

1. In Ubuntu: `bash ~/bd_project/bd setup`. It writes `/etc/wsl.conf`:
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
5. `ip -4 -br a` in Ubuntu shows `172.23.10.141`. `bash ~/bd_project/bd setup` again. Then `bd netcheck` on all 4 nodes: node3 must be `ALL OK` including "node2 -> node3 ✅ reached all my ports".
6. **Keep one Ubuntu terminal open** for the whole session. WSL shuts its VM down (and every daemon in it) when the last terminal closes. Optional: `%UserProfile%\.wslconfig`
   ```
   [wsl2]
   memory=6GB
   ```
   then `wsl --shutdown`.
7. After a Windows reboot: open Ubuntu, `source ~/bd_project/venv/bin/activate`, `bd doctor`, `bd start`.

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
In mirrored mode the ZeroTier adapter appears in Ubuntu (maybe as `eth1`); `bd` finds the IP on any interface. Verify with `bd netcheck` on all 4 nodes.

---

<a id="not-tested"></a>
## What we could not test

The scripts and the pipeline were tested on a 4-container podman cluster on one laptop (see the status line in [README.md](README.md)). Not tested, because they need the real machines:

- real ZeroTier (DIRECT/RELAY, MTU behaviour, UDP 9993, Central authorization),
- `ufw` on the real VMs (containers have no ufw),
- WSL2 and Windows itself (`wsl.conf`, `wsl --shutdown`, the Windows firewall, mirrored mode).

`bd netcheck` on the four real machines is the proof step for all three. If it says `ALL OK`, HDFS and Spark will work. If it does not, its hints point to the sections above.
