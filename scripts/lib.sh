#!/usr/bin/env bash
# Shared helpers: colors, messages, team.env, "which node am I?", ports, known-error hints.
# Sourced by `bd`, by the scripts and (through env.sh) by venv/bin/activate.
# No `set -e` / `set -u` here on purpose: this file is also sourced into your own shell.

BD_HOME="${BD_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

if [ -t 1 ]; then
  BD_RED=$'\033[31m'; BD_GREEN=$'\033[32m'; BD_YELLOW=$'\033[33m'; BD_BOLD=$'\033[1m'; BD_OFF=$'\033[0m'
else
  BD_RED=""; BD_GREEN=""; BD_YELLOW=""; BD_BOLD=""; BD_OFF=""
fi

ok()    { printf '  %sOK%s      %s\n' "$BD_GREEN" "$BD_OFF" "$*"; }
warn()  { printf '  %sWARN%s    %s\n' "$BD_YELLOW" "$BD_OFF" "$*"; }
fail()  { printf '  %sFAIL%s    %s\n' "$BD_RED" "$BD_OFF" "$*"; }
hint()  { printf '          -> %s\n' "$*"; }
step()  { printf '\n%s== %s ==%s\n' "$BD_BOLD" "$*" "$BD_OFF"; }
die()   { printf '%sERROR%s %s\n' "$BD_RED" "$BD_OFF" "$*" >&2; exit 1; }

# ---- team.env -------------------------------------------------------------------------------
bd_load_team() {
  BD_TEAM_ENV="${BD_TEAM_ENV:-$BD_HOME/team.env}"
  [ -f "$BD_TEAM_ENV" ] || { echo "team.env not found: $BD_TEAM_ENV" >&2; return 1; }
  set -a; . "$BD_TEAM_ENV"; set +a
}

bd_ip() {                       # bd_ip node3 -> 172.23.10.141
  local v="NODE${1#node}_IP"
  printf '%s' "${!v:-}"
}

# ---- which node am I? Match team IPs against ANY interface (no "zt*" assumption) -------------
bd_local_ips() {
  if command -v ip >/dev/null 2>&1; then
    ip -4 -o addr show 2>/dev/null | awk '{split($4,a,"/"); print a[1]}'
  else
    hostname -I 2>/dev/null | tr ' ' '\n'
  fi
}

bd_detect_node() {              # sets BD_NODE and MY_IP; returns 1 if this machine has no team IP
  local i ip addrs
  if [ -n "${BD_NODE:-}" ] && [ -z "${BD_NODE_AUTO:-}" ]; then      # forced (tests): BD_NODE=node2
    MY_IP="$(bd_ip "$BD_NODE")"; export MY_IP; return 0
  fi
  addrs="$(bd_local_ips)"
  for i in 1 2 3 4; do
    ip="$(bd_ip "node$i")"
    if [ -n "$ip" ] && printf '%s\n' "$addrs" | grep -qx "$ip"; then
      BD_NODE="node$i"; MY_IP="$ip"; BD_NODE_AUTO=1; export BD_NODE MY_IP BD_NODE_AUTO; return 0
    fi
  done
  unset BD_NODE MY_IP BD_NODE_AUTO
  return 1
}

bd_need_detected() {            # in scripts: stop with a clear message if no node was detected
  [ -n "${BD_NODE:-}" ] && return 0
  cat >&2 <<MSG
${BD_RED}ERROR${BD_OFF} ZeroTier IP not on this machine.
  None of these IPs is on any interface here: $(for i in 1 2 3 4; do printf '%s ' "$(bd_ip node$i)"; done)
  Is ZeroTier up and this machine authorized with its IP?   sudo zerotier-cli listnetworks ; ip -4 -br a
  Is team.env right?                                       $BD_TEAM_ENV
  (See TROUBLESHOOTING.md#zt-ip)
MSG
  exit 1
}

bd_nodenum() { printf '%s' "${BD_NODE#node}"; }
bd_is_wsl()  { grep -qi microsoft /proc/version 2>/dev/null; }

bd_iface_of() {                 # interface that holds this IP
  ip -4 -o addr show 2>/dev/null | awk -v ip="$1" '{split($4,a,"/"); if (a[1]==ip) {print $2; exit}}'
}

# ---- ports ----------------------------------------------------------------------------------
bd_port_open() {                # bd_port_open host port [seconds]
  timeout "${3:-2}" bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null
}

bd_wait_port() {                # bd_wait_port host port seconds label
  local i
  for ((i = 0; i < $3; i++)); do
    bd_port_open "$1" "$2" 1 && return 0
    sleep 1
  done
  return 1
}

# ---- sudo only where needed; never fail hard ------------------------------------------------
bd_sudo() {
  if [ "$(id -u)" = 0 ]; then "$@"; return; fi
  if command -v sudo >/dev/null 2>&1 && sudo "$@"; then return 0; fi
  warn "could not run with sudo. Run this yourself as root:"
  hint "sudo $*"
  return 1
}

# ---- daemons that are NOT ours (the course's single-node HDFS/YARN/Spark) ------------------
BD_DAEMONS="NameNode DataNode SecondaryNameNode ResourceManager NodeManager Master Worker HistoryServer"

bd_old_daemons() {              # prints "pid Name" for course daemons (pid not in our pid dirs)
  command -v jps >/dev/null 2>&1 || return 0
  local ours pid name
  ours="$(cat "${HADOOP_PID_DIR:-/nonexistent}"/*.pid "${SPARK_PID_DIR:-/nonexistent}"/*.pid 2>/dev/null | tr '\n' ' ')"
  while read -r pid name; do
    case " $BD_DAEMONS " in *" $name "*) ;; *) continue ;; esac
    case " $ours " in *" $pid "*) continue ;; esac
    echo "$pid $name"
  done < <(jps 2>/dev/null)
}

bd_stop_old() {                 # stop the course daemons: with THEIR config first, kill as last resort
  local list pid name left
  list="$(bd_old_daemons)"
  [ -z "$list" ] && { ok "no old (course) daemons running"; return 0; }
  warn "stopping old course daemons: $(echo "$list" | tr '\n' ' ')"
  (
    unset HADOOP_PID_DIR HADOOP_LOG_DIR SPARK_PID_DIR SPARK_LOG_DIR SPARK_CONF_DIR
    export HADOOP_CONF_DIR="${HADOOP_HOME:-/opt/hadoop}/etc/hadoop"
    while read -r pid name; do
      case "$name" in
        NameNode|DataNode|SecondaryNameNode) hdfs --daemon stop "$(echo "$name" | tr 'A-Z' 'a-z')" >/dev/null 2>&1 ;;
        ResourceManager|NodeManager)         yarn --daemon stop "$(echo "$name" | tr 'A-Z' 'a-z')" >/dev/null 2>&1 ;;
      esac
    done <<<"$list"
  ) || true
  sleep 2
  left="$(bd_old_daemons)"
  while read -r pid name; do
    [ -z "$pid" ] && continue
    kill "$pid" 2>/dev/null || true
    for _ in 1 2 3 4 5; do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -9 "$pid" 2>/dev/null || true
  done <<<"$left"
  [ -z "$(bd_old_daemons)" ] && ok "old daemons stopped" || fail "some old daemons are still running: jps"
}

# ---- known errors -> hint -------------------------------------------------------------------
# regex, TROUBLESHOOTING anchor, what to do
BD_ERRORS=(
  "Initial job has not accepted any resources" "no-resources" "Workers cannot reach this driver, or no free cores. Run: bd status ; bd netcheck (all nodes)"
  "Python in worker has different version" "py-version" "Executors use another Python than the driver. Needs /usr/bin/python3.10 on every node (bd setup)"
  "python3.10: (not found|No such file)|Cannot run program \"/usr/bin/python3.10\"" "py-missing" "python3.10 missing on a worker node: run bd setup there"
  "Incompatible clusterIDs" "clusterid" "Old DataNode dir. Run: bd stop ; bd datanode wipe   (on the node that says it)"
  "Name node is in safe mode|SafeModeException" "safemode" "Normal for ~30 s after the NameNode starts. Wait, or: hdfs dfsadmin -safemode get"
  "could only be written to [0-9]+ of the [0-9]+ minReplication" "minreplication" "The client cannot reach a DataNode on port 9866. Run bd netcheck; check ufw"
  "Connection refused.*:9000|Call From .* to node2:9000 failed" "refused-9000" "NameNode not running or not reachable. On node2: bd start ; here: bd doctor"
  "Permission denied" "permission" "dfs.permissions.enabled must be false on node2 (bd setup writes it). Re-run bd setup on node2, then bd stop; bd start"
  "TABLE_OR_VIEW_NOT_FOUND|Table or view not found|NoSuchTableException" "table-not-found" "The earlier step did not finish. Run the previous bd command first (see its log)"
  "ClassNotFoundException.*iceberg|java.lang.ClassNotFoundException: org.apache.iceberg" "iceberg-class" "Iceberg jar missing: run bd setup; check state/jars/"
  "FAIL index built from the current docs snapshot|FAIL PageRank computed from the current docs snapshot" "stale-export" "docs changed after index/pagerank ran. Run bd index N and bd pagerank N again, then bd export"
  "already exists" "exists" "Use a new version name, or add --overwrite (export)"
  "BindException|Address already in use" "port-in-use" "Another process holds the port (old course daemon?). Run: bd stop --old ; ss -tlnp | grep <port>"
  "UnknownHostException" "path-venv" "A node name does not resolve. Run bd setup (writes /etc/hosts) and bd doctor"
  "Master removed our application|Failed to connect to master|Could not connect to spark://" "no-resources" "Cannot reach the Spark master on node3:7077. Is node3 started (bd start)? bd netcheck"
  "No space left on device" "path-venv" "Disk full: df -h"
)

bd_known_errors() {             # bd_known_errors FILE  -> prints matching lines + hints, returns 0 if any matched
  local f="$1" i found=1 line
  [ -s "$f" ] || return 1
  for ((i = 0; i < ${#BD_ERRORS[@]}; i += 3)); do
    line="$(grep -m1 -E "${BD_ERRORS[i]}" "$f" 2>/dev/null | cut -c1-200)" || true
    [ -z "$line" ] && continue
    fail "$line"
    hint "${BD_ERRORS[i+2]}   (TROUBLESHOOTING.md#${BD_ERRORS[i+1]})"
    found=0
  done
  return $found
}

bd_lib_funcs="ok warn fail hint step die bd_need_detected bd_port_open bd_wait_port bd_sudo bd_stop_old bd_old_daemons bd_known_errors bd_local_ips bd_iface_of bd_nodenum bd_is_wsl bd_ip bd_load_team bd_detect_node"
