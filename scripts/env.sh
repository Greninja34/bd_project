#!/usr/bin/env bash
# The ONLY thing `source ~/bd_project/venv/bin/activate` loads (one line at the end of venv/bin/activate).
# It finds out which node this machine is and points Hadoop/Spark at our own config in state/conf/.
# Sourced into your shell: no `set -e`, every step is guarded. Safe to source twice.
# The course's own config in /opt/hadoop/etc/hadoop and $SPARK_HOME/conf is never touched.

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$PROJECT_HOME/state"
export PROJECT_HOME STATE_DIR

# 1. Where the course installed things (only if your shell did not already set them)
if [ -z "${JAVA_HOME:-}" ] && [ -d /usr/lib/jvm/java-11-openjdk-amd64 ]; then export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64; fi
if [ -z "${HADOOP_HOME:-}" ] && [ -d /opt/hadoop ]; then export HADOOP_HOME=/opt/hadoop; fi
if [ -z "${SPARK_HOME:-}" ]  && [ -d /opt/spark ];  then export SPARK_HOME=/opt/spark; fi
for _d in "${HADOOP_HOME:-}/bin" "${SPARK_HOME:-}/bin" "${SPARK_HOME:-}/sbin"; do
  case ":$PATH:" in *":$_d:"*) ;; *) [ -d "$_d" ] && PATH="$PATH:$_d" ;; esac
done
unset _d

# 2. Leftovers from old guides (solo practice exported these) must not leak in
unset SPARK_LOCAL_IP SPARK_LOCAL_HOSTNAME SPARK_MASTER_HOST

# 3. team.env: the four ZeroTier IPs
if [ -f "$PROJECT_HOME/team.env" ]; then set -a; . "$PROJECT_HOME/team.env"; set +a; fi

# 4. Which node am I? The one whose IP is on ANY interface of this machine
unset NODE_NAME MY_IP
_addrs="$(ip -4 -o addr show 2>/dev/null | awk '{split($4,a,"/"); print a[1]}')"
for _i in 1 2 3 4; do
  eval "_ip=\${NODE${_i}_IP:-}"
  if [ -n "$_ip" ] && printf '%s\n' "$_addrs" | grep -qx "$_ip"; then NODE_NAME="node$_i"; MY_IP="$_ip"; fi
done
unset _addrs _i _ip
export NODE_NAME MY_IP

# 5. Our own config, logs and pids under state/ (only once the config has been written)
if [ -n "${NODE_NAME:-}" ]; then
  export SERVING_DIR="$STATE_DIR/serving"
  export HADOOP_LOG_DIR="$STATE_DIR/logs/hadoop" HADOOP_PID_DIR="$STATE_DIR/pids/hadoop"
  [ -d "$STATE_DIR/conf/hadoop" ] && export HADOOP_CONF_DIR="$STATE_DIR/conf/hadoop"
  if [ "$NODE_NAME" != node4 ]; then
    export SPARK_LOG_DIR="$STATE_DIR/logs/spark" SPARK_PID_DIR="$STATE_DIR/pids/spark" SPARK_LOCAL_IP="$MY_IP"
    [ -d "$STATE_DIR/conf/spark" ] && export SPARK_CONF_DIR="$STATE_DIR/conf/spark"
  fi
  case "${PS1:-}" in ""|*"(node"*) ;; *) PS1="($NODE_NAME) $PS1" ;; esac
elif [ -n "${PS1:-}" ]; then
  echo "env.sh: none of the team.env IPs is on this machine (ip -4 -br a). Is ZeroTier up and authorized?" >&2
fi
