#!/usr/bin/env bash
# Environment for this project. Sourced by venv/bin/activate (setup adds that line) and by `bd`.
# Safe to source twice. Not for `set -e`/`set -u` callers to worry about: every step is guarded.
# It never changes the course's own config in /opt/hadoop/etc/hadoop or $SPARK_HOME/conf:
# it only points HADOOP_CONF_DIR / SPARK_CONF_DIR at our own copies in state/conf.

BD_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export BD_HOME
# shellcheck source=lib.sh
. "$BD_HOME/scripts/lib.sh"

# Where the course installed things (only used when your shell did not set them).
if [ -z "${JAVA_HOME:-}" ] && [ -d /usr/lib/jvm/java-11-openjdk-amd64 ]; then export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64; fi
if [ -z "${HADOOP_HOME:-}" ] && [ -d /opt/hadoop ]; then export HADOOP_HOME=/opt/hadoop; fi
if [ -z "${SPARK_HOME:-}" ]  && [ -d /opt/spark ];  then export SPARK_HOME=/opt/spark; fi
for _d in "${HADOOP_HOME:-}/bin" "${SPARK_HOME:-}/bin" "${SPARK_HOME:-}/sbin" "${JAVA_HOME:+$JAVA_HOME/bin}"; do
  case ":$PATH:" in *":$_d:"*) ;; *) [ -d "$_d" ] && PATH="$PATH:$_d" ;; esac
done
unset _d
case ":$PATH:" in *":$BD_HOME:"*) ;; *) PATH="$BD_HOME:$PATH" ;; esac

# Leftovers from an old terminal (solo practice exported SPARK_LOCAL_IP=127.0.0.1) must not leak in.
unset SPARK_LOCAL_IP SPARK_LOCAL_HOSTNAME SPARK_MASTER_HOST

bd_load_team || true
bd_detect_node || true

export BD_STATE="$BD_HOME/state"
if [ -n "${BD_NODE:-}" ]; then
  export HADOOP_CONF_DIR="$BD_STATE/conf/hadoop"
  export HADOOP_LOG_DIR="$BD_STATE/logs/hadoop"
  export HADOOP_PID_DIR="$BD_STATE/pids/hadoop"
  export SERVING_DIR="$BD_STATE/serving"
  case "$BD_NODE" in
    node4) ;;
    *)
      export SPARK_CONF_DIR="$BD_STATE/conf/spark"
      export SPARK_LOG_DIR="$BD_STATE/logs/spark"
      export SPARK_PID_DIR="$BD_STATE/pids/spark"
      export SPARK_LOCAL_IP="$MY_IP"
      ;;
  esac
  # prompt tag, once, only in interactive shells
  case "${PS1:-}" in ""|*"(bd "*) ;; *) PS1="(bd $BD_NODE) $PS1" ;; esac
  # export the team IPs for Python tools (cluster_check, netcheck)
  export NODE1_IP NODE2_IP NODE3_IP NODE4_IP
fi

# In your own shell the helper functions are not needed: keep the namespace clean.
# (bd and the scripts set BD_SCRIPT=1 and keep them.)
if [ -z "${BD_SCRIPT:-}" ]; then
  # shellcheck disable=SC2086
  unset -f $bd_lib_funcs 2>/dev/null || true
fi
