#!/usr/bin/env bash
# clean_old.sh - remove the PREVIOUS attempt's code and config from this machine so it cannot
# clash with this repo. Run it ONCE per VM, before RUNBOOK S01.
#
#   bash clean_old.sh            dry run: only LISTS what it found (changes nothing)
#   bash clean_old.sh --yes      really remove it (asks no questions)
#   bash clean_old.sh --yes --course-hdfs   also delete ~/hadoop-data (the course's single-node HDFS data)
#
# Never touched: /opt/hadoop, /opt/spark, /opt/iceberg, Java, Python, ZeroTier, ~/.ssh, the
# course's JAVA_HOME/HADOOP_HOME/SPARK_HOME lines, and the repo this script lives in.
# Needs sudo only for /etc/hosts, /serving and daemon files owned by root.
set -u

DO=0; COURSE_HDFS=0
for a in "$@"; do
  case "$a" in
    --yes) DO=1 ;;
    --course-hdfs) COURSE_HDFS=1 ;;
    -h|--help) sed -n 2,13p "$0"; exit 0 ;;
    *) echo "unknown option: $a (see --help)"; exit 2 ;;
  esac
done

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"     # the repo being used now: never removed
HC="${HADOOP_CONF_DIR_COURSE:-/opt/hadoop/etc/hadoop}"      # the course's own config folder
SC="${SPARK_HOME:-/opt/spark}/conf"
FOUND=0

say()  { printf '%s\n' "$*"; }
item() { FOUND=$((FOUND+1)); if [ "$DO" = 1 ]; then say "  removing: $*"; else say "  found:    $*"; fi; }

rm_path() {                    # rm_path <path>  : remove a file/folder of the old attempt
  local p="$1"
  [ -e "$p" ] || [ -L "$p" ] || return 0
  [ "$(cd "$p" 2>/dev/null && pwd)" = "$SELF" ] && { say "  (skipping $p: it is the repo in use)"; return 0; }
  item "$p"
  [ "$DO" = 1 ] || return 0
  rm -rf "$p" 2>/dev/null || sudo rm -rf "$p"
}

rm_matching() {                # rm_matching <file> <grep-pattern> <what> : delete matching lines of a file
  local f="$1" pat="$2" what="$3"
  [ -f "$f" ] && grep -qE "$pat" "$f" || return 0
  item "$what in $f"
  [ "$DO" = 1 ] || return 0
  sed -i -E "/$pat/d" "$f" 2>/dev/null || sudo sed -i -E "/$pat/d" "$f"
}

say "== 1. old daemons from the course single-node setup or a previous try (jps)"
OLD_PIDS=""
if command -v jps >/dev/null 2>&1; then
  while read -r pid name; do
    case "$name" in
      NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager|Master|Worker|HistoryServer|CoarseGrainedExecutorBackend)
        # our own new daemons carry this repo's state/ path in the command line: leave them alone
        if tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q "$SELF/state"; then continue; fi
        OLD_PIDS="$OLD_PIDS $pid"; item "java process $name (pid $pid)" ;;
    esac
  done < <(jps 2>/dev/null)
  if [ "$DO" = 1 ] && [ -n "$OLD_PIDS" ]; then
    # polite first (SIGTERM), then force
    for p in $OLD_PIDS; do kill "$p" 2>/dev/null; done; sleep 3
    for p in $OLD_PIDS; do kill -0 "$p" 2>/dev/null && kill -9 "$p" 2>/dev/null; done
  fi
fi
for pid in $(pgrep -f "uvicorn app:app" 2>/dev/null); do      # only a search service started from a node4_search folder
  case "$(readlink "/proc/$pid/cwd" 2>/dev/null)" in
    */node4_search) [ -d "$SELF/state" ] && tr '\0' ' ' < "/proc/$pid/environ" 2>/dev/null | grep -q "$SELF/state" && continue
                    item "old uvicorn search service (pid $pid)"; [ "$DO" = 1 ] && kill "$pid" ;;
  esac
done

say "== 2. folders of the previous attempt"
rm_path "$HOME/project_search_engine"        # the old clone name used by the old guides
rm_path "$HOME/search-venv"                  # old Member-4 venv
rm_path "$HOME/nlp-venv"
rm_path "$HOME/solo"                         # old solo-practice folder
rm_path "$HOME/spark-config/search.conf"      # old cluster config. The course keeps its own iceberg.conf in that folder: left alone
rm_path "$HOME/incoming"                     # old scp fallback folder
rm_path /serving                             # old serving folder (ours is state/serving)
for f in "$HOME"/ingest-p*.log "$HOME"/index-p*.log "$HOME"/pagerank-*.log "$HOME"/export-*.log; do rm_path "$f"; done
rm_path /tmp/spark-derby-"$USER"
if [ "$COURSE_HDFS" = 1 ]; then
  rm_path "$HOME/hadoop-data"                # course single-node HDFS data (the new HDFS lives in state/hdfs)
else
  [ -d "$HOME/hadoop-data" ] && say "  (kept ~/hadoop-data: course HDFS data; the new cluster does not use it. Add --course-hdfs to delete it)"
fi
# a stray ~/bd_project is fine only if it IS this repo
[ -d "$HOME/bd_project" ] && [ "$(cd "$HOME/bd_project" && pwd)" != "$SELF" ] && say "  WARNING: ~/bd_project exists but is not the repo you ran this from ($SELF). Check it by hand."

say "== 3. config edited by the previous attempt"
# 3a. old /etc/hosts block (team32 = previous attempt, bd_project = a previous try of this one)
if grep -qE '# >>> (team32|bd_project)' /etc/hosts 2>/dev/null; then
  item "team block in /etc/hosts"
  if [ "$DO" = 1 ]; then
    sudo cp -n /etc/hosts /etc/hosts.before-clean-old
    sudo sed -i -E '/# >>> (team32|bd_project)/,/# <<< (team32|bd_project)/d' /etc/hosts
    sudo sed -i -E 's/^#(127\.0\.1\.1)/\1/' /etc/hosts         # give Ubuntu its own line back; the RUNBOOK S05 redoes the hosts block
  fi
fi
# 3b. course Hadoop config that the old guide overwrote (it saved the originals as *.single-node)
for f in core-site.xml hdfs-site.xml; do
  # restore only a real single-node backup (it points at localhost); a backup that already holds the
  # cluster config (the old guide re-saved it on a second run) is left alone
  if [ -f "$HC/$f.single-node" ] && { [ "$f" != core-site.xml ] || grep -q "localhost" "$HC/$f.single-node"; }; then
    item "restore $HC/$f from $f.single-node"
    [ "$DO" = 1 ] && { cp "$HC/$f.single-node" "$HC/$f" 2>/dev/null || sudo cp "$HC/$f.single-node" "$HC/$f"; }
  fi
done
rm_matching "$HC/hadoop-env.sh" '^export HADOOP_HEAPSIZE_MAX=512m' "old HADOOP_HEAPSIZE_MAX line"
# 3c. old Spark block in spark-env.sh
if grep -q '# >>> team32' "$SC/spark-env.sh" 2>/dev/null; then
  item "team32 block in $SC/spark-env.sh"
  [ "$DO" = 1 ] && { sed -i '/# >>> team32/,/# <<< team32/d' "$SC/spark-env.sh" 2>/dev/null || sudo sed -i '/# >>> team32/,/# <<< team32/d' "$SC/spark-env.sh"; }
fi
# 3d. shell startup files: variables that break the cluster (backup first)
for rc in "$HOME/.bashrc" "$HOME/.profile" "$HOME/.bash_profile"; do
  [ -f "$rc" ] || continue
  if grep -qE '^[[:space:]]*export (SPARK_LOCAL_IP|SPARK_LOCAL_HOSTNAME|SPARK_MASTER_HOST|SPARK_CONF_DIR|HADOOP_CONF_DIR=.*(state|spark-config))|^[[:space:]]*(source|\.) .*(search-venv|nlp-venv|solo)' "$rc"; then
    item "SPARK_LOCAL_*/old venv lines in $rc (backup: $rc.before-clean-old)"
    if [ "$DO" = 1 ]; then
      cp -n "$rc" "$rc.before-clean-old"
      sed -i -E '/^[[:space:]]*export (SPARK_LOCAL_IP|SPARK_LOCAL_HOSTNAME|SPARK_MASTER_HOST|SPARK_CONF_DIR|HADOOP_CONF_DIR=.*(state|spark-config))/d; /^[[:space:]]*(source|\.) .*(search-venv|nlp-venv|solo)/d' "$rc"
    fi
  fi
done
# the course's own lines (JAVA_HOME, HADOOP_HOME, HADOOP_CONF_DIR=/opt/hadoop/..., SPARK_HOME, PYSPARK_PYTHON) are left in place on purpose

say ""
if [ "$FOUND" = 0 ]; then
  say "Nothing from the previous attempt found: this machine is clean."
elif [ "$DO" = 1 ]; then
  say "Done: $FOUND item(s) removed. Backups: /etc/hosts.before-clean-old and ~/.bashrc.before-clean-old (if they existed)."
  say "Open a NEW terminal (or run: unset SPARK_LOCAL_IP SPARK_LOCAL_HOSTNAME SPARK_MASTER_HOST; source ~/.bashrc), then continue with RUNBOOK S01."
else
  say "DRY RUN: $FOUND item(s) found, nothing changed. If the list is what you expect: bash $0 --yes"
fi
