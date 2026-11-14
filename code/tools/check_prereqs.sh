#!/usr/bin/env bash
# check_prereqs.sh - is this VM ready for its role? Read-only, changes nothing.
#   bash ~/project_search_engine/code/tools/check_prereqs.sh node3
# Checks everything PREREQUISITES.md lists for that node and prints OK / MISSING per item.

NODE="${1:-}"
case "$NODE" in node1|node2|node3|node4) ;; *) echo "usage: $0 node1|node2|node3|node4"; exit 2;; esac
fail=0
ok()   { printf '  \033[32mOK\033[0m      %s\n' "$*"; }
miss() { printf '  \033[31mMISSING\033[0m %s\n' "$*"; fail=$((fail+1)); }
chk()  { if eval "$2" >/dev/null 2>&1; then ok "$1"; else miss "$1  ->  $3"; fi; }

echo "== $NODE: common (every node)"
chk "Ubuntu 24.04"                    "grep -q 'VERSION_ID=\"24.04\"' /etc/os-release"  "course guide 02 (other versions may work, untested)"
chk "Java 11"                         "java -version 2>&1 | grep -q 'version \"11'"     "course guide 03: sudo apt install openjdk-11-jdk"
chk "JAVA_HOME set"                   "[ -n \"\$JAVA_HOME\" ]"                          "course guide 03 (add to ~/.bashrc)"
chk "Hadoop 3.3.6 at /opt/hadoop"     "/opt/hadoop/bin/hadoop version | grep -q 3.3.6"  "course guide 05"
chk "HADOOP_CONF_DIR set"             "[ -n \"\$HADOOP_CONF_DIR\" ]"                    "course guide 05 (~/.bashrc)"
chk "hdfs on PATH"                    "command -v hdfs"                                 "source ~/.bashrc"
chk "ZeroTier installed + joined"     "ip -o link show | grep -q ': zt'"                "course guide 04: zerotier-cli join <id>"
chk "git"                             "command -v git"                                  "sudo apt install git"
chk "repo at ~/project_search_engine" "[ -f ~/project_search_engine/code/common/textproc.py ]" "git clone <repo> ~/project_search_engine"
chk "data files present"              "[ -s ~/project_search_engine/data/phase-0/pages.jsonl ] && [ -s ~/project_search_engine/data/phase-1/changes.json ]" "they are in the repo: git pull"

if [ "$NODE" != node4 ]; then
  echo "== $NODE: Spark side (nodes 1-3)"
  chk "Spark 3.3.4 at /opt/spark"     "/opt/spark/bin/spark-submit --version 2>&1 | grep -q 'version 3.3.4'" "course guide 07"
  chk "Python 3.10"                   "[ -x /usr/bin/python3.10 ] || [ -x /usr/local/bin/python3.10 ]"    "course guide 07 (deadsnakes PPA)"
  chk "PYSPARK_PYTHON = python3.10"   "echo \"\$PYSPARK_PYTHON\" | grep -q python3.10"                    "course guide 07 §3 (~/.bashrc)"
  chk "Iceberg 1.8.1 jar"             "[ -f /opt/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar ]"      "team-run/01 §8"
  chk "passwordless ssh localhost"    "ssh -o BatchMode=yes -o ConnectTimeout=3 localhost true"           "course guide 03 §4"
fi
if [ "$NODE" = node4 ]; then
  echo "== node4: search service"
  chk "~/search-venv with the deps"   "~/search-venv/bin/python -c 'import fastapi, uvicorn, pandas, pyarrow'" "team-run/05 M4-1"
  chk "/serving writable"             "[ -w /serving ]"                                                       "sudo mkdir -p /serving && sudo chown \$USER:\$USER /serving"
fi

echo "== RAM: $(free -g | awk '/Mem:/ {print $2}') GB (recommended: node1 3, node2 4, node3 4-6, node4 2)"
[ $fail -eq 0 ] && echo "ALL GOOD for $NODE" || echo "$fail item(s) missing for $NODE"
exit $fail
