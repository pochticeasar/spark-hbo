#!/usr/bin/env bash
# Локальный standalone-кластер Spark: 1 master + N worker'ов на этой машине.
# Работает с pip-версией pyspark (в ней нет start-master.sh/start-worker.sh,
# поэтому процессы запускаются напрямую через sbin/spark-daemon.sh).
#
#   ./cluster.sh start 4 2 3g   # 4 worker'а, по 2 ядра и 3 ГБ памяти каждый
#   ./cluster.sh status
#   ./cluster.sh stop
#
# Потом:
#   python bench.py --master spark://127.0.0.1:7077 --executor-cores 2 --executor-memory 2g --label standalone-4w
#
# Каждый worker — отдельная JVM со своим executor'ом, поэтому shuffle идёт
# между процессами через сеть (loopback), а не внутри одного процесса, как в local[N].
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')}"
export SPARK_HOME
export SPARK_MASTER_HOST=127.0.0.1
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOG_DIR="${SPARK_LOG_DIR:-/tmp/spark-hbo-logs}"
export SPARK_PID_DIR="${SPARK_PID_DIR:-/tmp/spark-hbo-pids}"
WORK_DIR="${SPARK_WORKER_DIR:-/tmp/spark-hbo-work}"
MASTER_URL="spark://127.0.0.1:7077"
DAEMON="$SPARK_HOME/sbin/spark-daemon.sh"
MASTER_CLASS=org.apache.spark.deploy.master.Master
WORKER_CLASS=org.apache.spark.deploy.worker.Worker

chmod +x "$SPARK_HOME"/sbin/*.sh "$SPARK_HOME"/bin/* 2>/dev/null || true

case "${1:-}" in
  start)
    n="${2:-4}"; cores="${3:-2}"; mem="${4:-3g}"
    "$DAEMON" start "$MASTER_CLASS" 1 --host 127.0.0.1 --port 7077 --webui-port 8080
    for i in $(seq 1 "$n"); do
      "$DAEMON" start "$WORKER_CLASS" "$i" --host 127.0.0.1 --webui-port $((8080 + i)) \
        --cores "$cores" --memory "$mem" --work-dir "$WORK_DIR/$i" "$MASTER_URL"
    done
    echo "master: $MASTER_URL (UI http://127.0.0.1:8080), workers: $n x $cores cores x $mem, logs: $SPARK_LOG_DIR"
    ;;
  stop)
    for f in "$SPARK_PID_DIR"/*Worker*.pid; do
      [ -e "$f" ] || continue
      i="${f##*-}"; i="${i%.pid}"
      "$DAEMON" stop "$WORKER_CLASS" "$i" || true
    done
    "$DAEMON" stop "$MASTER_CLASS" 1 || true
    ;;
  status)
    found=0
    for f in "$SPARK_PID_DIR"/*.pid; do
      [ -e "$f" ] || continue
      pid=$(cat "$f")
      if kill -0 "$pid" 2>/dev/null; then echo "running: $(basename "$f" .pid) (pid $pid)"; found=1; fi
    done
    [ "$found" = 1 ] || echo "no Master/Worker processes running"
    ;;
  *)
    echo "usage: $0 start [workers] [cores] [memory] | stop | status"; exit 1
    ;;
esac
