"""
Эксперимент: AQE vs AQE + история (HBO) на повторяющемся запросе.

Сценарий — классическая ошибка оценки: справочник после фильтра крошечный,
но без статистики Spark считает размер Filter равным размеру всего файла
(десятки МБ > порога broadcast 10 МБ) и строит sort-merge join.
AQE исправляет это в рантайме, но уже после того, как отшаффлил факт-таблицу.
HBO запоминает реальный размер и со второго запуска сразу строит broadcast.

Запуск:
    source env.sh
    sbt package
    python bench.py                                  # local[4], как в базовом замере
    python bench.py --master 'local[*]' --label all-cores
    python bench.py --master spark://127.0.0.1:7077 --executors 4 --executor-cores 2 \
        --host-local-read off --label standalone-4w        # см. cluster.sh
    python bench.py --local-reader off --label no-local-reader

Каждый прогон дописывается строкой в results.csv (для графиков),
разбивка по стадиям — в results-stages.jsonl.
"""
import argparse
import csv
import json
import os
import statistics
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

p = argparse.ArgumentParser()
p.add_argument("--jar", default="target/scala-2.12/spark-hbo_2.12-0.1.jar")
p.add_argument("--master", default="local[4]")
p.add_argument("--label", default=None, help="метка серии в results.csv (по умолчанию = master)")
p.add_argument("--data", default="/tmp/hbo-data", help="папка с данными; на кластере — общая для всех worker'ов")
p.add_argument("--history", default="/tmp/hbo-history.tsv")
p.add_argument("--fact-rows", type=int, default=20_000_000)
p.add_argument("--dim-rows", type=int, default=2_000_000)
p.add_argument("--runs", type=int, default=5)
p.add_argument("--shuffle-partitions", type=int, default=64)
p.add_argument("--local-reader", choices=["on", "off"], default="on",
               help="spark.sql.adaptive.localShuffleReader.enabled")
p.add_argument("--host-local-read", choices=["on", "off"], default="on",
               help="spark.shuffle.readHostLocalDisk: off = блоки соседних executor'ов на том же хосте "
                    "читаются по сети, как будто это разные машины")
p.add_argument("--executors", type=int, default=0,
               help="standalone: дождаться регистрации стольких executor'ов перед замером")
p.add_argument("--executor-memory", default="2g", help="для standalone")
p.add_argument("--executor-cores", default=None, help="для standalone")
p.add_argument("--out", default="results.csv")
args = p.parse_args()
label = args.label or args.master

builder = (
    SparkSession.builder.master(args.master)
    .appName(f"spark-hbo-bench:{label}")
    .config("spark.jars", os.path.abspath(args.jar))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true" if args.local_reader == "on" else "false")
    .config("spark.shuffle.readHostLocalDisk", "true" if args.host_local_read == "on" else "false")
    .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", args.executor_memory)
    .config("spark.ui.enabled", "false")
)
if args.executor_cores:
    builder = builder.config("spark.executor.cores", args.executor_cores)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
jvm = spark._jvm
if args.executors:
    # executor'ы регистрируются не мгновенно; без ожидания первые запросы идут на части кластера
    deadline = time.time() + 120
    while spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1 < args.executors:
        if time.time() > deadline:
            raise SystemExit(f"only {spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1} "
                             f"of {args.executors} executors registered")
        time.sleep(1)
parallelism = spark.sparkContext.defaultParallelism

# ---------- данные ----------
fact_path, dim_path = f"{args.data}/fact", f"{args.data}/dim"
if not os.path.exists(fact_path):
    print("generating data ...")
    (spark.range(args.fact_rows)
        .select((F.col("id") % args.dim_rows).alias("dim_id"),
                (F.rand(1) * 100).alias("amount"))
        .write.parquet(fact_path))
    (spark.range(args.dim_rows)
        .select(F.col("id").alias("dim_id"),
                # ~0.05% строк проходят фильтр
                F.when(F.col("id") % 2000 == 0, "rare").otherwise("common").alias("category"),
                F.sha2(F.col("id").cast("string"), 256).alias("payload"))
        .write.parquet(dim_path))

QUERY = f"""
SELECT d.category, count(*) AS n, sum(f.amount) AS total
FROM parquet.`{fact_path}` f
JOIN parquet.`{dim_path}` d ON f.dim_id = d.dim_id
WHERE d.category = 'rare'
GROUP BY d.category
"""

jvm.hbo.Hbo.install(spark._jsparkSession, args.history)
jvm.hbo.Hbo.clearHistory()

MB = 2 ** 20
started = datetime.now(timezone.utc).isoformat(timespec="seconds")
csv_fields = ["started", "label", "master", "parallelism", "local_reader", "host_local_read", "fact_rows", "dim_rows",
              "mode", "run", "learn", "wall_s", "task_time_s", "stages",
              "shuffle_write_mb", "shuffle_write_s", "shuffle_local_read_mb", "shuffle_remote_read_mb",
              "hbo_hints", "report"]
new_file = not os.path.exists(args.out)
out = open(args.out, "a", newline="")
writer = csv.DictWriter(out, fieldnames=csv_fields)
if new_file:
    writer.writeheader()
stages_out = open(os.path.splitext(args.out)[0] + "-stages.jsonl", "a")


def run(mode, i, learn=False):
    jvm.hbo.Hbo.resetCounters()
    t0 = time.perf_counter()
    spark.sql(QUERY).collect()
    wall = time.perf_counter() - t0
    m = json.loads(jvm.hbo.Hbo.metricsJson())
    row = {
        "started": started, "label": label, "master": args.master, "parallelism": parallelism,
        "local_reader": args.local_reader, "host_local_read": args.host_local_read, "fact_rows": args.fact_rows, "dim_rows": args.dim_rows,
        "mode": mode, "run": i, "learn": learn, "wall_s": round(wall, 3),
        "task_time_s": round(m["task_time_ms"] / 1000, 3), "stages": m["stages"],
        "shuffle_write_mb": round(m["shuffle_write_bytes"] / MB, 2),
        "shuffle_write_s": round(m["shuffle_write_time_ns"] / 1e9, 3),
        "shuffle_local_read_mb": round(m["shuffle_local_read_bytes"] / MB, 2),
        "shuffle_remote_read_mb": round(m["shuffle_remote_read_bytes"] / MB, 2),
        "hbo_hints": m["hbo_hints"], "report": m["report"],
    }
    writer.writerow(row)
    out.flush()
    stages_out.write(json.dumps({"label": label, "mode": mode, "run": i, "per_stage": m["per_stage"]}) + "\n")
    name = f"{mode} #{i}" + (" (learn)" if learn else "")
    print(f"{name:<16} wall={row['wall_s']:6.2f}s  tasks={row['task_time_s']:6.2f}s  "
          f"shuffle: write={row['shuffle_write_mb']:7.1f}MB/{row['shuffle_write_s']:5.2f}s "
          f"read local={row['shuffle_local_read_mb']:7.1f}MB remote={row['shuffle_remote_read_mb']:6.1f}MB  "
          f"hints={row['hbo_hints']}  | {m['report']}")
    return row


print(f"\nmaster={args.master} parallelism={parallelism} local_reader={args.local_reader} "
      f"host_local_read={args.host_local_read} label={label}")

# прогрев JIT и кэша ФС, чтобы первый замер не был заведомо медленнее
jvm.hbo.Hbo.setEnabled(False)
spark.sql(QUERY).collect()

print("\n=== AQE only ===")
base = [run("aqe", i + 1) for i in range(args.runs)]

print("\n=== AQE + HBO ===")
jvm.hbo.Hbo.setEnabled(True)
jvm.hbo.Hbo.clearHistory()
run("hbo", 0, learn=True)  # обучающий запуск, в сводку не входит
hbo = [run("hbo", i + 1) for i in range(args.runs)]


def med(rows, k):
    return statistics.median(r[k] for r in rows)


print(f"\n{'':<12}{'wall, s':>10}{'tasks, s':>10}{'shuffle W, MB':>15}{'write, s':>10}{'remote R, MB':>14}   (медиана по {args.runs})")
for name, rows in (("AQE", base), ("AQE + HBO", hbo)):
    print(f"{name:<12}{med(rows, 'wall_s'):>10.2f}{med(rows, 'task_time_s'):>10.2f}"
          f"{med(rows, 'shuffle_write_mb'):>15.1f}{med(rows, 'shuffle_write_s'):>10.2f}{med(rows, 'shuffle_remote_read_mb'):>14.1f}")
speedup = med(base, "wall_s") / max(med(hbo, "wall_s"), 1e-9)
print(f"\nускорение по медиане: x{speedup:.1f};  цена исправления AQE ≈ "
      f"{med(base, 'wall_s') - med(hbo, 'wall_s'):.2f} s wall, "
      f"{med(base, 'task_time_s') - med(hbo, 'task_time_s'):.2f} s task time")
print(f"results -> {args.out}, history entries: {jvm.hbo.Hbo.historySize()}")
out.close()
stages_out.close()
spark.stop()
