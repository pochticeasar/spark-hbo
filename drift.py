"""
Эксперимент на протухание истории.

1. Обучаемся на справочнике v1: фильтр `category = 'rare'` пропускает каждую 2000-ю строку
   (крошечный результат) — HBO начинает ставить broadcast.
2. Подменяем справочник на v2 по ТОМУ ЖЕ пути: строк столько же, файлы почти того же
   размера (фактор размера в ключе не меняется), но 'rare' теперь у всех строк —
   результат фильтра больше порога broadcast.
3. Смотрим, что делает HBO: первый запуск ещё идёт по устаревшей истории
   (broadcast большой стороны), листенер записывает новый реальный размер,
   и со следующего запуска хинт больше не ставится.

Запуск:  python drift.py
"""
import argparse
import json
import os
import shutil

from pyspark.sql import SparkSession, functions as F

p = argparse.ArgumentParser()
p.add_argument("--jar", default="target/scala-2.12/spark-hbo_2.12-0.1.jar")
p.add_argument("--master", default="local[4]")
p.add_argument("--data", default="/tmp/hbo-drift")
p.add_argument("--history", default="/tmp/hbo-drift-history.tsv")
p.add_argument("--fact-rows", type=int, default=4_000_000)
p.add_argument("--dim-rows", type=int, default=400_000)
args = p.parse_args()

spark = (
    SparkSession.builder.master(args.master).appName("spark-hbo-drift")
    .config("spark.jars", os.path.abspath(args.jar))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.driver.memory", "4g")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
jvm = spark._jvm
threshold_mb = int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").rstrip("b")) / 2**20

fact_path, dim_path = f"{args.data}/fact", f"{args.data}/dim"


def write_dim(rare_every):
    """rare_every=2000 -> каждая 2000-я строка 'rare'; rare_every=1 -> все строки."""
    tmp = dim_path + ".tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    (spark.range(args.dim_rows)
        .select(F.col("id").alias("dim_id"),
                F.when(F.col("id") % rare_every == 0, "rare").otherwise("common").alias("category"),
                F.sha2(F.col("id").cast("string"), 256).alias("payload"))
        .write.parquet(tmp))
    shutil.rmtree(dim_path, ignore_errors=True)
    os.rename(tmp, dim_path)
    size = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(dim_path) for f in fs)
    print(f"  dim: rare every {rare_every} row(s), files {size / 2**20:.1f} MB")


if not os.path.exists(fact_path):
    (spark.range(args.fact_rows)
        .select((F.col("id") % args.dim_rows).alias("dim_id"), (F.rand(1) * 100).alias("amount"))
        .write.parquet(fact_path))

QUERY = f"""
SELECT d.category, count(*) AS n, sum(f.amount) AS total
FROM parquet.`{fact_path}` f
JOIN parquet.`{dim_path}` d ON f.dim_id = d.dim_id
WHERE d.category = 'rare'
GROUP BY d.category
"""

jvm.hbo.Hbo.install(spark._jsparkSession, args.history)
jvm.hbo.Hbo.clearHistory()
jvm.hbo.Hbo.setEnabled(True)


def run(name):
    jvm.hbo.Hbo.resetCounters()
    spark.catalog.clearCache()
    rows = spark.sql(QUERY).collect()
    m = json.loads(jvm.hbo.Hbo.metricsJson())
    n = rows[0]["n"] if rows else 0
    print(f"  {name:<28} rows_joined={n:<9} hints={m['hbo_hints']}  "
          f"shuffle={m['shuffle_write_bytes'] / 2**20:6.1f}MB  | {m['report']}")


print(f"\nbroadcast threshold = {threshold_mb:.0f} MB")
print("\n[1] обучение на справочнике v1 (фильтр почти всё отсекает)")
write_dim(2000)
run("v1 learn")
run("v1 with history")

print("\n[2] данные изменились: тот же путь, почти тот же размер файлов, фильтр пропускает всё")
write_dim(1)
run("v2 run 1 (stale history)")
run("v2 run 2 (history refreshed)")
run("v2 run 3")
spark.stop()
