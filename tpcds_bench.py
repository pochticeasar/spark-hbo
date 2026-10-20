"""
Замер на TPC-DS: сколько AQE тратит на исправления и сколько из этого убирает история.

Для каждого запроса:
  1. прогрев (AQE, без истории);
  2. noaqe — AQE выключен (статический план);
  3. aqe   — AQE включён, история выключена;
  4. hbo   — обучающий запуск (не в сводке), затем замер с историей.

Запуск:
    python gen_tpcds.py --sf 1 --out /tmp/tpcds-sf1      # данные (один раз)
    python tpcds_bench.py --data /tmp/tpcds-sf1                    # все 103 запроса
    python tpcds_bench.py --data /tmp/tpcds-sf1 --queries q3,q7,q19 # подмножество
    python tpcds_bench.py --data /tmp/tpcds-sf1 --broadcast-threshold 1m  # имитация большего масштаба

Запросы (адаптированные под Spark SQL) скачиваются из репозитория Apache Spark
при первом запуске в папку tpcds-queries/.
Результаты: tpcds_results.csv (строка на прогон) + сводка в конце.
"""
import argparse
import csv
import json
import os
import statistics
import time
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import SparkSession

TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]
QUERY_NAMES = [n for i in range(1, 100)
               for n in ([f"q{i}a", f"q{i}b"] if i in (14, 23, 24, 39) else [f"q{i}"])]
QUERY_URL = "https://raw.githubusercontent.com/apache/spark/v3.5.3/sql/core/src/test/resources/tpcds/{}.sql"

p = argparse.ArgumentParser()
p.add_argument("--jar", default="target/scala-2.12/spark-hbo_2.12-0.1.jar")
p.add_argument("--data", required=True, help="папка от gen_tpcds.py")
p.add_argument("--master", default="local[4]")
p.add_argument("--label", default=None)
p.add_argument("--queries", default="", help="через запятую, например q3,q7,q14a; по умолчанию все")
p.add_argument("--runs", type=int, default=1, help="повторов каждого режима (медиана)")
p.add_argument("--history", default="/tmp/hbo-tpcds-history.tsv")
p.add_argument("--queries-dir", default="tpcds-queries")
p.add_argument("--shuffle-partitions", type=int, default=64)
p.add_argument("--broadcast-threshold", default="10m",
               help="spark.sql.autoBroadcastJoinThreshold (по умолчанию 10m). На малых SF справочники "
                    "крошечные и ошибка оценки не возникает; уменьшение порога (например, 1m на SF1) "
                    "грубо имитирует соотношение «справочник / порог» на больших масштабах")
p.add_argument("--driver-memory", default="6g")
p.add_argument("--out", default="tpcds_results.csv")
args = p.parse_args()
label = args.label or f"{os.path.basename(args.data.rstrip('/'))}:{args.master}:bt={args.broadcast_threshold}"

# ---------- запросы ----------
os.makedirs(args.queries_dir, exist_ok=True)
names = [q.strip() for q in args.queries.split(",") if q.strip()] or QUERY_NAMES
for q in names:
    path = os.path.join(args.queries_dir, f"{q}.sql")
    if not os.path.exists(path):
        with urllib.request.urlopen(QUERY_URL.format(q)) as r, open(path, "wb") as f:
            f.write(r.read())

spark = (
    SparkSession.builder.master(args.master).appName(f"spark-hbo-tpcds:{label}")
    .config("spark.jars", os.path.abspath(args.jar))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    .config("spark.sql.autoBroadcastJoinThreshold", args.broadcast_threshold)
    .config("spark.driver.memory", args.driver_memory)
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
jvm = spark._jvm
for t in TABLES:
    spark.read.parquet(os.path.join(args.data, t)).createOrReplaceTempView(t)

jvm.hbo.Hbo.install(spark._jsparkSession, args.history)
jvm.hbo.Hbo.clearHistory()

MB = 2 ** 20
started = datetime.now(timezone.utc).isoformat(timespec="seconds")
fields = ["started", "label", "query", "mode", "run", "ok", "wall_s", "task_time_s", "stages",
          "shuffle_write_mb", "shuffle_remote_read_mb", "initial_joins", "final_joins", "hbo_hints", "error"]
new_file = not os.path.exists(args.out)
out = open(args.out, "a", newline="")
writer = csv.DictWriter(out, fieldnames=fields)
if new_file:
    writer.writeheader()


def set_mode(mode):
    spark.conf.set("spark.sql.adaptive.enabled", "false" if mode == "noaqe" else "true")
    jvm.hbo.Hbo.setEnabled(mode == "hbo")


def run(q, sql, mode, i, record=True):
    set_mode(mode)
    jvm.hbo.Hbo.resetCounters()
    t0 = time.perf_counter()
    err = ""
    try:
        spark.sql(sql).collect()
        ok = True
    except Exception as e:  # noqa: BLE001 — запрос может не поддерживаться, фиксируем и идём дальше
        ok, err = False, str(e).splitlines()[0][:200]
    wall = time.perf_counter() - t0
    m = json.loads(jvm.hbo.Hbo.metricsJson())
    rep = m["report"]
    ini = rep.split("initial=")[1].split(" ")[0] if "initial=" in rep else ""
    fin = rep.split("final=")[1].split(" ")[0] if "final=" in rep else ""
    row = {"started": started, "label": label, "query": q, "mode": mode, "run": i, "ok": ok,
           "wall_s": round(wall, 3), "task_time_s": round(m["task_time_ms"] / 1000, 3), "stages": m["stages"],
           "shuffle_write_mb": round(m["shuffle_write_bytes"] / MB, 2),
           "shuffle_remote_read_mb": round(m["shuffle_remote_read_bytes"] / MB, 2),
           "initial_joins": ini, "final_joins": fin, "hbo_hints": m["hbo_hints"], "error": err}
    if record:
        writer.writerow(row)
        out.flush()
    return row


summary = []
print(f"label={label} queries={len(names)} runs={args.runs}\n")
print(f"{'query':<6}{'noaqe':>8}{'aqe':>8}{'hbo':>8}  {'shuffle aqe→hbo, MB':>22}  {'SMJ→BHJ':>8}  hints")
for q in names:
    sql = open(os.path.join(args.queries_dir, f"{q}.sql")).read().strip().rstrip(";")
    run(q, sql, "aqe", 0, record=False)  # прогрев
    res = {}
    for mode in ("noaqe", "aqe"):
        res[mode] = [run(q, sql, mode, i + 1) for i in range(args.runs)]
    run(q, sql, "hbo", 0)  # обучение
    res["hbo"] = [run(q, sql, "hbo", i + 1) for i in range(args.runs)]
    if not all(r["ok"] for rows in res.values() for r in rows):
        print(f"{q:<6}  error: {next(r['error'] for rows in res.values() for r in rows if not r['ok'])}")
        continue
    med = {k: statistics.median(r["wall_s"] for r in v) for k, v in res.items()}
    shuf = {k: statistics.median(r["shuffle_write_mb"] for r in v) for k, v in res.items()}
    a = res["aqe"][0]
    switched = a["initial_joins"].count("SMJ") - a["final_joins"].count("SMJ")
    hints = res["hbo"][0]["hbo_hints"]
    summary.append({"q": q, **{f"w_{k}": v for k, v in med.items()}, **{f"s_{k}": v for k, v in shuf.items()},
                    "switched": switched, "hints": hints})
    print(f"{q:<6}{med['noaqe']:>8.2f}{med['aqe']:>8.2f}{med['hbo']:>8.2f}  "
          f"{shuf['aqe']:>10.1f} → {shuf['hbo']:<9.1f}  {switched:>8}  {hints}")

# ---------- сводка ----------
if summary:
    tot = {k: sum(s[f"w_{k}"] for s in summary) for k in ("noaqe", "aqe", "hbo")}
    sh = {k: sum(s[f"s_{k}"] for s in summary) for k in ("noaqe", "aqe", "hbo")}
    corrected = [s for s in summary if s["switched"] > 0]
    hinted = [s for s in summary if s["hints"] > 0]
    print(f"\nзапросов выполнено: {len(summary)} из {len(names)}")
    print(f"суммарное время, s:   noaqe {tot['noaqe']:.1f} | aqe {tot['aqe']:.1f} | aqe+hbo {tot['hbo']:.1f}")
    print(f"суммарный shuffle, MB: noaqe {sh['noaqe']:.0f} | aqe {sh['aqe']:.0f} | aqe+hbo {sh['hbo']:.0f}")
    print(f"AQE переключал SMJ→BHJ в {len(corrected)} запросах; HBO поставил хинты в {len(hinted)}")
    if corrected:
        c_aqe = sum(s["w_aqe"] for s in corrected)
        c_hbo = sum(s["w_hbo"] for s in corrected)
        print(f"  в этих запросах: aqe {c_aqe:.1f}s → aqe+hbo {c_hbo:.1f}s "
              f"(цена исправления ≈ {c_aqe - c_hbo:.1f}s, {100 * (c_aqe - c_hbo) / max(c_aqe, 1e-9):.0f}%)")
    top = sorted(summary, key=lambda s: s["w_aqe"] - s["w_hbo"], reverse=True)[:10]
    print("топ-10 по выигрышу HBO: " + ", ".join(f"{s['q']} ({s['w_aqe'] - s['w_hbo']:+.2f}s)" for s in top))
print(f"results -> {args.out}")
out.close()
spark.stop()
