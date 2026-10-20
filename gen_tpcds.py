"""
Генерация данных TPC-DS в parquet для замеров на Spark.

По умолчанию — через расширение tpcds в DuckDB (ставится через pip, компилировать ничего не надо):

    pip install duckdb
    python gen_tpcds.py --sf 1    --out /tmp/tpcds-sf1      # ~1 ГБ сырых данных, 1–2 минуты
    python gen_tpcds.py --sf 10   --out /tmp/tpcds-sf10     # ~10 ГБ сырых, 10–20 минут, ~4 ГБ parquet

Запасной путь, если расширение DuckDB недоступно: сгенерировать .dat официальной утилитой
dsdgen (github.com/gregrahn/tpcds-kit) и сконвертировать:

    ./dsdgen -SCALE 1 -DIR /tmp/tpcds-raw -TERMINATE N
    python gen_tpcds.py --from-dat /tmp/tpcds-raw --ddl tpcds-kit/tools/tpcds.sql --out /tmp/tpcds-sf1

Результат: <out>/<таблица>/data.parquet — по папке на таблицу (Spark читает папку как таблицу).
Статистика таблиц не собирается — это и есть типичный случай лейкхауса.
"""
import argparse
import os
import time

import duckdb

TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]

p = argparse.ArgumentParser()
p.add_argument("--sf", type=float, default=1, help="scale factor (1 ≈ 1 ГБ сырых данных)")
p.add_argument("--out", required=True)
p.add_argument("--from-dat", help="папка с .dat от dsdgen (вместо расширения DuckDB)")
p.add_argument("--ddl", help="tpcds.sql из tpcds-kit (нужен с --from-dat)")
p.add_argument("--threads", type=int, default=os.cpu_count())
args = p.parse_args()

con = duckdb.connect()
con.execute(f"SET threads = {args.threads}")
t0 = time.time()

if args.from_dat:
    if not args.ddl:
        raise SystemExit("--from-dat needs --ddl path/to/tpcds.sql")
    con.execute(open(args.ddl).read())
    for t in TABLES:
        path = os.path.join(args.from_dat, f"{t}.dat")
        con.execute(f"COPY {t} FROM '{path}' (DELIMITER '|', HEADER false, NULL '')")
else:
    con.execute("INSTALL tpcds; LOAD tpcds;")
    print(f"generating TPC-DS sf={args.sf} ...")
    con.execute(f"CALL dsdgen(sf={args.sf})")

for t in TABLES:
    d = os.path.join(args.out, t)
    os.makedirs(d, exist_ok=True)
    con.execute(f"COPY {t} TO '{d}/data.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 1000000)")
    n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    print(f"  {t:<24} {n:>12,} rows")

print(f"done in {time.time() - t0:.0f}s -> {args.out}")
