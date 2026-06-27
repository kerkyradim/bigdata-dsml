from __future__ import annotations

import argparse
import csv
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DATA_DIR = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data"
CRIME_FILES = [
    f"{DATA_DIR}/LA_Crime_Data_2010_2019.csv",
    f"{DATA_DIR}/LA_Crime_Data_2020_2025.csv",
]

TIME_OCC_IDX = 3
PREMIS_DESC_IDX = 15


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 1: share of STREET crimes per part of day (RDD API).",
    )
    parser.add_argument("--base-path", help="Base HDFS path where the output folder is written.")
    parser.add_argument("--output", help="Explicit output path (overrides --base-path).")
    return parser.parse_args()


def time_to_day_part(time_occ_str: str) -> str:
    try:
        hour = int(time_occ_str) // 100
    except (TypeError, ValueError):
        return "Night"
    if 5 <= hour <= 11:
        return "Morning"
    if 12 <= hour <= 16:
        return "Afternoon"
    if 17 <= hour <= 20:
        return "Evening"
    return "Night"


def street_crime_day_parts(line: str):
    if not line.strip() or line.startswith('"DR_NO"') or line.startswith("DR_NO"):
        return []

    row = next(csv.reader([line]))
    if len(row) <= PREMIS_DESC_IDX:
        return []
    if row[PREMIS_DESC_IDX] != "STREET":
        return []

    return [(time_to_day_part(row[TIME_OCC_IDX]), 1)]


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q1 street crime by day part - RDD").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    start = perf_counter()

    crimes = sc.textFile(",".join(CRIME_FILES))
    counts = crimes.flatMap(street_crime_day_parts).reduceByKey(lambda left, right: left + right)

    total = counts.map(lambda item: item[1]).reduce(lambda left, right: left + right)
    results = [
        (day_part, count, round(count * 100.0 / total, 2))
        for day_part, count in counts.collect()
    ]
    results.sort(key=lambda item: item[2], reverse=True)

    elapsed = perf_counter() - start

    for row in results:
        print(row)
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = f"{args.base_path.rstrip('/')}/Q1_rdd_{sc.applicationId}"
    if output_path:
        sc.parallelize(results, 1).map(lambda row: f"{row[0]},{row[1]},{row[2]}").saveAsTextFile(output_path)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
