from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DEFAULT_PARQUET_DIR = (
    "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289/LA_Crime_Data_parquet"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 1 over Parquet input (same logic as Q1_df.py).",
    )
    parser.add_argument(
        "--parquet-path",
        default=DEFAULT_PARQUET_DIR,
        help="HDFS directory with Parquet crime data.",
    )
    parser.add_argument("--base-path", help="Base HDFS path where the output folder is written.")
    parser.add_argument("--output", help="Explicit output path (overrides --base-path).")
    return parser.parse_args()


def build_result(crimes):
    street = crimes.filter(F.col("premis_desc") == "STREET")

    hour = (F.col("time_occ").cast("int") / F.lit(100)).cast("int")
    day_part = (
        F.when((hour >= 5) & (hour <= 11), "Morning")
        .when((hour >= 12) & (hour <= 16), "Afternoon")
        .when((hour >= 17) & (hour <= 20), "Evening")
        .otherwise("Night")
    )
    street = street.withColumn("day_part", day_part)

    counts = street.groupBy("day_part").count()
    total = counts.agg(F.sum("count").alias("t")).collect()[0]["t"]
    return counts.withColumn(
        "percentage",
        F.round(F.col("count") * 100.0 / F.lit(total), 2),
    ).orderBy(F.col("percentage").desc())


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q1 street crime by day part - Parquet").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start = perf_counter()

    crimes = (
        spark.read.parquet(args.parquet_path)
        .withColumnRenamed("TIME OCC", "time_occ")
        .withColumnRenamed("Premis Desc", "premis_desc")
    )
    result = build_result(crimes)
    rows = result.collect()
    elapsed = perf_counter() - start

    for row in rows:
        print((row["day_part"], row["count"], row["percentage"]))
    print(f"INPUT_FORMAT=parquet")
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = f"{args.base_path.rstrip('/')}/Q1_df_parquet_{spark.sparkContext.applicationId}"
    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
