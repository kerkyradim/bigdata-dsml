from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DATA_DIR = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data"
CRIME_FILES = [
    f"{DATA_DIR}/LA_Crime_Data_2010_2019.csv",
    f"{DATA_DIR}/LA_Crime_Data_2020_2025.csv",
]
DATE_PATTERN = "yyyy MMM dd hh:mm:ss a"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 2: top 3 months by crime count for each year (DataFrame API).",
    )
    parser.add_argument("--base-path", help="Base HDFS path where the output folder is written.")
    parser.add_argument("--output", help="Explicit output path (overrides --base-path).")
    return parser.parse_args()


def build_result(crimes):
    dated = (
        crimes.withColumn("occ_ts", F.to_timestamp(F.col("DATE OCC"), DATE_PATTERN))
        .withColumn("year", F.year("occ_ts"))
        .withColumn("month", F.month("occ_ts"))
        .filter(F.col("year").isNotNull() & F.col("month").isNotNull())
    )

    monthly = dated.groupBy("year", "month").agg(F.count("*").alias("crime_total"))

    ranking_window = Window.partitionBy("year").orderBy(F.col("crime_total").desc())
    top3 = (
        monthly.withColumn("ranking", F.row_number().over(ranking_window))
        .filter(F.col("ranking") <= 3)
    )

    return top3.orderBy(
        F.col("year").asc(),
        F.col("crime_total").desc(),
        F.col("ranking").asc(),
    )


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q2 top crime months per year - DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start = perf_counter()

    crimes = spark.read.option("header", "true").csv(CRIME_FILES)
    result = build_result(crimes)
    rows = result.collect()

    elapsed = perf_counter() - start

    for row in rows:
        print((row["year"], row["month"], row["crime_total"], row["ranking"]))
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = f"{args.base_path.rstrip('/')}/Q2_df_{spark.sparkContext.applicationId}"
    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
