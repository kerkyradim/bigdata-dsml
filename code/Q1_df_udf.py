from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DATA_DIR = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data"
CRIME_FILES = [
    f"{DATA_DIR}/LA_Crime_Data_2010_2019.csv",
    f"{DATA_DIR}/LA_Crime_Data_2020_2025.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 1: share of STREET crimes per part of day (DataFrame API with UDF).",
    )
    parser.add_argument("--base-path", help="Base HDFS path where the output folder is written.")
    parser.add_argument("--output", help="Explicit output path (overrides --base-path).")
    return parser.parse_args()


def get_day_part(time_occ):
    if time_occ is None:
        return "Night"
    try:
        hour = int(time_occ) // 100
    except (TypeError, ValueError):
        return "Night"
    if 5 <= hour <= 11:
        return "Morning"
    if 12 <= hour <= 16:
        return "Afternoon"
    if 17 <= hour <= 20:
        return "Evening"
    return "Night"


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q1 street crime by day part - DataFrame + UDF").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    day_part_udf = udf(get_day_part, StringType())
    start = perf_counter()

    crimes = (
        spark.read.option("header", "true").csv(CRIME_FILES)
        .withColumnRenamed("TIME OCC", "time_occ")
        .withColumnRenamed("Premis Desc", "premis_desc")
    )
    
    day_part_udf=udf(get_day_part, StringType())
    street = crimes.filter(F.col("premis_desc") == "STREET").withColumn(
        "day_part",
        day_part_udf(F.col("time_occ")),
    )

    counts = street.groupBy("day_part").count()
    total = counts.agg(F.sum("count").alias("t")).collect()[0]["t"]
    result = (
        counts.withColumn("percentage", F.round(F.col("count") * 100.0 / F.lit(total), 2))
        .orderBy(F.col("percentage").desc())
    )

    rows = result.collect()
    elapsed = perf_counter() - start

    for row in rows:
        print((row["day_part"], row["count"], row["percentage"]))
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = f"{args.base_path.rstrip('/')}/Q1_df_udf_{spark.sparkContext.applicationId}"
    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
