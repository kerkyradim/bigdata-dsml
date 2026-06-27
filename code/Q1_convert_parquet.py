from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DATA_DIR = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data"
CRIME_FILES = [
    f"{DATA_DIR}/LA_Crime_Data_2010_2019.csv",
    f"{DATA_DIR}/LA_Crime_Data_2020_2025.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert LA crime CSV files to Parquet on HDFS (one-time setup).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="HDFS directory where Parquet files will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q1 convert crime CSV to Parquet").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    crimes = spark.read.option("header", "true").csv(CRIME_FILES)
    crimes.write.mode("overwrite").parquet(args.output.rstrip("/"))

    row_count = crimes.count()
    print(f"PARQUET_OUTPUT={args.output.rstrip('/')}")
    print(f"ROW_COUNT={row_count}")

    spark.stop()


if __name__ == "__main__":
    main()
