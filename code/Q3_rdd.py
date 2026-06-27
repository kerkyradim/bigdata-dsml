from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

HDFS_DATA = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data"
CENSUS_GEOJSON = f"{HDFS_DATA}/LA_Census_Blocks_2020.geojson"
INCOME_CSV = f"{HDFS_DATA}/LA_income_2021.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 3: average annual per-capita income per ZIP (RDD API).",
    )
    parser.add_argument("--base-path", help="Base HDFS path where the output folder is written.")
    parser.add_argument("--output", help="Explicit output path (overrides --base-path).")
    return parser.parse_args()


def read_census_blocks(spark: SparkSession):
    return (
        spark.read.option("multiLine", "true")
        .json(CENSUS_GEOJSON)
        .selectExpr("explode(features) as features")
        .select(
            F.col("features.properties.ZCTA20").alias("ZCTA20"),
            F.col("features.properties.POP20").alias("POP20"),
            F.col("features.properties.HOUSING20").alias("HOUSING20"),
        )
    )


def census_zip_rdd(spark: SparkSession):
    blocks = read_census_blocks(spark)
    return (
        blocks.filter(F.col("ZCTA20").isNotNull() & (F.col("ZCTA20") != ""))
        .rdd.map(
            lambda row: (
                row.ZCTA20,
                (int(row.POP20 or 0), int(row.HOUSING20 or 0)),
            )
        )
        .reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))
    )


def parse_income_line(line: str):
    if not line.strip() or line.startswith("Zip Code"):
        return []

    parts = line.split(";")
    if len(parts) < 3:
        return []

    zip_code = parts[0].strip()
    income_raw = parts[2].strip().replace("$", "").replace(",", "")
    try:
        return [(zip_code, float(income_raw))]
    except ValueError:
        return []


def income_rdd(sc):
    return sc.textFile(INCOME_CSV).flatMap(parse_income_line)


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q3 per-capita income by ZIP - RDD").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    start = perf_counter()

    zip_rdd = census_zip_rdd(spark)
    income = income_rdd(sc)
    joined = zip_rdd.join(income)

    results = [
        (
            zip_code,
            totals[0],
            totals[1],
            median_income,
            round((median_income * totals[1]) / totals[0], 2),
        )
        for zip_code, (totals, median_income) in joined.collect()
        if totals[0] > 0
    ]
    results.sort(key=lambda row: row[0])

    elapsed = perf_counter() - start

    for row in results[:10]:
        print(row)
    if len(results) > 10:
        print(f"... ({len(results)} ZIP codes total)")
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = f"{args.base_path.rstrip('/')}/Q3_rdd_{sc.applicationId}"
    if output_path:
        sc.parallelize(results, 1).map(
            lambda row: ",".join(str(value) for value in row)
        ).saveAsTextFile(output_path)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
