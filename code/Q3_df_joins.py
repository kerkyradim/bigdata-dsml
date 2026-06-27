from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from contextlib import redirect_stdout
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

HDFS_DATA = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data"
CENSUS_GEOJSON = f"{HDFS_DATA}/LA_Census_Blocks_2020.geojson"
INCOME_CSV = f"{HDFS_DATA}/LA_income_2021.csv"

JOIN_STRATEGIES = (
    "default",
    "broadcast",
    "merge",
    "shuffle_hash",
    "shuffle_replicate_nl",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 3 join-strategy experiments (DataFrame API).",
    )
    parser.add_argument(
        "--join-strategy",
        choices=JOIN_STRATEGIES,
        default="default",
        help="Join hint/strategy to test.",
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


def read_income(spark: SparkSession):
    income_raw = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .csv(INCOME_CSV)
        .withColumnRenamed("Zip Code", "zip_code")
        .withColumnRenamed("Estimated Median Income", "median_household_income_raw")
    )
    return income_raw.withColumn(
        "median_household_income",
        F.regexp_replace(F.col("median_household_income_raw"), r"[$,]", "").cast("double"),
    ).select("zip_code", "median_household_income")


def zip_population_df(spark: SparkSession, blocks):
    aggregated = (
        blocks.filter(F.col("ZCTA20").isNotNull() & (F.col("ZCTA20") != ""))
        .rdd.map(
            lambda row: (row.ZCTA20, (int(row.POP20 or 0), int(row.HOUSING20 or 0)))
        )
        .reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))
    )
    return spark.createDataFrame(
        aggregated.map(lambda item: (item[0], item[1][0], item[1][1])),
        ["zip_code", "total_population", "total_housing_units"],
    )


def join_with_strategy(zip_df, income_df, strategy: str):
    if strategy == "default":
        return zip_df.join(income_df, on="zip_code", how="inner")
    if strategy == "broadcast":
        return zip_df.join(broadcast(income_df), on="zip_code", how="inner")
    return zip_df.join(income_df.hint(strategy), on="zip_code", how="inner")


def finalize_result(joined):
    return (
        joined.withColumn(
            "avg_annual_per_capita_income",
            F.round(
                (F.col("median_household_income") * F.col("total_housing_units"))
                / F.col("total_population"),
                2,
            ),
        )
        .select(
            "zip_code",
            "total_population",
            "total_housing_units",
            "median_household_income",
            "avg_annual_per_capita_income",
        )
        .orderBy("zip_code")
    )


def capture_explain(df) -> str:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain("formatted")
    return buffer.getvalue()


def detect_join_operators(plan: str) -> str:
    markers = [
        "BroadcastHashJoin",
        "SortMergeJoin",
        "ShuffledHashJoin",
        "BroadcastNestedLoopJoin",
    ]
    found = [name for name in markers if name in plan]
    return ", ".join(found) if found else "unknown"


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName(f"Q3 join strategy {args.join_strategy} - DataFrame")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    blocks = read_census_blocks(spark)
    zip_df = zip_population_df(spark, blocks)
    income_df = read_income(spark)
    joined = join_with_strategy(zip_df, income_df, args.join_strategy)
    result = finalize_result(joined)

    plan = capture_explain(result)
    print(f"JOIN_STRATEGY={args.join_strategy}")
    print(f"DETECTED_JOIN_OPERATORS={detect_join_operators(plan)}")
    print("=== EXPLAIN FORMATTED ===")
    print(plan)

    start = perf_counter()
    rows = result.collect()
    elapsed = perf_counter() - start

    for row in rows[:5]:
        print(
            (
                row["zip_code"],
                row["total_population"],
                row["median_household_income"],
                row["avg_annual_per_capita_income"],
            )
        )
    if len(rows) > 5:
        print(f"... ({len(rows)} ZIP codes total)")
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = (
            f"{args.base_path.rstrip('/')}/Q3_df_join_{args.join_strategy}_"
            f"{spark.sparkContext.applicationId}"
        )
    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
