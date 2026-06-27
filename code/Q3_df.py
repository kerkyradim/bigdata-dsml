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
        description=(
            "Query 3: average annual per-capita income per ZIP "
            "(2020 census population/housing + 2021 median household income)."
        ),
    )
    parser.add_argument("--base-path", help="Base HDFS path where the output folder is written.")
    parser.add_argument("--output", help="Explicit output path (overrides --base-path).")
    parser.add_argument(
        "--show-plan",
        action="store_true",
        help="Print Catalyst physical plan (explain formatted) before execution.",
    )
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


def build_result(spark: SparkSession):
    blocks = read_census_blocks(spark)
    zip_population = zip_population_df(spark, blocks)
    income = read_income(spark)

    
    joined = zip_population.join(income, on="zip_code", how="inner")

    
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


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q3 per-capita income by ZIP - DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start = perf_counter()
    result = build_result(spark)

    if args.show_plan:
        print("=== EXPLAIN FORMATTED (default join) ===")
        result.explain("formatted")

    rows = result.collect()
    elapsed = perf_counter() - start

    for row in rows[:10]:
        print(
            (
                row["zip_code"],
                row["total_population"],
                row["median_household_income"],
                row["avg_annual_per_capita_income"],
            )
        )
    if len(rows) > 10:
        print(f"... ({len(rows)} ZIP codes total)")
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = f"{args.base_path.rstrip('/')}/Q3_df_{spark.sparkContext.applicationId}"
    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
