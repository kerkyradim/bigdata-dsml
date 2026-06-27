from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DATA_DIR = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data"
CRIME_FILES = [
    f"{DATA_DIR}/LA_Crime_Data_2010_2019.csv",
    f"{DATA_DIR}/LA_Crime_Data_2020_2025.csv",
]
DATE_PATTERN = "yyyy MMM dd hh:mm:ss a"

MONTHLY_COUNTS_SQL = """
SELECT
    YEAR(occ_ts) AS year,
    MONTH(occ_ts) AS month,
    COUNT(*) AS crime_total
FROM crimes_occ
WHERE occ_ts IS NOT NULL
GROUP BY YEAR(occ_ts), MONTH(occ_ts)
"""

RANKED_MONTHS_SQL = """
SELECT
    year,
    month,
    crime_total,
    ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
FROM monthly_counts
"""

TOP3_SQL = """
SELECT year, month, crime_total, ranking
FROM ranked_months
WHERE ranking <= 3
ORDER BY year ASC, crime_total DESC, ranking ASC
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query 2: top 3 months by crime count for each year (Spark SQL)",
    )
    parser.add_argument("--base-path", help="Base HDFS path where the output folder is written.")
    parser.add_argument("--output", help="Explicit output path (overrides --base-path).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("Q2 top crime months per year - SQL").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start = perf_counter()

    crimes = spark.read.option("header", "true").csv(CRIME_FILES)
    crimes_occ = crimes.withColumn(
        "occ_ts",
        F.to_timestamp(F.col("DATE OCC"), DATE_PATTERN),
    )
    crimes_occ.createOrReplaceTempView("crimes_occ")

    spark.sql(MONTHLY_COUNTS_SQL).createOrReplaceTempView("monthly_counts")
    spark.sql(RANKED_MONTHS_SQL).createOrReplaceTempView("ranked_months")
    result = spark.sql(TOP3_SQL)
    rows = result.collect()

    elapsed = perf_counter() - start

    for row in rows:
        print((row["year"], row["month"], row["crime_total"], row["ranking"]))
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = f"{args.base_path.rstrip('/')}/Q2_sql_{spark.sparkContext.applicationId}"
    if output_path:
        result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
