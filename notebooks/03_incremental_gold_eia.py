# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# =====================================================
# CONFIGURATION
# =====================================================

CATALOG = "dbw_energy_dev"

SILVER_TABLE = (
    f"{CATALOG}.silver.eia_hourly_incremental"
)

# We intentionally create a new incremental Gold table
# so the original project Gold table remains untouched.
GOLD_TABLE = (
    f"{CATALOG}.gold.eia_daily_kpi_incremental"
)


# =====================================================
# CREATE GOLD SCHEMA
# =====================================================

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold"
)


# =====================================================
# READ INCREMENTAL SILVER
# =====================================================

silver = spark.table(
    SILVER_TABLE
)

silver_count = silver.count()

print(
    "Silver records:",
    silver_count
)


# =====================================================
# SILVER DATA QUALITY
# =====================================================

invalid_silver_count = (
    silver
    .filter(
        F.col("period").isNull()
        | F.col("date").isNull()
        | F.col("respondent").isNull()
        | F.col("actual_demand_mwh").isNull()
        | F.col("forecast_demand_mwh").isNull()
        | F.col("absolute_error_mwh").isNull()
        | F.col("source_modified_at").isNull()
    )
    .count()
)

if invalid_silver_count > 0:
    raise ValueError(
        "Invalid Silver records detected: "
        f"{invalid_silver_count}"
    )


# =====================================================
# CHECK SILVER BUSINESS KEY UNIQUENESS
# =====================================================

silver_duplicate_count = (
    silver
    .groupBy(
        "period",
        "respondent"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if silver_duplicate_count > 0:
    raise ValueError(
        "Duplicate Silver business keys detected: "
        f"{silver_duplicate_count}"
    )

print(
    "Silver validation: PASSED"
)


# =====================================================
# BUILD DAILY GOLD KPIs
# =====================================================
#
# Grain:
#   one row per date + respondent
#
# Metrics:
#   hour_count
#   avg_hourly_demand_mwh
#   peak_demand_mwh
#   total_daily_energy_mwh
#   mae_mwh
#
# source_modified_at represents the newest upstream
# source modification used by that day's KPI.

gold_source = (
    silver
    .groupBy(
        "date",
        "respondent"
    )
    .agg(
        F.count("*").alias(
            "hour_count"
        ),

        F.avg(
            "actual_demand_mwh"
        ).alias(
            "avg_hourly_demand_mwh"
        ),

        F.max(
            "actual_demand_mwh"
        ).alias(
            "peak_demand_mwh"
        ),

        F.sum(
            "actual_demand_mwh"
        ).alias(
            "total_daily_energy_mwh"
        ),

        F.avg(
            "absolute_error_mwh"
        ).alias(
            "mae_mwh"
        ),

        F.max(
            "source_modified_at"
        ).alias(
            "source_modified_at"
        )
    )
)


# =====================================================
# GOLD SOURCE DATA QUALITY
# =====================================================

invalid_gold_count = (
    gold_source
    .filter(
        F.col("date").isNull()
        | F.col("respondent").isNull()
        | F.col("hour_count").isNull()
        | F.col("avg_hourly_demand_mwh").isNull()
        | F.col("peak_demand_mwh").isNull()
        | F.col("total_daily_energy_mwh").isNull()
        | F.col("mae_mwh").isNull()
        | F.col("source_modified_at").isNull()
    )
    .count()
)

if invalid_gold_count > 0:
    raise ValueError(
        "Invalid Gold KPI rows detected: "
        f"{invalid_gold_count}"
    )


# =====================================================
# COMPLETE-DAY VALIDATION
# =====================================================
# Current pipeline only promotes complete daily files.
# Therefore every Gold day should contain exactly
# 24 hourly Silver records.

incomplete_day_count = (
    gold_source
    .filter(
        F.col("hour_count") != 24
    )
    .count()
)

if incomplete_day_count > 0:

    print(
        "Incomplete Gold days detected:"
    )

    display(
        gold_source
        .filter(
            F.col("hour_count") != 24
        )
        .select(
            "date",
            "respondent",
            "hour_count"
        )
        .orderBy("date")
    )

    raise ValueError(
        "Gold contains incomplete days: "
        f"{incomplete_day_count}"
    )


# =====================================================
# CHECK GOLD BUSINESS KEY UNIQUENESS
# =====================================================

gold_duplicate_count = (
    gold_source
    .groupBy(
        "date",
        "respondent"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if gold_duplicate_count > 0:
    raise ValueError(
        "Duplicate Gold source keys detected: "
        f"{gold_duplicate_count}"
    )


gold_source_count = (
    gold_source.count()
)

print(
    "Gold source records:",
    gold_source_count
)

print(
    "Gold source validation: PASSED"
)


# =====================================================
# CREATE INCREMENTAL GOLD TABLE
# =====================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
    date DATE,
    respondent STRING,
    hour_count BIGINT,
    avg_hourly_demand_mwh DOUBLE,
    peak_demand_mwh DOUBLE,
    total_daily_energy_mwh DOUBLE,
    mae_mwh DOUBLE,
    source_modified_at TIMESTAMP
)
USING DELTA
""")


# =====================================================
# INCREMENTAL GOLD MERGE
# =====================================================
#
# Business key:
#   date + respondent
#
# If a historical EIA revision changes any hourly
# Silver value, the daily KPI row is updated.

gold_delta = DeltaTable.forName(
    spark,
    GOLD_TABLE
)

(
    gold_delta.alias("target")
    .merge(
        gold_source.alias("source"),
        """
        target.date = source.date
        AND target.respondent = source.respondent
        """
    )
    .whenMatchedUpdate(
        condition="""
        NOT (
            target.hour_count
                <=> source.hour_count
        )
        OR NOT (
            target.avg_hourly_demand_mwh
                <=> source.avg_hourly_demand_mwh
        )
        OR NOT (
            target.peak_demand_mwh
                <=> source.peak_demand_mwh
        )
        OR NOT (
            target.total_daily_energy_mwh
                <=> source.total_daily_energy_mwh
        )
        OR NOT (
            target.mae_mwh
                <=> source.mae_mwh
        )
        OR NOT (
            target.source_modified_at
                <=> source.source_modified_at
        )
        """,
        set={
            "hour_count":
                "source.hour_count",

            "avg_hourly_demand_mwh":
                "source.avg_hourly_demand_mwh",

            "peak_demand_mwh":
                "source.peak_demand_mwh",

            "total_daily_energy_mwh":
                "source.total_daily_energy_mwh",

            "mae_mwh":
                "source.mae_mwh",

            "source_modified_at":
                "source.source_modified_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print(
    "Incremental Gold MERGE: COMPLETE"
)


# =====================================================
# FINAL GOLD VALIDATION
# =====================================================

gold_final = spark.table(
    GOLD_TABLE
)

gold_final_count = (
    gold_final.count()
)

gold_final_duplicate_count = (
    gold_final
    .groupBy(
        "date",
        "respondent"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


print()
print("=" * 55)
print("INCREMENTAL GOLD RESULTS")
print("=" * 55)

print(
    "Gold records:",
    gold_final_count
)

print(
    "Duplicate Gold keys:",
    gold_final_duplicate_count
)


if gold_final_duplicate_count > 0:
    raise ValueError(
        "Duplicate business keys detected "
        "in Incremental Gold."
    )


# =====================================================
# VERIFY ALL GOLD DAYS HAVE 24 HOURS
# =====================================================

bad_final_day_count = (
    gold_final
    .filter(
        F.col("hour_count") != 24
    )
    .count()
)

print(
    "Incomplete Gold days:",
    bad_final_day_count
)

if bad_final_day_count > 0:
    raise ValueError(
        "Incomplete daily KPI rows detected "
        "in final Gold table."
    )


# =====================================================
# REVISION VERIFICATION
# =====================================================

revision_check = (
    gold_final
    .filter(
        (F.col("date") == F.lit("2026-09-26").cast("date"))
        & (F.col("respondent") == "US48")
    )
    .select(
        "date",
        "respondent",
        "hour_count",
        "avg_hourly_demand_mwh",
        "peak_demand_mwh",
        "total_daily_energy_mwh",
        "mae_mwh",
        "source_modified_at"
    )
)

print()
print(
    "Gold revision check for 2026-09-26:"
)

display(
    revision_check
)


# =====================================================
# DISPLAY ALL DAILY KPIs
# =====================================================

print()
print(
    "Daily Gold KPI results:"
)

display(
    gold_final
    .orderBy("date")
)