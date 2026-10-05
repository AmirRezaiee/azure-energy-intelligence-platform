# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# =====================================================
# CONFIGURATION
# =====================================================

CATALOG = "dbw_energy_dev"

BRONZE_TABLE = (
    f"{CATALOG}.bronze.eia_hourly_latest"
)

# IMPORTANT:
# We intentionally use a NEW table.
# The original eia_hourly table belongs to the
# earlier project version and has a different schema.
SILVER_TABLE = (
    f"{CATALOG}.silver.eia_hourly_incremental"
)


# =====================================================
# CREATE SILVER SCHEMA
# =====================================================

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver"
)


# =====================================================
# READ LATEST BRONZE
# =====================================================

bronze = spark.table(BRONZE_TABLE)

bronze_count = bronze.count()

print(
    "Bronze latest records:",
    bronze_count
)


# =====================================================
# BRONZE DATA QUALITY
# =====================================================

invalid_bronze_count = (
    bronze
    .filter(
        F.col("period").isNull()
        | F.col("respondent").isNull()
        | F.col("type").isNull()
        | F.col("value").isNull()
        | ~F.col("type").isin("D", "DF")
    )
    .count()
)

if invalid_bronze_count > 0:
    raise ValueError(
        "Invalid Bronze records detected: "
        f"{invalid_bronze_count}"
    )


# =====================================================
# CHECK BRONZE BUSINESS KEY UNIQUENESS
# =====================================================

bronze_duplicate_count = (
    bronze
    .groupBy(
        "period",
        "respondent",
        "type"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if bronze_duplicate_count > 0:
    raise ValueError(
        "Duplicate Bronze business keys detected: "
        f"{bronze_duplicate_count}"
    )

print(
    "Bronze validation: PASSED"
)


# =====================================================
# PIVOT D / DF INTO ONE HOURLY ROW
# =====================================================

silver_source = (
    bronze
    .groupBy(
        "period",
        "respondent"
    )
    .agg(
        F.max(
            F.when(
                F.col("type") == "D",
                F.col("value")
            )
        ).alias("actual_demand_mwh"),

        F.max(
            F.when(
                F.col("type") == "DF",
                F.col("value")
            )
        ).alias("forecast_demand_mwh"),

        F.max(
            F.when(
                F.col("type") == "D",
                F.col("source_modified_at")
            )
        ).alias("actual_source_modified_at"),

        F.max(
            F.when(
                F.col("type") == "DF",
                F.col("source_modified_at")
            )
        ).alias("forecast_source_modified_at")
    )
)


# =====================================================
# ADD DERIVED COLUMNS
# =====================================================

silver_source = (
    silver_source
    .withColumn(
        "period_ts",
        F.to_timestamp(
            F.col("period"),
            "yyyy-MM-dd'T'HH"
        )
    )
    .withColumn(
        "date",
        F.to_date(
            F.col("period_ts")
        )
    )
    .withColumn(
        "forecast_error_mwh",
        F.col("actual_demand_mwh")
        - F.col("forecast_demand_mwh")
    )
    .withColumn(
        "absolute_error_mwh",
        F.abs(
            F.col("forecast_error_mwh")
        )
    )
    .withColumn(
        "source_modified_at",
        F.greatest(
            F.col("actual_source_modified_at"),
            F.col("forecast_source_modified_at")
        )
    )
    .select(
        "period",
        "period_ts",
        "date",
        "respondent",
        "actual_demand_mwh",
        "forecast_demand_mwh",
        "forecast_error_mwh",
        "absolute_error_mwh",
        "source_modified_at"
    )
)


# =====================================================
# SILVER DATA QUALITY
# =====================================================

invalid_silver_count = (
    silver_source
    .filter(
        F.col("period").isNull()
        | F.col("period_ts").isNull()
        | F.col("date").isNull()
        | F.col("respondent").isNull()
        | F.col("actual_demand_mwh").isNull()
        | F.col("forecast_demand_mwh").isNull()
        | F.col("forecast_error_mwh").isNull()
        | F.col("absolute_error_mwh").isNull()
        | F.col("source_modified_at").isNull()
    )
    .count()
)

if invalid_silver_count > 0:
    raise ValueError(
        "Incomplete D/DF hourly pairs detected: "
        f"{invalid_silver_count}"
    )


# =====================================================
# CHECK SILVER BUSINESS KEY UNIQUENESS
# =====================================================

silver_duplicate_count = (
    silver_source
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
        "Duplicate Silver source keys detected: "
        f"{silver_duplicate_count}"
    )


source_count = silver_source.count()

print(
    "Silver source records:",
    source_count
)

print(
    "Silver source validation: PASSED"
)


# =====================================================
# CREATE NEW INCREMENTAL SILVER TABLE
# =====================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    period STRING,
    period_ts TIMESTAMP,
    date DATE,
    respondent STRING,
    actual_demand_mwh DOUBLE,
    forecast_demand_mwh DOUBLE,
    forecast_error_mwh DOUBLE,
    absolute_error_mwh DOUBLE,
    source_modified_at TIMESTAMP
)
USING DELTA
""")


# =====================================================
# INCREMENTAL SILVER MERGE
# =====================================================

silver_delta = DeltaTable.forName(
    spark,
    SILVER_TABLE
)

(
    silver_delta.alias("target")
    .merge(
        silver_source.alias("source"),
        """
        target.period = source.period
        AND target.respondent = source.respondent
        """
    )
    .whenMatchedUpdate(
        condition="""
        NOT (
            target.actual_demand_mwh
                <=> source.actual_demand_mwh
        )
        OR NOT (
            target.forecast_demand_mwh
                <=> source.forecast_demand_mwh
        )
        OR NOT (
            target.forecast_error_mwh
                <=> source.forecast_error_mwh
        )
        OR NOT (
            target.absolute_error_mwh
                <=> source.absolute_error_mwh
        )
        OR NOT (
            target.source_modified_at
                <=> source.source_modified_at
        )
        """,
        set={
            "period_ts":
                "source.period_ts",

            "date":
                "source.date",

            "actual_demand_mwh":
                "source.actual_demand_mwh",

            "forecast_demand_mwh":
                "source.forecast_demand_mwh",

            "forecast_error_mwh":
                "source.forecast_error_mwh",

            "absolute_error_mwh":
                "source.absolute_error_mwh",

            "source_modified_at":
                "source.source_modified_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print(
    "Incremental Silver MERGE: COMPLETE"
)


# =====================================================
# FINAL SILVER VALIDATION
# =====================================================

silver_final = spark.table(
    SILVER_TABLE
)

silver_final_count = (
    silver_final.count()
)

silver_final_duplicate_count = (
    silver_final
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


print()
print("=" * 55)
print("INCREMENTAL SILVER RESULTS")
print("=" * 55)

print(
    "Silver records:",
    silver_final_count
)

print(
    "Duplicate Silver keys:",
    silver_final_duplicate_count
)


if silver_final_duplicate_count > 0:
    raise ValueError(
        "Duplicate business keys detected "
        "in Incremental Silver."
    )


# =====================================================
# REVISION VERIFICATION
# =====================================================

revision_check = (
    silver_final
    .filter(
        (F.col("period") == "2026-09-26T00")
        & (F.col("respondent") == "US48")
    )
    .select(
        "period",
        "actual_demand_mwh",
        "forecast_demand_mwh",
        "forecast_error_mwh",
        "absolute_error_mwh",
        "source_modified_at"
    )
)

print()
print(
    "Silver revision check for 2026-09-26T00:"
)

display(revision_check)


# =====================================================
# DISPLAY SAMPLE
# =====================================================

display(
    silver_final
    .orderBy("period")
    .limit(20)
)