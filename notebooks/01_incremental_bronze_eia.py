# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# =====================================================
# CONFIGURATION
# =====================================================

CATALOG = "dbw_energy_dev"

RAW_ROOT = (
    "abfss://raw@stdata2026.dfs.core.windows.net/"
    "eia/region-data/respondent=US48/"
)

HISTORY_TABLE = f"{CATALOG}.bronze.eia_hourly_history"
LATEST_TABLE = f"{CATALOG}.bronze.eia_hourly_latest"


# =====================================================
# CREATE SCHEMA
# =====================================================

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze"
)


# =====================================================
# EIA JSON SCHEMA
# =====================================================
# IMPORTANT:
# EIA stores "value" as a JSON string.
# We therefore read it as StringType first
# and safely cast it to DOUBLE afterward.

record_schema = T.StructType([
    T.StructField("period", T.StringType(), True),
    T.StructField("respondent", T.StringType(), True),
    T.StructField("respondent-name", T.StringType(), True),
    T.StructField("type", T.StringType(), True),
    T.StructField("type-name", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
    T.StructField("value-units", T.StringType(), True)
])

payload_schema = T.StructType([
    T.StructField(
        "response",
        T.StructType([
            T.StructField(
                "data",
                T.ArrayType(record_schema),
                True
            )
        ]),
        True
    )
])


# =====================================================
# READ RAW FILES RECURSIVELY
# =====================================================

raw_files = (
    spark.read
    .format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load(RAW_ROOT)
)


# =====================================================
# FILTER VALID DAILY AND REVISION FILES
# =====================================================

valid_files = (
    raw_files
    .filter(
        F.col("path").rlike(
            r"/day=\d{2}/"
            r"(?:revisions/)?"
            r"eia_us48_\d{4}-\d{2}-\d{2}"
            r"(?:_(?:revision_[a-f0-9]{64}|"
            r"sha256_[a-f0-9]{64}))?"
            r"\.json$"
        )
    )
)

accepted_file_count = valid_files.count()

print(
    "Accepted raw files:",
    accepted_file_count
)

if accepted_file_count == 0:
    raise ValueError(
        "No valid EIA raw files were found."
    )


# =====================================================
# PARSE JSON
# =====================================================

parsed = (
    valid_files
    .withColumn(
        "payload",
        F.from_json(
            F.col("content").cast("string"),
            payload_schema
        )
    )
    .withColumn(
        "content_hash",
        F.sha2(
            F.col("content"),
            256
        )
    )
    .withColumn(
        "record",
        F.explode_outer(
            F.col("payload.response.data")
        )
    )
)


# =====================================================
# SELECT RAW RECORD FIELDS
# =====================================================

records_raw = (
    parsed
    .select(
        F.col("record.period").alias("period"),
        F.col("record.respondent").alias("respondent"),
        F.col("record.type").alias("type"),
        F.col("record.value").alias("value_raw"),
        F.col("record.value-units").alias("value_units"),
        F.col("path").alias("source_path"),
        F.col("modificationTime").alias(
            "source_modified_at"
        ),
        F.col("content_hash")
    )
)


# =====================================================
# SAFE NUMERIC CONVERSION
# =====================================================

records = (
    records_raw
    .withColumn(
        "value",
        F.col("value_raw").cast("double")
    )
    .select(
        "period",
        "respondent",
        "type",
        "value",
        "value_units",
        "source_path",
        "source_modified_at",
        "content_hash"
    )
)


# =====================================================
# DATA QUALITY CHECK 1:
# REQUIRED FIELDS
# =====================================================

invalid_required_count = (
    records
    .filter(
        F.col("period").isNull()
        | F.col("respondent").isNull()
        | F.col("type").isNull()
        | F.col("value").isNull()
        | F.col("value_units").isNull()
    )
    .count()
)

if invalid_required_count > 0:
    raise ValueError(
        "Invalid required fields detected: "
        f"{invalid_required_count}"
    )


# =====================================================
# DATA QUALITY CHECK 2:
# RESPONDENT
# =====================================================

invalid_respondent_count = (
    records
    .filter(
        F.col("respondent") != "US48"
    )
    .count()
)

if invalid_respondent_count > 0:
    raise ValueError(
        "Unexpected respondent records detected: "
        f"{invalid_respondent_count}"
    )


# =====================================================
# DATA QUALITY CHECK 3:
# TYPE
# =====================================================

invalid_type_count = (
    records
    .filter(
        ~F.col("type").isin("D", "DF")
    )
    .count()
)

if invalid_type_count > 0:
    raise ValueError(
        "Unexpected EIA record types detected: "
        f"{invalid_type_count}"
    )


# =====================================================
# DATA QUALITY CHECK 4:
# UNIT
# =====================================================

invalid_unit_count = (
    records
    .filter(
        F.col("value_units") != "megawatthours"
    )
    .count()
)

if invalid_unit_count > 0:
    raise ValueError(
        "Unexpected EIA units detected: "
        f"{invalid_unit_count}"
    )


# =====================================================
# DATA QUALITY CHECK 5:
# DUPLICATES INSIDE EACH FILE
# =====================================================

duplicate_count = (
    records
    .groupBy(
        "source_path",
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

if duplicate_count > 0:
    raise ValueError(
        "Duplicate business keys were found "
        "inside an individual raw file."
    )


print("Raw record validation: PASSED")


# =====================================================
# CREATE HISTORY TABLE
# =====================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    period STRING,
    respondent STRING,
    type STRING,
    value DOUBLE,
    value_units STRING,
    source_path STRING,
    source_modified_at TIMESTAMP,
    content_hash STRING
)
USING DELTA
""")


# =====================================================
# HISTORY MERGE
# =====================================================
# A specific record from a specific raw file and
# content hash should enter history only once.

history_delta = DeltaTable.forName(
    spark,
    HISTORY_TABLE
)

(
    history_delta.alias("target")
    .merge(
        records.alias("source"),
        """
        target.source_path = source.source_path
        AND target.content_hash = source.content_hash
        AND target.period = source.period
        AND target.respondent = source.respondent
        AND target.type = source.type
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("History MERGE: COMPLETE")


# =====================================================
# DETERMINE LATEST VERSION
# =====================================================
# For the current project, Azure file modification time
# is used as the revision ordering mechanism.
#
# Later, the ingestion pipeline will write an explicit
# ingestion/revision timestamp.

latest_window = (
    Window
    .partitionBy(
        "period",
        "respondent",
        "type"
    )
    .orderBy(
        F.col("source_modified_at").desc(),
        F.col("source_path").desc()
    )
)

latest = (
    spark.table(HISTORY_TABLE)
    .withColumn(
        "row_number",
        F.row_number().over(
            latest_window
        )
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


# =====================================================
# CREATE LATEST BRONZE TABLE
# =====================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LATEST_TABLE} (
    period STRING,
    respondent STRING,
    type STRING,
    value DOUBLE,
    value_units STRING,
    source_path STRING,
    source_modified_at TIMESTAMP,
    content_hash STRING
)
USING DELTA
""")


# =====================================================
# LATEST BRONZE MERGE
# =====================================================

latest_delta = DeltaTable.forName(
    spark,
    LATEST_TABLE
)

(
    latest_delta.alias("target")
    .merge(
        latest.alias("source"),
        """
        target.period = source.period
        AND target.respondent = source.respondent
        AND target.type = source.type
        """
    )
    .whenMatchedUpdateAll(
        condition="""
        source.source_modified_at
            > target.source_modified_at
        OR (
            source.source_modified_at
                = target.source_modified_at
            AND source.source_path
                > target.source_path
        )
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Incremental Bronze MERGE: COMPLETE")


# =====================================================
# FINAL VALIDATION
# =====================================================

history_count = (
    spark.table(HISTORY_TABLE)
    .count()
)

latest_count = (
    spark.table(LATEST_TABLE)
    .count()
)

latest_duplicate_count = (
    spark.table(LATEST_TABLE)
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


print()
print("=" * 55)
print("INCREMENTAL BRONZE RESULTS")
print("=" * 55)

print(
    "History records:",
    history_count
)

print(
    "Latest records:",
    latest_count
)

print(
    "Duplicate latest keys:",
    latest_duplicate_count
)


if latest_duplicate_count > 0:
    raise ValueError(
        "Duplicate business keys detected "
        "in the latest Bronze table."
    )


# =====================================================
# REVISION CHECK FOR SEPTEMBER 26
# =====================================================

revision_check = (
    spark.table(LATEST_TABLE)
    .filter(
        (F.col("period") == "2026-09-26T00")
        & (F.col("respondent") == "US48")
    )
    .select(
        "period",
        "type",
        "value",
        "source_modified_at",
        "source_path"
    )
    .orderBy("type")
)

print()
print(
    "Revision check for 2026-09-26T00:"
)

display(revision_check)


# =====================================================
# DISPLAY SAMPLE
# =====================================================

display(
    spark.table(LATEST_TABLE)
    .orderBy(
        "period",
        "type"
    )
    .limit(20)
)

