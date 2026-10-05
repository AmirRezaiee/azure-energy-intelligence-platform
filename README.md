# Azure Energy Intelligence Platform

An end-to-end Azure Data Engineering project that ingests U.S. electricity demand and demand forecast data from the U.S. Energy Information Administration (EIA), stores raw data in Azure Data Lake Storage Gen2, and transforms it through an incremental Bronze-Silver-Gold architecture in Azure Databricks.

The project is designed around production-oriented data engineering concepts including incremental ingestion, historical revision handling, idempotent processing, Delta Lake MERGE operations, data quality validation, medallion architecture, workflow orchestration, and version-controlled Databricks deployment configuration.

---

## Project Overview

Electricity demand data can change after its initial publication. Historical values may be revised by the source system, which creates an important engineering challenge:

> How can a data pipeline preserve historical versions while ensuring downstream analytics always use the latest available data?

This project addresses that problem with a revision-aware architecture.

The pipeline:

1. Extracts hourly U.S. electricity demand and demand forecast data from the EIA API.
2. Stores complete daily raw JSON files in Azure Data Lake Storage Gen2.
3. Detects historical revisions without overwriting the original raw files.
4. Preserves multiple raw versions when EIA data changes.
5. Loads raw records into revision-aware Bronze Delta tables.
6. Produces clean hourly demand and forecast records in Silver.
7. Generates daily analytical KPIs in Gold.
8. Uses idempotent MERGE logic so the pipeline can be safely rerun.
9. Orchestrates Bronze, Silver, and Gold through a dependency-aware Databricks Job.
10. Manages the Databricks workflow definition through Databricks Asset Bundles and Git.

---

## Architecture

```text
U.S. Energy Information Administration (EIA)
                    |
                    | REST API
                    v
           Python Ingestion Pipeline
                    |
                    | Validation
                    | Pagination
                    | Retry / Backoff
                    | Watermarking
                    | Revision Detection
                    v
         Azure Data Lake Storage Gen2
                    |
                    | Raw JSON
                    | Partitioned by date
                    | Canonical + revision files
                    v
              Azure Databricks
                    |
              Bronze History
                    |
                    v
              Bronze Latest
                    |
                    v
                 Silver
          Hourly Demand + Forecast
                    |
                    v
                  Gold
             Daily Energy KPIs
```

The Databricks transformation workflow is orchestrated as:

```text
Bronze
   |
   | ALL_SUCCESS
   v
Silver
   |
   | ALL_SUCCESS
   v
Gold
```

The workflow configuration is version-controlled through Databricks Asset Bundles.

---

### Technology Stack

- Python 3.11
- REST APIs
- U.S. Energy Information Administration (EIA) API
- Azure Data Lake Storage Gen2
- Azure Identity
- Azure Storage SDK
- Azure Databricks
- Databricks Jobs / Workflows
- Databricks Asset Bundles
- Apache Spark / PySpark
- Delta Lake
- Databricks Unity Catalog
- Git
- GitHub

---

## Data Source

The project uses hourly electricity balancing authority data from the U.S. Energy Information Administration.

Current pipeline configuration processes:

- Respondent: `US48`
- `D`: Actual electricity demand
- `DF`: Demand forecast
- Frequency: Hourly
- Unit: Megawatthours

A complete day contains:

```text
24 actual demand records
+
24 demand forecast records
=
48 raw records per day
```

---

## Raw Data Lake Design

Raw EIA data is stored in Azure Data Lake Storage Gen2 using date-based partitions.

Example canonical path:

```text
eia/
└── region-data/
    └── respondent=US48/
        └── year=2026/
            └── month=09/
                └── day=26/
                    └── eia_us48_2026-09-26.json
```

When EIA changes previously published values, the original canonical file is preserved and the revised version is stored separately.

Example:

```text
day=26/
├── eia_us48_2026-09-26.json
└── revisions/
    └── eia_us48_2026-09-26_revision_<sha256>.json
```

This provides an immutable raw history instead of silently overwriting previously ingested source data.

---

## Revision-Aware Ingestion

One of the main engineering features of this project is historical revision detection.

The ingestion pipeline compares newly retrieved business records with the existing canonical data.

### If the records are unchanged

The upload is skipped.

```text
EIA API
   |
   v
Existing canonical file
   |
   v
Same business records
   |
   v
SKIP
```

### If historical values changed

The original canonical file remains untouched and the new version is stored under the `revisions/` directory.

```text
EIA API
   |
   v
Existing canonical file
   |
   v
Different business values
   |
   +--> Preserve canonical
   |
   +--> Store revision
```

Revision identifiers are generated from SHA-256 hashes of normalized business records.

This allows the pipeline to distinguish meaningful source-data changes from irrelevant JSON formatting or metadata differences.

---

## Incremental Processing

The ingestion process maintains a local watermark representing the latest successfully stored hour.

The pipeline intentionally revisits the previous day so that recently revised EIA values can be detected.

The watermark only advances after successful storage processing.

This helps prevent data loss when:

- API requests fail
- Azure uploads fail
- data quality validation fails
- historical values are revised

Only complete daily datasets are promoted by the current pipeline.

---

## Medallion Architecture

### Bronze Layer

Notebook:

```text
notebooks/01_incremental_bronze_eia.py
```

The Bronze layer recursively reads accepted canonical and revision JSON files from ADLS Gen2.

It maintains two Delta tables:

```text
bronze.eia_hourly_history
bronze.eia_hourly_latest
```

#### Bronze History

The History table preserves records from different raw file versions.

Its purpose is lineage and historical traceability.

A record is uniquely associated with:

```text
source_path
content_hash
period
respondent
type
```

This makes reruns idempotent while preserving revised source versions.

#### Bronze Latest

The Latest table contains the current version of each business record.

Business key:

```text
(period, respondent, type)
```

The current implementation orders competing versions using source file modification time with source path as a deterministic tie-breaker.

This allows downstream layers to consume the latest known EIA value without losing historical versions from Bronze History.

---

### Silver Layer

Notebook:

```text
notebooks/02_incremental_silver_eia.py
```

Silver transforms Bronze demand records into one analytical row per hour.

Bronze contains separate records for:

```text
D  = Actual Demand
DF = Demand Forecast
```

Silver combines them into:

```text
period
period_ts
date
respondent
actual_demand_mwh
forecast_demand_mwh
forecast_error_mwh
absolute_error_mwh
source_modified_at
```

Forecast error is calculated as:

```text
actual demand - forecast demand
```

Absolute error is also calculated for downstream forecast accuracy metrics.

Silver business key:

```text
(period, respondent)
```

Delta Lake MERGE operations update historical hourly rows when revised Bronze values propagate downstream.

---

### Gold Layer

Notebook:

```text
notebooks/03_incremental_gold_eia.py
```

Gold aggregates hourly Silver data into daily analytical KPIs.

Gold grain:

```text
(date, respondent)
```

Metrics include:

| Metric | Description |
|---|---|
| `hour_count` | Number of hourly records in the day |
| `avg_hourly_demand_mwh` | Average hourly electricity demand |
| `peak_demand_mwh` | Maximum hourly demand |
| `total_daily_energy_mwh` | Sum of hourly demand |
| `mae_mwh` | Mean Absolute Error between actual and forecast demand |
| `source_modified_at` | Latest upstream source modification used by the KPI |

The Gold pipeline validates that each promoted day contains exactly 24 hourly records.

If an upstream historical revision changes a Silver record, the corresponding Gold daily KPI is recalculated and updated through Delta MERGE.

---

## Data Quality Controls

Data quality checks are implemented across the pipeline.

### Raw / Ingestion

The ingestion layer validates:

- expected respondent
- expected record types
- complete daily coverage
- expected hourly records
- API response structure
- successful ADLS persistence

### Bronze

Bronze validates:

- required fields
- respondent
- EIA record type
- measurement units
- duplicate business keys inside individual source files
- uniqueness of the Bronze Latest business key

### Silver

Silver validates:

- required fields
- complete actual/forecast pairs
- valid timestamps
- uniqueness of `(period, respondent)`

### Gold

Gold validates:

- required KPI values
- uniqueness of `(date, respondent)`
- exactly 24 hourly records per daily KPI

Pipeline execution stops when critical data quality requirements fail.

---

## Idempotency

The pipeline is designed to be safely rerunnable.

Repeated execution with unchanged source data does not create duplicate analytical records.

Idempotency is implemented through:

- deterministic raw file handling
- SHA-256 content identification
- business-key comparison
- Bronze History MERGE
- Bronze Latest MERGE
- Silver MERGE
- Gold MERGE
- duplicate-key validation

This behavior was tested by rerunning the Bronze, Silver, and Gold transformations without changing the source data and verifying stable record counts and zero duplicate business keys.

---

## Example Pipeline Validation

During development, the pipeline processed four complete days of hourly EIA data.

The validated incremental layers contained:

```text
Bronze History : 288 records
Bronze Latest  : 192 records
Silver         : 96 hourly records
Gold           : 4 daily records
```

The difference between Bronze History and Bronze Latest demonstrates that historical source versions were preserved while downstream processing continued to use one latest value per business key.

A historical EIA revision for September 26 was successfully propagated through:

```text
Raw Revision
     |
     v
Bronze History
     |
     v
Bronze Latest
     |
     v
Silver
     |
     v
Gold
```

This validated both revision propagation and end-to-end idempotency.

---

## Example Revision Result

A historical revision for `2026-09-26T00` was detected and propagated through the pipeline.

The revised Bronze values included:

```text
Actual Demand   : 539,672 MWh
Demand Forecast : 533,334 MWh
```

Silver calculated:

```text
Forecast Error  : 6,338 MWh
Absolute Error  : 6,338 MWh
```

The revised daily Gold KPI for September 26 included:

```text
Hour Count           : 24
Average Hourly Demand : 458,994.33 MWh
Peak Demand           : 539,672 MWh
Total Daily Energy    : 11,015,864 MWh
Mean Absolute Error   : 3,130.375 MWh
```

This demonstrates that a historical source revision can propagate from raw storage through Bronze, Silver, and Gold without creating duplicate analytical records.

---

## Repository Structure

```text
azure-energy-intelligence-platform/
|
├── databricks.yml
|
├── databricks/
|   └── resources/
|       └── energy_eia_medallion_job.yml
|
├── notebooks/
|   ├── 01_incremental_bronze_eia.py
|   ├── 02_incremental_silver_eia.py
|   └── 03_incremental_gold_eia.py
|
├── src/
|   └── ingestion/
|       ├── compare_adls_files.py
|       ├── eia_api_client.py
|       ├── eia_to_adls_pipeline.py
|       └── upload_eia_revision.py
|
├── data/
|   ├── raw/
|   └── state/
|
├── .gitignore
├── requirements.txt
└── README.md
```

Local raw data, pipeline state, virtual environments, and secrets are excluded from Git.

The Databricks workflow configuration is stored in:

```text
databricks.yml
databricks/resources/energy_eia_medallion_job.yml
```

This keeps workflow configuration version-controlled alongside the transformation code.

---

## Python Dependencies

Current Python dependencies:

```text
requests
azure-identity
azure-storage-file-datalake
```

Install them with:

```bash
pip install -r requirements.txt
```

---

## Authentication and Secrets

The EIA API key is never hardcoded in the repository.

It is provided through an environment variable:

```text
EIA_API_KEY
```

Azure authentication uses Azure CLI credentials through the Azure Identity SDK during local development.

Secrets and local state files are excluded from Git.

Example PowerShell environment setup:

```powershell
$secureKey = Read-Host "Enter your EIA API Key" -AsSecureString
$env:EIA_API_KEY = [System.Net.NetworkCredential]::new("", $secureKey).Password
Remove-Variable secureKey
```

This keeps the API key out of source code and shell history.

Databricks CLI authentication is used for Asset Bundle validation and deployment. Databricks access credentials are not stored in the repository.

---

## Local Setup

Clone the repository:

```bash
git clone https://github.com/AmirRezaiee/azure-energy-intelligence-platform.git
```

Enter the project directory:

```bash
cd azure-energy-intelligence-platform
```

Create a Python virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Authenticate with Azure CLI before running the ADLS ingestion pipeline.

The local implementation uses Azure CLI credentials through the Azure Identity SDK.

---

## Running the Ingestion Pipeline

Set the EIA API key as an environment variable before execution.

Run the ingestion pipeline from the repository root:

```bash
python src/ingestion/eia_to_adls_pipeline.py
```

The current implementation requires an explicit complete end hour through the pipeline configuration and only promotes complete daily datasets.

The ingestion pipeline handles:

```text
EIA API
   |
   +--> Pagination
   |
   +--> Retry / Backoff
   |
   +--> Data Quality Validation
   |
   +--> Daily File Generation
   |
   +--> ADLS Upload
   |
   +--> SHA-256 Verification
   |
   +--> Revision Detection
   |
   +--> Watermark Advancement
```

---

## Databricks Workflow Orchestration

The medallion transformation pipeline is orchestrated using an Azure Databricks Job named:

```text
energy-eia-medallion-pipeline
```

The workflow executes the three incremental transformation notebooks in dependency order:

```text
01_incremental_bronze_eia
          |
          v
02_incremental_silver_eia
          |
          v
03_incremental_gold_eia
```

The corresponding repository notebooks are:

```text
notebooks/01_incremental_bronze_eia.py
notebooks/02_incremental_silver_eia.py
notebooks/03_incremental_gold_eia.py
```

The Databricks Job contains three tasks:

```text
bronze_eia_incremental
        |
        | ALL_SUCCESS
        v
silver_eia_incremental
        |
        | ALL_SUCCESS
        v
gold_eia_daily_kpi
```

Each downstream task runs only after its upstream dependency succeeds.

The workflow therefore enforces the transformation sequence:

```text
ADLS Raw
   |
   v
Bronze History / Latest
   |
   v
Silver Hourly Analytics
   |
   v
Gold Daily KPIs
```

The workflow was successfully validated with an end-to-end Databricks Job run in which all three tasks completed successfully.

The current workflow uses the project's single-node Databricks compute configuration for controlled development and testing.

The EIA API ingestion process remains a separate local Python pipeline. The Databricks Job currently orchestrates the Bronze, Silver, and Gold transformation layers after raw data has been stored in ADLS Gen2.

---

## Databricks Job-as-Code

The Databricks transformation workflow is managed as code using Databricks Asset Bundles.

The root Bundle configuration is:

```text
databricks.yml
```

The Job resource definition is stored in:

```text
databricks/resources/energy_eia_medallion_job.yml
```

The Bundle defines:

- the Databricks workspace target
- the existing development compute configuration
- the `energy-eia-medallion-pipeline` Job
- Bronze, Silver, and Gold tasks
- task dependencies
- `ALL_SUCCESS` execution requirements
- queue configuration
- concurrency configuration
- workflow performance configuration

The Job resource follows this dependency graph:

```text
bronze_eia_incremental
        |
        v
silver_eia_incremental
        |
        v
gold_eia_daily_kpi
```

The Bundle configuration was validated using the Databricks CLI before deployment.

```bash
databricks bundle validate -t dev
```

The existing Databricks Job was bound to the Bundle rather than creating a duplicate Job.

After binding, the Bundle was deployed successfully and updated the existing workflow resource.

```text
Resources:
0 created
1 changed
0 deleted
```

The deployed Job is now Bundle-managed, making Git the version-controlled source for workflow configuration changes.

This improves:

- reproducibility
- change tracking
- deployment consistency
- workflow configuration review
- future CI/CD integration

The current Job still references the existing Databricks workspace notebooks and development compute. Future iterations can further decouple deployment from user-specific workspace resources.

---

## Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

- Cloud data ingestion
- REST API integration
- Azure Data Lake Storage Gen2
- Incremental data pipelines
- Watermark-based processing
- Historical revision handling
- Immutable raw data design
- Medallion architecture
- Apache Spark
- PySpark transformations
- Delta Lake MERGE
- Data quality validation
- Idempotent pipelines
- Business-key design
- Data lineage
- Forecast accuracy analytics
- Azure authentication
- Databricks workflow orchestration
- Task dependency management
- Databricks Asset Bundles
- Workflow configuration as code
- Git-based source control

---

## Current Scope

The current implementation focuses on batch-oriented processing of EIA US48 hourly demand and forecast data.

The project currently uses:

- local Python execution for API ingestion
- Azure CLI authentication for local Azure access
- ADLS Gen2 for raw storage
- Azure Databricks for transformation
- Unity Catalog for Bronze, Silver, and Gold Delta tables
- Delta Lake for incremental analytical processing
- Databricks Jobs for Bronze-Silver-Gold orchestration
- Databricks Asset Bundles for version-controlled workflow configuration
- Git and GitHub for source control

The transformation workflow is currently manually triggered. Automated scheduling is intentionally left as a future enhancement.

---

## Design Decisions

### Preserve Raw History

Historical EIA revisions are stored as additional raw files rather than overwriting the original canonical file.

This improves traceability and allows historical source changes to be investigated.

### Separate History from Latest

Bronze separates:

```text
Historical lineage
        |
        v
eia_hourly_history
```

from:

```text
Current analytical state
        |
        v
eia_hourly_latest
```

This allows downstream transformations to remain simple while retaining historical versions for auditing and debugging.

### Fail on Incomplete Days

The current pipeline only promotes complete daily datasets.

Gold therefore requires exactly 24 hourly Silver records for each daily KPI.

### Idempotent MERGE Operations

Bronze, Silver, and Gold use Delta Lake MERGE operations so rerunning the same data does not create duplicate business records.

### Dependency-Aware Workflow Execution

Silver runs only after Bronze succeeds, and Gold runs only after Silver succeeds.

This prevents downstream layers from processing when an upstream transformation has failed.

### Workflow Configuration as Code

The Databricks Job definition is stored in the repository using Databricks Asset Bundles.

This reduces configuration drift between source control and the deployed workflow and creates a foundation for future CI/CD deployment.

---

## Future Enhancements

Planned improvements include:

- cloud-hosted pipeline checkpointing
- managed identity authentication
- automated workflow scheduling
- configurable rolling revision windows
- explicit ingestion and revision timestamps
- automated unit and integration testing
- pipeline monitoring and alerting
- CI/CD with GitHub Actions
- broader infrastructure as code for Azure resources
- bundle-managed deployment of transformation notebooks
- environment-specific Databricks deployment targets
- analytical dashboarding
- support for additional EIA respondents
- support for additional EIA datasets

---

## Project Goal

The goal of this project is to build a realistic cloud data engineering platform rather than a simple API-to-file demonstration.

The architecture emphasizes:

- reliability
- reproducibility
- historical traceability
- incremental processing
- data quality
- idempotency
- orchestration
- version-controlled deployment configuration
- analytics-ready modeling

The project is intended to demonstrate practical Azure Data Engineering skills using Python, ADLS Gen2, Azure Databricks, Apache Spark, Delta Lake, Databricks Jobs, Databricks Asset Bundles, and Git.