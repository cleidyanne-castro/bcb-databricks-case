# BCB Interest and Inflation Databricks Pipeline

## Overview

This project implements a Databricks Medallion Architecture pipeline using two public time series from Banco Central do Brasil:

- SELIC daily rate — SGS series 11
- IPCA monthly inflation — SGS series 433

The pipeline extracts the data locally, uploads the raw JSON files to a Unity Catalog Volume, and processes them through Bronze, Silver, and Gold Delta tables.

## Architecture


BCB API
   |
   v
Local Python extraction
   |
   v
Unity Catalog Volume
   |
   v
Bronze Delta tables
   |
   v
Silver Delta tables
   |
   v
Monthly Gold analytical table

## Data sources
| Dataset | SGS series | Frequency | Period                   |
| ------- | ---------: | --------- | ------------------------ |
| SELIC   |         11 | Daily     | 2020-01-01 to 2024-12-31 |
| IPCA    |        433 | Monthly   | 2020-01-01 to 2024-12-31 |

## Repository sctructure
extract/      Local BCB API extraction
src/          Modular PySpark pipeline code
notebooks/    Databricks execution notebooks
resources/    Databricks Job definition
evidence/     Pipeline and idempotency evidence

## Local extraction
python3 -m venv .venv
source .venv/bin/activate

## Install dependencies
pip install -r requirements.txt

## Run the extractions
python -m extract.extract_bcb

## Then the script generates
data/raw/selic.json
data/raw/ipca.json

## The extraction should include:

request timeout;
retries;
exponential backoff;
HTTP error handling;
invalid JSON validation;
empty payload validation;
source schema validation;
atomic file replacement.

## Databricks setup
Catalog: beanalytic_case

Schemas:
- landing
- bronze
- silver
- gold

Volume:
beanalytic_case.landing.raw_files

Run the notebook:

```text
notebooks/00_setup
```

Then upload the generated JSON files to:

```text
/Volumes/beanalytic_case/landing/raw_files/selic/
/Volumes/beanalytic_case/landing/raw_files/ipca/
```

## Bronze Layer

The Bronze layer uses Databricks Auto Loader with `availableNow=True`.

Tables:

```text
beanalytic_case.bronze.selic
beanalytic_case.bronze.ipca
```

Raw fields are preserved as strings together with:

- series name;
- source file;
- source path;
- ingestion timestamp;
- ingestion date.

Separate checkpoints are used for SELIC and IPCA.

## Silver Layer

Tables:

```text
beanalytic_case.silver.selic
beanalytic_case.silver.ipca
```

Business keys:

```text
SELIC: reference_date
IPCA: reference_month
```

The Silver layer performs:

- date parsing;
- numeric casting;
- null validation;
- deduplication;
- range validation;
- idempotent Delta MERGE.

## Gold Layer

Table:

```text
beanalytic_case.gold.interest_inflation_monthly
```

Grain:

> One record per calendar month for which both SELIC and IPCA data are available.

Metrics:

- average daily SELIC rate in the month;
- effective monthly SELIC rate;
- monthly IPCA;
- monthly real interest rate;
- accumulated SELIC over 12 months;
- accumulated IPCA over 12 months;
- accumulated real interest rate over 12 months.

## Financial Calculations

### Effective Monthly SELIC

Daily SELIC rates are compounded within each month.

### Monthly Real Interest Rate

The Fisher relation is used:

```text
((1 + monthly SELIC) / (1 + monthly IPCA)) - 1
```

### Twelve-Month Accumulated Rates

Monthly rates are compounded across a rolling 12-month window.

The first 11 months contain null accumulated values because a complete 12-month window is not yet available.

## Data Quality

The pipeline fails explicitly when any of the following checks are violated:

1. Empty dataset
2. Null business keys or rates
3. Duplicate business keys
4. Rate outside expected boundaries
5. Month without SELIC observations
6. Duplicate Gold month

## Workflow

The pipeline is orchestrated using a Databricks Job:

```text
bronze → silver → gold
```

All tasks use serverless compute and execute only when the preceding task succeeds.

The Job definition is available at:

```text
resources/bcb_job.json
```

## Idempotency

The complete workflow was executed twice without changes to the raw inputs.

Expected row counts after both runs:

| Table | Rows | Distinct business keys |
|---|---:|---:|
| Bronze SELIC | 1255 | 1255 |
| Bronze IPCA | 60 | 60 |
| Silver SELIC | 1255 | 1255 |
| Silver IPCA | 60 | 60 |
| Gold monthly | 60 | 60 |

Bronze incremental behavior is controlled through Auto Loader checkpoints.

Silver and Gold use Delta MERGE operations based on declared business keys.

## Execution Evidence

### Workflow Runs
(evidence/workflow_run_1_and_2.png)

### Second Successful Run
(evidence/workflow_run_2.png)

### Idempotency Validation
(evidence/idempotency_validation.png)

### Gold Sample
(evidence/gold_sample.png)

## Backfill Strategy

A backfill can be performed by:

1. Running the local extractor for the desired date range.
2. Generating a new uniquely named raw file.
3. Uploading the file to the corresponding Volume directory.
4. Running the Databricks Job.

Auto Loader identifies the new file.

Silver MERGE inserts new business dates and updates existing dates without creating duplicate keys.

Gold is recalculated from the complete Silver tables.

## Technical Decisions

- Local extraction was used because Databricks Free Edition restricts outbound access to the BCB API.
- Unity Catalog Volumes are used for raw files and checkpoints.
- Raw values remain strings in Bronze.
- Explicit schemas avoid unreliable inference.
- Auto Loader provides incremental ingestion.
- Delta MERGE provides idempotent Silver and Gold loads.
- Fully qualified table names prevent collisions with other projects.
- The 12-month calculation uses a global chronological window because this dataset contains a single national time series.

## Known Limitations

- Raw file upload is manual.
- The extraction date range is currently configured in the Python script.
- The current Gold dataset is small and does not require physical table partitioning.