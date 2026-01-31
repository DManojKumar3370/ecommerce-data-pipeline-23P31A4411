\# Architecture



\## Overview



This project implements an end-to-end ETL pipeline for an e-commerce analytics platform. It covers data generation, ingestion, transformation, warehousing, BI, orchestration, monitoring, and testing.



\## Components



\- \*\*Raw data\*\*: Synthetic CSVs in `data/raw/` (`customers.csv`, `products.csv`, `transactions.csv`, `transaction\_items.csv`, `generation\_metadata.json`).

\- \*\*Staging layer\*\*: PostgreSQL schema `staging` created via `sql/ddl/create\_staging\_schema.sql`.

\- \*\*Warehouse layer\*\*: PostgreSQL schema `warehouse` created via `sql/ddl/create\_warehouse\_schema.sql` with `dim\_customers`, `dim\_products`, `fact\_sales`, and aggregate tables.

\- \*\*ETL scripts\*\*:

&nbsp; - Ingestion scripts under `scripts/ingestion/` load raw CSVs into staging.

&nbsp; - `scripts/transformation/staging\_to\_production.py` moves data from staging to warehouse with SCD Type 2 for dimensions.

\- \*\*Quality \& monitoring\*\*:

&nbsp; - `scripts/quality\_checks/validate\_data.py` plus `sql/queries/data\_quality\_checks.sql` for data validation.

&nbsp; - `scripts/monitoring/pipeline\_monitor.py` and `sql/queries/monitoring\_queries.sql` for health checks.

\- \*\*Orchestration\*\*:

&nbsp; - `scripts/pipeline\_orchestrator.py` coordinates ingestion, transformation, and quality checks.

&nbsp; - `scripts/scheduler.py` triggers the pipeline run.

\- \*\*BI layer\*\*:

&nbsp; - Tableau dashboards under `dashboards/tableau/` using the warehouse schema.

\- \*\*CI \& tests\*\*:

&nbsp; - GitHub Actions workflow at `.github/workflows/ci.yml`.

&nbsp; - Tests in `tests/` executed with pytest.



\## Data Flow



1\. Raw CSVs are generated and stored in `data/raw/`.

2\. Ingestion scripts load raw data into `staging` tables.

3\. `staging\_to\_production.py` transforms staging tables into SCD Type 2 dimensions and the `fact\_sales` fact table.

4\. Analytical SQL queries and BI dashboards read from the `warehouse` schema.

5\. Monitoring scripts and queries validate pipeline health and data freshness.



\## Technologies



\- PostgreSQL for staging and warehouse.

\- Python (pandas, psycopg2) for ETL and checks.

\- Pytest for automated testing.

\- Docker and docker-compose for local deployment.

\- GitHub Actions for CI.

\- Tableau / Power BI for analytics dashboards.



