# Assignment 2 - Airbnb Analytics

## Project Overview

This project is designed to analyze Airbnb data using dbt (Data Build Tool). It transforms raw Airbnb data into meaningful insights by leveraging dbt models, staging layers, and marts. The project focuses on host activity, listing performance, pricing trends, and customer behavior.

## Table of Contents

- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Staging Models](#staging-models)
- [Intermediate Models](#intermediate-models)
- [Marts](#marts)
- [Snapshots](#snapshots)
- [Tests](#tests)
- [Key Transformations](#key-transformations)
- [How to Run the Project](#how-to-run-the-project)

## Tech Stack

This project utilizes the following technologies:

- **dbt Core**: For transforming and modeling data
- **Snowflake**: As the data warehouse
- **Git**: For version control and collaboration
- **Dagster**: For workflow orchestration

## Project Structure

The directory structure of this dbt project is as follows:

```
assignment2
├── macros
│   ├── analyze_snowflake_queries
│   ├── audit_post_hook
│   ├── audit_pre_hook
│   ├── create_audit_log
│   ├── get_column_names
│   ├── print_audit_logs
├── models
│   ├── staging
│   │   ├── stg_raw_airbnb_data__hosts
│   │   ├── stg_raw_airbnb_data__listings
│   │   ├── stg_raw_airbnb_data__reviews
│   ├── intermediate
│   │   ├── average_nightly_rate
│   │   ├── host_earnings
│   │   ├── listing_revenue
│   ├── marts
│   │   ├── dimensions
│   │   │   ├── host_activity_analysis
│   │   │   ├── listing_popularity
│   │   │   ├── repeat_customers
│   │   ├── facts
│   │   │   ├── pricing_trends
│   │   │   ├── room_type_revenue
│   │   │   ├── room_type_popularity
├── snapshots
│   ├── dim_host_snapshot
├── tests
│   ├── data_quality
│   ├── schema_tests
```

## Data Sources

The raw Airbnb data is sourced from Snowflake and includes the following tables:

- `raw_hosts`
- `raw_listings`
- `raw_reviews`

## Staging Models

Staging models clean and transform raw data into a structured format:

- `stg_raw_airbnb_data__hosts`
- `stg_raw_airbnb_data__listings`
- `stg_raw_airbnb_data__reviews`

## Intermediate Models

Intermediate models perform aggregations and calculations:

- `average_nightly_rate`
- `host_earnings`
- `listing_revenue`

## Marts

Marts contain final transformed data for business insights:

- **Dimensions**:
  - `host_activity_analysis`
  - `listing_popularity`
  - `repeat_customers`
- **Facts**:
  - `pricing_trends`
  - `room_type_revenue`
  - `room_type_popularity`

## Snapshots

Snapshot models capture historical changes in data over time:

- `dim_host_snapshot`

## Tests

Testing ensures data integrity and correctness:

- **Data Quality Tests**: Validate transformations
- **Schema Tests**: Check for constraints like unique keys and null values

## Key Transformations

### ⚡ Ephemeral Models

- Implemented in intermediate to create temporary tables that do not persist in the database, improving query efficiency.

### 🔄 Incremental Loading

- Implemented in `stg_raw_airbnb_data__listings` and `pricing_trends` to optimize performance and reduce processing time.

### 📊 Window Functions & Aggregations

- Used for analyzing revenue trends, customer behavior, and listing performance.

### 🔍 Normalization

- Cleaning and structuring host and listing details to maintain data integrity.

### DAG & Data Lineage

- The project follows a structured DAG (Directed Acyclic Graph), ensuring dependencies are well-defined, and each model builds upon previous transformations.
- The DAG optimizes query performance by clustering Snowflake ❄️ tables efficiently.

### Performance Optimization

#### 📌 Snowflake Clustering

- Applied to large tables to improve query performance.

#### 🕒 Pre & Post Hooks

- Implemented using macros to log model execution times.
- **Pre-hooks** insert a log entry before execution, and **post-hooks** update the log with completion time to analyze execution performance and optimize models.

#### 📊 Snowflake Query Monitoring

- Implemented through the `analyze_snowflake_queries` macro, which retrieves query history and performance metrics such as execution time, partition scanning efficiency, and resource consumption.
- Helps in identifying and optimizing slow or resource-intensive queries.

## How to Run the Project

1. Clone the repository: `git clone <repo_url>`
2. Navigate to the project folder: `cd assignment2`
3. Set up your dbt profile with Snowflake credentials
4. Run dbt models: `dbt run`
5. Execute tests: `dbt test`
