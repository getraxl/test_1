# Cocothon dbt Conversion Project

## Overview
This dbt project converts SQL Server stored procedures into a layered dbt pipeline on Snowflake.

## Layers
- **Bronze**: Raw 1:1 copies with type casting
- **Silver**: Enriched/joined models with business logic
- **Mart**: Incremental dimension tables (SCD Type 1 with soft-delete)

## Models
| Model | Layer | Materialization |
|-------|-------|-----------------|
| raw_customer_address | Bronze | Table |
| raw_territory_lu | Bronze | Table |
| raw_territory_region_lu | Bronze | Table |
| int_address__enriched | Silver | Table |
| dim_address | Mart | Incremental (Merge) |

## Usage
```bash
dbt run          # Run all models
dbt test         # Run all tests
dbt run -s mart  # Run only mart layer
```
