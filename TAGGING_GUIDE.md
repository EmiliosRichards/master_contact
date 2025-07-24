# Operations Guide

This guide explains the operational features of the ETL pipeline, including data processing, tagging, testing, and auditing.

## 1. Supported File Types

The pipeline automatically finds and processes all `.csv` and `.xlsx` files located in the `etl/input_data` directory. It uses the `SalesOutreachReport` profile in `config.yaml` to map the columns from these files to the database.

## 2. Tagging Contacts

To assign a tag to a batch of contacts, open `config.yaml` and set the `tag` field.

```yaml
# config.yaml
tag: "Your-Custom-Tag-Here"
```

## 3. Running the ETL Pipeline

### Dry Run (Recommended First Step)
This simulates the process without changing the database. Use the `--quiet` flag for a clean, readable output that shows a single sample record from each file.
```bash
python etl/scripts/main.py --dry-run --quiet
```

### Live Run
This will process all supported files and load the data into your database.
```bash
python etl/scripts/main.py
```

## 4. Auditing and Data Validation

### Viewing ETL Run History
To see a history of all live runs, use the `view-etl-runs` command:
```bash
python etl/scripts/reporting.py view-etl-runs
```

### Data Enrichment and Validation
The pipeline will automatically attempt to derive a `company_name` from the `url` if it is missing. Any records that still lack a company name after this step will be saved to the `etl/invalid_records` directory for your review.