# Master Contact ETL Pipeline: Architectural Plan

## 1. Overview & Goals

The objective is to design and build a robust, configuration-driven ETL pipeline. This pipeline will ingest contact data from various CSV files, intelligently clean and transform the data, identify and merge duplicates, and load the curated information into a central PostgreSQL database.

The key design goals are:
*   **Accuracy:** Ensure high data quality through intelligent deduplication and conflict resolution.
*   **Flexibility:** Easily adapt to new data sources with different column names via a mapping configuration.
*   **Maintainability:** A modular and well-documented codebase that is easy to understand and extend.
*   **Security:** Manage sensitive credentials securely, separate from the application code.

## 2. Proposed Architecture & Data Flow

The proposed architecture follows a standard Extract, Transform, Load pattern with distinct, modular stages.

```mermaid
graph TD
    subgraph Ingestion
        A[Source CSV Files] --> B{ETL Orchestrator};
    end

    subgraph Configuration
        C[config.yaml] --> B;
        D[.env File] --> B;
    end

    subgraph ETL Process
        B --> E[Extract: Read CSV];
        E --> F[Transform: Apply Column Mapping];
        F --> G[Transform: Clean & Standardize Data];
        G --> H{Deduplication Stage};
        H --> I[1. Fuzzy Match Company Names];
        H --> J[2. Check for Unique Phone Numbers];
        I --> K[Merge/Flag Duplicates];
        J --> K;
        K --> L[Load: Insert/Update Master DB];
    end

    subgraph Database
        L --> M[PostgreSQL: contacts table];
    end

    subgraph Monitoring
        L --> N[Log File];
        K --> O[Manual Review File];
    end
```

## 3. Key Components & Process Steps

#### A. Configuration (The "Brain")

*   **`.env` File:** For all sensitive information. This file should be included in `.gitignore`.
    *   `DATABASE_URL="postgresql://user:pass@host:port/dbname"`
*   **`config.yaml` File:** For all non-sensitive pipeline configuration. This makes the pipeline highly adaptable without changing code.
    ```yaml
    # config.yaml
    source_directory: "etl/input_data"
    processed_directory: "etl/processed_data"
    review_directory: "etl/review"
    log_file: "etl/logs/pipeline.log"
    deduplication:
      company_name_threshold: 90 # Fuzzy match similarity threshold
    
    # --- Column Mapping ---
    # Maps source CSV headers to target database columns
    column_mapping:
      # Standard Mappings
      "Company": "company_name"
      "Company Name": "company_name"
      "Website": "url"
      "URL": "url"
      "Company Phone": "phone_number"
      "Phone": "phone_number"
      "Industry": "industry"
      
      # Mappings for boolean flags or specific values
      "is_b2b": "is_b2b"
      
      # Columns to be aggregated into the JSONB field
      # 'additional_info' is the keyword for the JSONB catch-all
      "additional_info":
        - "# Employees"
        - "sales_pitch"
        - "match_reasoning"
        - "serves_1000"
    ```

#### B. Extraction

A script will scan the `source_directory` specified in `config.yaml` for new CSV files. It will read each file into a pandas DataFrame.

#### C. Transformation

This is the core logic stage, performed in memory using pandas.

1.  **Column Mapping:** The `column_mapping` from `config.yaml` is applied. Any column in the CSV that is not a key in the mapping and not listed under `additional_info` will be dropped. Columns listed under `additional_info` will be consolidated into a new column containing a JSON object.
2.  **Data Cleaning:**
    *   Standardize phone numbers (e.g., remove `()`, `-`, ` `).
    *   Trim whitespace from all string fields.
    *   Handle missing values (`NaN`) appropriately.
3.  **Deduplication:**
    *   **Fetch Existing Data:** Load `company_name` and `phone_number` from the `contacts` table into a DataFrame for comparison.
    *   **Fuzzy Match Company Names:** For each new contact, use `rapidfuzz` to check for existing companies with a similarity score above the `company_name_threshold`. Potential duplicates will be flagged. For this initial design, we will log them for manual review and skip insertion.
    *   **Check Phone Number Uniqueness:** Filter out any new contacts whose standardized phone number already exists in the database. This prevents errors from the database's unique constraint.

#### D. Loading

1.  **Database Constraint:** First, we will apply the unique constraint to the database:
    ```sql
    ALTER TABLE contacts ADD CONSTRAINT unique_phone_number UNIQUE (phone_number);
    ```
2.  **Insert/Update:** The cleaned, transformed, and deduplicated DataFrame is loaded into the `contacts` table. The initial implementation will use `to_sql(if_exists='append')`.

#### E. Logging & Monitoring

*   **`pipeline.log`:** A general log file will capture the pipeline's execution status, including files processed, rows added, and any errors encountered.
*   **`duplicates_for_review.csv`:** Any potential duplicates identified by the fuzzy matching logic will be saved to this separate file for manual review.

## 4. Proposed Project Structure

To keep the project organized and scalable, I recommend the following directory structure:

```
master_contact/
├── .env
├── .gitignore
├── config.yaml
├── etl/
│   ├── __init__.py
│   ├── main.py                 # Orchestrator script to run the pipeline
│   ├── extract.py              # Functions for reading data
│   ├── transform.py            # Core transformation and deduplication logic
│   ├── load.py                 # Functions for writing to the database
│   ├── utils.py                # Helper functions, logging setup
│   ├── input_data/             # Source CSVs are placed here
│   ├── processed_data/         # Successfully processed files are moved here
│   ├── review/                 # CSVs for manual review are saved here
│   └── logs/
│       └── pipeline.log
└── requirements.txt