# ETL Pipeline Architecture Overview

This document provides a comprehensive overview of the ETL (Extract, Transform, Load) pipeline. The system is designed to process contact information from CSV files, clean and transform the data, perform deduplication, and load the results into a PostgreSQL database.

## 1. Core Objective

The primary goal of this pipeline is to automate the ingestion of new contact data from various CSV sources. It ensures data quality through cleaning, standardization, and a robust deduplication process, preventing redundant entries in the central `contacts` database.

## 2. Pipeline Workflow

The ETL process is orchestrated by `etl/main.py` and follows a sequential, multi-stage workflow for each source file found.

```mermaid
graph TD
    A[Start] --> B{Find CSV Files};
    B --> C{Process File};
    C --> D[Extract Data from CSV];
    D --> E[Transform Data & Profile Schema];
    E --> F{Deduplicate Data};
    F --> G[Load to Database];
    G --> H{Move Processed File};
    H --> I[End];

    subgraph "Extraction"
        D
    end

    subgraph "Transformation & Profiling"
        E
    end

    subgraph "Deduplication"
        F
    end

    subgraph "Loading"
        G
    end

    style D fill:#f9f,stroke:#333,stroke-width:2px
    style E fill:#f9f,stroke:#333,stroke-width:2px
    style F fill:#f9f,stroke:#333,stroke-width:2px
    style G fill:#f9f,stroke:#333,stroke-width:2px
```

### Stages:

1.  **Initialization**:
    *   Loads the database connection URL from `.env`.
    *   Reads configuration from `config.yaml`.
    *   Sets up logging to both console and a rotating file (`etl/logs/pipeline.log`).

2.  **Pre-fetch for Deduplication**:
    *   Before processing files, it connects to the database and fetches all existing `company_name` and `phone_number` values from the `contacts` table to be used in the deduplication step.

3.  **Extraction (`etl/extract.py`)**:
    *   Scans the `source_directory` (defined in `config.yaml`) for new `.csv` files.
    *   Reads each CSV file into a pandas DataFrame.

4.  **Transformation & Profiling (`etl/transform.py`)**:
    *   **Dynamic Profiling**: Before transformation, the script captures the exact schema (column names) of the source CSV. It generates a unique hash for this schema and stores it in the `contact_profiles` table. This allows for tracking the structure of every dataset that enters the pipeline.
    *   **Two-Tiered Transformation**:
        1.  **Preservation**: The entire raw data from each row is serialized into a `JSONB` field (`additional_info`). This ensures no data is ever lost.
        2.  **Promotion**: Key fields (like `company_name`, `phone_number`, etc.) are "promoted" from the raw data into the main structured columns of the `contacts` table. The promotion rules are defined in `config.yaml` for each data source profile, allowing the system to intelligently pick the best available data (e.g., choosing `found_number` over `Original_Number`).
    *   **Data Cleaning**: Standardizes phone numbers, trims whitespace, and ensures data types are correct (e.g., converting "yes"/'no" to booleans).

5.  **Deduplication (`etl/main.py`)**:
    *   **Phone Number Check**: An exact match is performed to discard any records where the `phone_number` already exists in the database.
    *   **Fuzzy Company Name Matching**: Uses the `rapidfuzz` library to compare the `company_name` against existing names. If the similarity score exceeds the `company_name_threshold` from `config.yaml`, the record is flagged as a potential duplicate.
    *   **Review Process**: Potential duplicates are not loaded. They are saved to a separate CSV in the `review_directory` for manual inspection.

6.  **Load (`etl/load.py`)**:
    *   Assigns the appropriate `profile_id` (from the `contact_profiles` table) to each record.
    *   The final, cleaned, and unique data is loaded into the `contacts` table in the PostgreSQL database.
    *   The operation uses an `append` method, as duplicates have already been filtered out.

7.  **File Management**:
    *   After a file is successfully processed, it is moved from the source directory to the `processed_directory`.

## 3. Key Components

*   **`etl/main.py`**: The main orchestrator that runs the entire pipeline.
*   **`etl/extract.py`**: Handles finding and reading source CSV files.
*   **`etl/transform.py`**: Contains logic for data cleaning and restructuring.
*   **`etl/load.py`**: Manages database connections, data loading, and profile creation.
*   **`etl/setup_database.py`**: Defines the database schema (`contacts` and `contact_profiles` tables) and ensures it exists.
*   **`config.yaml`**: A critical configuration file that makes the pipeline adaptable. It controls file paths, data source profiles, promotion rules, and the deduplication threshold.
*   **`etl/requirements.txt`**: Lists all Python dependencies for the project.

## 4. Database Schema

The database schema is defined in `etl/setup_database.py` and consists of two core tables.

**Table: `contacts`**
| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `id` | `SERIAL` | `PRIMARY KEY` | Unique identifier for each record. |
| `profile_id` | `INTEGER` | `FOREIGN KEY` | Links to the `contact_profiles` table. |
| `company_name` | `TEXT` | `NOT NULL` | Name of the company. |
| `url` | `TEXT` | | Company website. |
| `phone_number` | `TEXT` | `UNIQUE` | Contact phone number (enforces no duplicates). |
| `is_b2b` | `BOOLEAN` | | Flag for B2B status. |
| `industry` | `TEXT` | | Company's industry. |
| `customer_target_segments` | `TEXT` | | Target customer segments. |
| `additional_info` | `JSONB` | | Catch-all for extra data from source files. |
| `tags` | `TEXT[]` | | Array of tags for categorization. |
| `status` | `TEXT` | `DEFAULT 'active'` | Current status of the contact. |
| `last_used` | `TIMESTAMP` | | Timestamp of the last interaction. |
| `created_at` | `TIMESTAMP` | `DEFAULT NOW()` | Timestamp of record creation. |
| `updated_at` | `TIMESTAMP` | `DEFAULT NOW()` | Timestamp of the last update. |

**Table: `contact_profiles`**
| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `id` | `SERIAL` | `PRIMARY KEY` | Unique identifier for the profile. |
| `profile_hash` | `TEXT` | `UNIQUE` | MD5 hash of the sorted list of JSON keys. |
| `json_keys` | `TEXT[]` | `NOT NULL` | An array of the column names from the source file. |
| `contact_count` | `INTEGER` | `DEFAULT 1` | The number of contacts associated with this profile. |
| `created_at` | `TIMESTAMP` | `DEFAULT NOW()` | Timestamp of profile creation. |

This two-table schema provides a robust system for both storing structured contact data and tracking the metadata of its origin.