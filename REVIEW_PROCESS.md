# Data Review and Management Workflow

This document outlines the standard operating procedure for reviewing and managing the data processed by the ETL pipeline.

## Core Principle

The pipeline is designed to be cautious. It automates the cleaning of clear-cut duplicates but sets aside ambiguous cases for a human decision. This ensures both high data quality and that no valuable contacts are accidentally discarded.

## The Review Folders

Your review process is centered around two key directories:

1.  **`etl/review/`**: Contains **Potential Duplicates**. These are contacts from a new file that the pipeline suspects might already be in your database (based on a "fuzzy" match of the company name). The pipeline has paused on these, waiting for your decision.
2.  **`etl/dropped_duplicates/`**: Contains **Exact Duplicates**. These are contacts that were duplicates *within the same source file*. The pipeline has automatically removed them to prevent data corruption and saved them here for your records.

---

## Your Periodic Workflow

You should perform this check periodically to keep your data clean and up-to-date.

### Step 1: Check if a Review is Needed

Open your terminal and run the following commands using the reporting tool:

```bash
# 1. Check for potential duplicates that require a decision
python etl/scripts/reporting.py check-review-folder

# 2. Check which duplicates were automatically dropped
python etl/scripts/reporting.py check-dropped-duplicates
```

### Step 2: The Review Process

#### A. Handling Potential Duplicates (The `review` folder)

This is your most important manual task. Your goal is to decide if a flagged contact is a true duplicate or a new, valid entry.

1.  **Open the File**: Navigate to the `etl/review/` directory and open the relevant CSV file (e.g., `review_mid_001_ER4K_spg_apol_20250701.csv`).
2.  **Analyze the Contact**: Look at the data in the row. Pay attention to the company name, URL, and other details.
3.  **Compare with Existing Data**: The pipeline logs will tell you which existing company caused the match. Use a SQL client or the `view-contacts` command in the reporting tool to find and examine that existing contact in your database.
4.  **Make a Decision**:
    *   **If it IS a duplicate**: The pipeline was correct. You can simply **delete the review CSV file**. No further action is needed.
    *   **If it is NOT a duplicate**: The pipeline was being overly cautious. This is a new, valid contact. You should **manually add the contact to the database** using a SQL `INSERT` statement. This ensures the valid contact is not lost. After adding it, you can delete the review CSV file.

#### B. Handling Dropped Duplicates (The `dropped_duplicates` folder)

This folder is primarily for transparency and record-keeping. It shows you what the pipeline cleaned up on its own.

1.  **Open the File**: Navigate to the `etl/dropped_duplicates/` directory and open the relevant CSV file.
2.  **Analyze the Data**: This file contains all the rows that were removed from an input file because their `phone_number` was identical to another row *in that same file*. The pipeline always keeps the *first* instance it encounters and drops the subsequent ones.
3.  **Make a Decision**:
    *   **In 99% of cases, no action is needed**. This file is a log of the automated cleaning process. You can review it for peace of mind and then archive or delete it.
    *   If you find that a dropped record contained better or more complete information than the one that was kept, you can **manually update the existing contact in the database** with the better information using a SQL `UPDATE` statement.

By following this simple workflow, you maintain full control over your data quality and ensure that your contact database remains accurate and valuable.