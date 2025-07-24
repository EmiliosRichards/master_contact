import logging
import os
import pandas as pd
import yaml
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
from sqlalchemy.engine import Engine
from sqlalchemy import text
from datetime import datetime
import sys
import click

# Add project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from etl.scripts.extract import find_files, extract_data
from etl.scripts.transform import apply_transformations, clean_data
from etl.scripts.load import get_db_engine, load_to_db, move_processed_file
from etl.scripts.utils import setup_logging

@click.command()
@click.option('--dry-run', is_flag=True, help="Run the ETL process without loading data into the database.")
@click.option('--quiet', is_flag=True, help="Suppress log output during a dry run for cleaner output.")
def main(dry_run, quiet):
    """Main ETL pipeline orchestrator."""
    load_dotenv()

    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    logger = setup_logging(config["log_file"])

    # If quiet mode is enabled during a dry run, suppress INFO logs
    if dry_run and quiet:
        logging.getLogger().setLevel(logging.WARNING)

    if dry_run:
        logger.info("--- Starting ETL in Dry Run mode. No changes will be made to the database. ---")
    else:
        logger.info("--- Starting ETL in Live mode. ---")

    try:
        engine = get_db_engine()
    except ValueError as e:
        logger.critical(f"Halting execution: {e}")
        return

    # --- Audit Log: Create a new run record (skip in dry run) ---
    run_id = None
    if not dry_run:
        with engine.connect() as connection:
            try:
                tag_used = config.get('tag')
                result = connection.execute(
                    text("INSERT INTO etl_runs (status, tag_used) VALUES ('running', :tag) RETURNING id"),
                    {"tag": tag_used}
                ).fetchone()
                if result:
                    run_id = result[0]
                    logger.info(f"Created new ETL run record with ID: {run_id}")
                connection.commit()
            except Exception as e:
                logger.error(f"Failed to create ETL run record: {e}")

    total_contacts_added = 0
    processed_files = []
    pipeline_status = "completed"

    try:
        # Load existing contacts for deduplication checks
        existing_names = []
        existing_phones = []
        if not dry_run:
            try:
                existing_contacts = pd.read_sql("SELECT company_name, phone_number FROM contacts", engine)
                existing_names = existing_contacts['company_name'].str.lower().tolist()
                existing_phones = existing_contacts['phone_number'].tolist()
                logger.info(f"Loaded {len(existing_contacts)} existing contacts for deduplication.")
            except Exception as e:
                logger.warning(f"Could not load existing contacts. Deduplication may be affected. Error: {e}")

        source_dir = config["source_directory"]
        files_to_process = find_files(source_dir)

        for file_path in files_to_process:
            logger.info(f"--- Processing file: {file_path.name} ---")
            
            raw_df = extract_data(file_path)
            if raw_df.empty:
                continue

            transformed_df, json_keys = apply_transformations(raw_df, file_path.name, config)
            cleaned_df = clean_data(transformed_df.copy())

            logger.info("Starting deduplication...")
            pre_dedupe_rows = len(cleaned_df)
            cleaned_df = cleaned_df[~cleaned_df['phone_number'].isin(existing_phones)]
            rows_after_phone_check = len(cleaned_df)
            logger.info(f"Removed {pre_dedupe_rows - rows_after_phone_check} rows with existing phone numbers.")

            potential_duplicates_to_review = []
            if config.get("deduplication", {}).get("enable_fuzzy_matching", True):
                logger.info("Fuzzy matching for company names is enabled.")
                rows_to_drop = []
                for idx, row in cleaned_df.iterrows():
                    company_name = str(row.get("company_name", "")).lower()
                    if not company_name: continue
                    matches = process.extract(company_name, existing_names, scorer=fuzz.token_sort_ratio, limit=1, score_cutoff=config["deduplication"]["company_name_threshold"])
                    if matches:
                        match_name, score, _ = matches[0]
                        logger.warning(f"Potential duplicate for '{row['company_name']}'. Similarity: {score}%. Matched: '{match_name}'. Skipping.")
                        potential_duplicates_to_review.append(row)
                        rows_to_drop.append(idx)
                if rows_to_drop:
                    cleaned_df = cleaned_df.drop(rows_to_drop)
            else:
                logger.info("Fuzzy matching for company names is disabled.")
            
            if potential_duplicates_to_review:
                review_dir = config["review_directory"]
                os.makedirs(review_dir, exist_ok=True)
                pd.DataFrame(potential_duplicates_to_review).to_csv(os.path.join(review_dir, f"review_{file_path.stem}.csv"), index=False)

            logger.info(f"Deduplication complete. {len(cleaned_df)} rows remaining.")

            if dry_run:
                logger.info(f"[DRY RUN] Would load {len(cleaned_df)} new contacts from {file_path.name}.")
                if not cleaned_df.empty:
                    print(f"\n--- [DRY RUN] Sample of Processed Data for {file_path.name} ---")
                    print(cleaned_df.head(1).to_string())
                    print("--- End of Sample ---\n")
            else:
                try:
                    load_to_db(cleaned_df, "contacts", engine, json_keys)
                    move_processed_file(file_path, config["processed_directory"])
                    if not cleaned_df.empty:
                        existing_names.extend(cleaned_df['company_name'].str.lower().tolist())
                        existing_phones.extend(cleaned_df['phone_number'].tolist())
                except Exception as e:
                    logger.error(f"Failed to load data for {file_path.name}. Error: {e}")
                    pipeline_status = "failed"
                    continue
            
            total_contacts_added += len(cleaned_df)
            processed_files.append(file_path.name)

    except Exception as e:
        logger.critical(f"An unexpected error occurred in the main pipeline: {e}", exc_info=True)
        pipeline_status = "failed"
    
    finally:
        if not dry_run and run_id:
            with engine.connect() as connection:
                try:
                    connection.execute(
                        text("""
                            UPDATE etl_runs
                            SET status = :status, files_processed = :files, contacts_added = :count, finished_at = :finished
                            WHERE id = :run_id
                        """),
                        {
                            "status": pipeline_status, "files": processed_files, "count": total_contacts_added,
                            "finished": datetime.utcnow(), "run_id": run_id
                        }
                    )
                    connection.commit()
                    logger.info(f"Successfully updated ETL run record ID: {run_id}")
                except Exception as e:
                    logger.error(f"Failed to update ETL run record: {e}")
        
        if dry_run:
            logger.info("--- ETL dry run finished. No changes were made to the database. ---")
        else:
            logger.info(f"--- ETL pipeline finished with status: {pipeline_status}. ---")

if __name__ == "__main__":
    main()