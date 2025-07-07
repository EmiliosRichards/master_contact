import logging
import os
import pandas as pd
import yaml
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
from sqlalchemy.engine import Engine

import sys

# Add project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from etl.scripts.extract import find_csv_files, extract_from_csv
from etl.scripts.transform import apply_transformations, clean_data
from etl.scripts.load import get_db_engine, load_to_db, move_processed_file
from etl.scripts.utils import setup_logging

def main():
    """Main ETL pipeline orchestrator."""
    load_dotenv()

    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    logger = setup_logging(config["log_file"])
    logger.info("ETL pipeline started.")

    try:
        engine = get_db_engine()
    except ValueError as e:
        logger.critical(f"Halting execution: {e}")
        return

    # Load existing contacts for deduplication checks
    try:
        existing_contacts = pd.read_sql("SELECT company_name, phone_number FROM contacts", engine)
        existing_names = existing_contacts['company_name'].str.lower().tolist()
        existing_phones = existing_contacts['phone_number'].tolist()
        logger.info(f"Loaded {len(existing_contacts)} existing contacts for deduplication.")
    except Exception as e:
        logger.warning(f"Could not load existing contacts. Deduplication may be affected. Error: {e}")
        existing_names = []
        existing_phones = []

    source_dir = config["source_directory"]
    source_dir = config["source_directory"]
    csv_files = find_csv_files(source_dir)

    for file_path in csv_files:
        logger.info(f"--- Processing file: {file_path.name} ---")
        
        raw_df = extract_from_csv(file_path)
        if raw_df.empty:
            continue

        # --- Transformation Stage ---
        transformed_df, json_keys = apply_transformations(raw_df, file_path.name, config)
        cleaned_df = clean_data(transformed_df.copy())

        # --- Deduplication Stage ---
        logger.info("Starting deduplication...")
        
        # 1. Check for unique phone numbers
        pre_dedupe_rows = len(cleaned_df)
        cleaned_df = cleaned_df[~cleaned_df['phone_number'].isin(existing_phones)]
        rows_after_phone_check = len(cleaned_df)
        logger.info(f"Removed {pre_dedupe_rows - rows_after_phone_check} rows with existing phone numbers.")

        # 2. Fuzzy match company names (optional)
        potential_duplicates_to_review = []
        if config.get("deduplication", {}).get("enable_fuzzy_matching", True):
            logger.info("Fuzzy matching for company names is enabled.")
            potential_duplicates_to_review = []
            rows_to_drop = []
            
            for idx, row in cleaned_df.iterrows():
                company_name = str(row.get("company_name", "")).lower()
                if not company_name:
                    continue

                matches = process.extract(
                    company_name,
                    existing_names,
                    scorer=fuzz.token_sort_ratio,
                    limit=1,
                    score_cutoff=config["deduplication"]["company_name_threshold"]
                )

                if matches:
                    match_name, score, _ = matches[0]
                    logger.warning(
                        f"Potential duplicate found for '{row['company_name']}'. "
                        f"Similarity: {score}%. Matched with: '{match_name}'. "
                        "Skipping row and adding to review file."
                    )
                    potential_duplicates_to_review.append(row)
                    rows_to_drop.append(idx)

            if rows_to_drop:
                cleaned_df = cleaned_df.drop(rows_to_drop)
        else:
            logger.info("Fuzzy matching for company names is disabled.")
            
        if potential_duplicates_to_review:
            review_dir = config["review_directory"]
            os.makedirs(review_dir, exist_ok=True)
            review_df = pd.DataFrame(potential_duplicates_to_review)
            review_path = os.path.join(review_dir, f"review_{file_path.stem}.csv")
            review_df.to_csv(review_path, index=False)
            logger.info(f"Saved {len(review_df)} potential duplicates to {review_path}")

        logger.info(f"Deduplication complete. {len(cleaned_df)} rows remaining for insertion.")

        # --- Load Stage ---
        try:
            load_to_db(cleaned_df, "contacts", engine, json_keys)
            move_processed_file(file_path, config["processed_directory"])

            # --- Update existing contacts in memory for the next iteration ---
            # Add the newly loaded contacts' data to the lists for deduplication
            if not cleaned_df.empty:
                logger.debug(f"Updating in-memory contact lists with {len(cleaned_df)} new records.")
                newly_added_names = cleaned_df['company_name'].str.lower().tolist()
                newly_added_phones = cleaned_df['phone_number'].tolist()
                
                existing_names.extend(newly_added_names)
                existing_phones.extend(newly_added_phones)
                logger.debug("In-memory lists updated.")
        except Exception as e:
            logger.error(f"Failed to load data for {file_path.name}. Halting processing for this file. Error: {e}")
            # Optionally, move to an error directory
            continue

    logger.info("ETL pipeline finished.")

if __name__ == "__main__":
    main()