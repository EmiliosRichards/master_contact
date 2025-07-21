import logging
import os
import sys
from datetime import datetime
import glob
import pandas as pd
from sqlalchemy import text
import click

# Add project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from etl.scripts.load import get_db_engine
from etl.scripts.utils import setup_logging

# --- Configuration ---
setup_logging("etl/logs/batch_update.log")
logger = logging.getLogger(__name__)

def clean_phone_number(phone):
    """Removes common characters and whitespace from a phone number."""
    if not isinstance(phone, str):
        return None
    return phone.replace(" ", "").replace("(", "").replace(")", "").replace("-", "")

def find_phone_column(df):
    """Finds the correct phone number column in a DataFrame."""
    possible_columns = ["Company Phone", "Number"]
    for col in possible_columns:
        if col in df.columns:
            return col
    return None

@click.command()
@click.argument('input_dir', type=click.Path(exists=True))
@click.option('--report-only', is_flag=True, help="Run in report-only mode without updating the database.")
@click.option('--tag', default='used', help="The tag to apply to the updated contacts.")
def batch_update(input_dir, report_only, tag):
    """
    Scans a directory for CSV files, extracts phone numbers, and updates their status.
    """
    logger.info(f"--- Starting Batch Contact Status Update from directory: {input_dir} ---")
    
    all_phones = set()
    source_files = glob.glob(os.path.join(input_dir, "*.csv"))

    if not source_files:
        logger.warning(f"No CSV files found in directory: {input_dir}")
        return

    logger.info(f"Found {len(source_files)} CSV files to process.")

    for file_path in source_files:
        try:
            try:
                # First, try to read as a standard comma-separated CSV
                df = pd.read_csv(file_path)
            except Exception:
                # If that fails, try reading as a tab-separated file
                logger.info(f"Could not parse {file_path} as standard CSV, trying tab-separated...")
                df = pd.read_csv(file_path, sep='\t')

            phone_col = find_phone_column(df)
            
            if not phone_col:
                logger.warning(f"No recognized phone number column found in {file_path}. Skipping.")
                continue

            logger.info(f"Extracting phone numbers from '{phone_col}' column in {file_path}...")
            phones_in_file = df[phone_col].dropna().tolist()
            for phone in phones_in_file:
                cleaned = clean_phone_number(str(phone))
                if cleaned:
                    all_phones.add(cleaned)
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            continue
            
    if not all_phones:
        logger.warning("No valid phone numbers found across all files.")
        return
        
    logger.info(f"Found a total of {len(all_phones)} unique phone numbers to update.")

    engine = get_db_engine()
    
    if report_only:
        logger.info("--- Running in Report-Only Mode ---")
        with engine.connect() as connection:
            try:
                # Use a single, parameterized query for security and efficiency
                query = text("SELECT COUNT(id) FROM contacts WHERE phone_number = ANY(:phones)")
                result = connection.execute(query, {"phones": list(all_phones)}).scalar_one_or_none()
                found_count = result if result is not None else 0
                
                print("\n--- Dry Run Report ---")
                print(f"Total unique phone numbers in source files: {len(all_phones)}")
                print(f"Phone numbers found in the database: {found_count}")
                print("----------------------")

            except Exception as e:
                logger.error(f"An error occurred during the database query: {e}")
    else:
        updated_count = 0
        with engine.connect() as connection:
            with connection.begin():
                try:
                    # Use a single, parameterized query to update all contacts at once
                    update_sql = text("""
                        UPDATE contacts
                        SET
                            status = 'used',
                            last_used = :current_time,
                            tags = array_append(tags, :tag)
                        WHERE phone_number = ANY(:phones) AND (tags IS NULL OR NOT (tags @> ARRAY[:tag]));
                    """)
                    result = connection.execute(
                        update_sql,
                        {"current_time": datetime.now(), "phones": list(all_phones), "tag": tag}
                    )
                    updated_count = result.rowcount
                except Exception as e:
                    logger.error(f"An error occurred during the database update. Rolling back. Error: {e}")
                    raise
        logger.info(f"--- Batch update finished. Successfully updated {updated_count} contacts. ---")

if __name__ == "__main__":
    batch_update()