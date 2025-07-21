import logging
import os
import sys
from datetime import datetime
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
setup_logging("etl/logs/update_status.log")
logger = logging.getLogger(__name__)

def clean_phone_number(phone):
    """Removes common characters and whitespace from a phone number."""
    if not isinstance(phone, str):
        return None
    return phone.replace(" ", "").replace("(", "").replace(")", "").replace("-", "")

@click.command()
@click.argument('input_file', type=click.Path(exists=True))
def update_contacts(input_file):
    """
    Updates the status of contacts in the database based on a list of phone numbers.

    This script will:
    - Set the status to 'used'.
    - Set the last_used timestamp to the current time.
    - Add a 'used' tag to the tags array.
    """
    logger.info(f"--- Starting Contact Status Update from file: {input_file} ---")
    
    # Read and clean phone numbers from the input file
    try:
        with open(input_file, 'r') as f:
            raw_phones = [line.strip() for line in f if line.strip()]
        
        cleaned_phones = [clean_phone_number(p) for p in raw_phones]
        # Filter out any None values that may result from cleaning
        phones_to_update = [p for p in cleaned_phones if p]
        
        if not phones_to_update:
            logger.warning("No valid phone numbers found in the input file.")
            return
            
        logger.info(f"Found {len(phones_to_update)} unique phone numbers to process.")

    except Exception as e:
        logger.critical(f"Failed to read or process input file: {e}")
        return

    engine = get_db_engine()
    updated_count = 0
    
    # Using a transaction ensures that all updates succeed or none do.
    with engine.connect() as connection:
        with connection.begin(): # Starts a transaction
            try:
                # SQL statement to update status, timestamp, and append a tag if not present
                update_sql = text("""
                    UPDATE contacts
                    SET 
                        status = 'used',
                        last_used = :current_time,
                        tags = array_append(tags, 'used')
                    WHERE phone_number = :phone AND NOT ('used' = ANY(tags));
                """)

                for phone in phones_to_update:
                    result = connection.execute(
                        update_sql,
                        {"current_time": datetime.now(), "phone": phone}
                    )
                    if result.rowcount > 0:
                        updated_count += result.rowcount
                        logger.info(f"Successfully updated contact with phone: {phone}")

            except Exception as e:
                logger.error(f"An error occurred during the database update. Rolling back changes. Error: {e}")
                # The 'with' block will automatically handle the rollback on exception
                raise

    logger.info(f"--- Update process finished. Successfully updated {updated_count} contacts. ---")

if __name__ == "__main__":
    update_contacts()