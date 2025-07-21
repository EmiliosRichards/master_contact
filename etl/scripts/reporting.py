import logging
import os
import sys
import json
from pathlib import Path
import pandas as pd
import yaml
from dotenv import load_dotenv
import click

# Add project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from sqlalchemy import text
from etl.scripts.load import get_db_engine
from etl.scripts.utils import setup_logging

# --- Configuration ---
load_dotenv()
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logger = setup_logging(config["log_file"])

# --- Helper Functions ---
def get_engine():
    try:
        return get_db_engine()
    except ValueError as e:
        logger.critical(f"Database not configured. Halting execution: {e}")
        sys.exit(1)

# --- CLI Commands ---
@click.group()
def cli():
    """A CLI tool to view and manage the contacts database."""
    pass

@cli.command()
@click.option('--limit', default=10, help='Number of contacts to display.')
def view_contacts(limit):
    """Displays the most recent contacts from the database."""
    logger.info(f"Fetching the last {limit} contacts...")
    engine = get_engine()
    try:
        df = pd.read_sql(f"SELECT id, company_name, phone_number, industry, created_at FROM contacts ORDER BY created_at DESC LIMIT {limit}", engine)
        if df.empty:
            print("No contacts found in the database.")
            return
        print("--- Most Recent Contacts ---")
        print(df.to_string())
        print("--------------------------")
    except Exception as e:
        logger.error(f"An error occurred while fetching contacts: {e}")

@cli.command()
@click.option('--filename', default='contact_export.xlsx', help='Name of the output Excel file.')
def export_contacts(filename):
    """Exports all contacts to an Excel file."""
    logger.info(f"Exporting all contacts to {filename}...")
    engine = get_engine()
    try:
        df = pd.read_sql("SELECT * FROM contacts", engine)
        if df.empty:
            print("No contacts to export.")
            return
        df.to_excel(filename, index=False)
        logger.info(f"Successfully exported {len(df)} contacts to {os.path.abspath(filename)}")
    except Exception as e:
        logger.error(f"An error occurred during export: {e}")

@cli.command()
def view_profiles():
    """Displays all discovered contact data profiles."""
    logger.info("Fetching contact profiles...")
    engine = get_engine()
    try:
        df = pd.read_sql("SELECT id, contact_count, json_keys FROM contact_profiles ORDER BY contact_count DESC", engine)
        if df.empty:
            print("No contact profiles found.")
            return
        print("--- Contact Data Profiles ---")
        for _, row in df.iterrows():
            print(f"\nProfile ID: {row['id']} ({row['contact_count']} contacts)")
            print("  Keys:", ", ".join(row['json_keys']))
        print("\n---------------------------")
    except Exception as e:
        logger.error(f"An error occurred while fetching profiles: {e}")

@cli.command()
def check_review_folder():
    """Checks for files needing manual review."""
    review_dir = Path(config["review_directory"])
    logger.info(f"Checking for files in {review_dir}...")
    if not review_dir.exists() or not any(review_dir.iterdir()):
        print("Review folder is empty. No files need manual review.")
        return
    
    print("--- Files for Manual Review ---")
    for file in review_dir.glob("*.csv"):
        print(f"- {file.name}")
    print("-----------------------------")

@cli.command()
def check_dropped_duplicates():
    """Checks for files containing dropped duplicate records."""
    dropped_dir = Path("etl/dropped_duplicates")
    logger.info(f"Checking for files in {dropped_dir}...")
    if not dropped_dir.exists() or not any(dropped_dir.iterdir()):
        print("Dropped duplicates folder is empty.")
        return
        
    print("--- Dropped Duplicate Files ---")
    for file in dropped_dir.glob("*.csv"):
        print(f"- {file.name}")
    print("-------------------------------")

@cli.command()
@click.confirmation_option(prompt='Are you sure you want to delete all contacts and profiles?')
def reset_database():
    """
    Deletes all records from the contacts and contact_profiles tables.
    This is a destructive operation and cannot be undone.
    """
    logger.warning("--- Starting Database Reset ---")
    engine = get_engine()
    with engine.connect() as connection:
        try:
            logger.info("Deleting all records from 'contacts' table...")
            connection.execute(text("DELETE FROM contacts;"))
            logger.info("...done.")
            
            logger.info("Deleting all records from 'contact_profiles' table...")
            connection.execute(text("DELETE FROM contact_profiles;"))
            logger.info("...done.")
            
            connection.commit()
            logger.info("--- Database Reset Successfully ---")
        except Exception as e:
            logger.error(f"An error occurred during database reset: {e}")
            connection.rollback()

@cli.command()
@click.option('--id', required=True, type=int, help='The ID of the contact to audit.')
def audit_contact(id):
    """Displays a full audit report for a single contact."""
    logger.info(f"Generating audit report for contact ID: {id}")
    engine = get_engine()
    try:
        # Fetch the main contact record
        contact_df = pd.read_sql(f"SELECT * FROM contacts WHERE id = {id}", engine)
        if contact_df.empty:
            print(f"Error: No contact found with ID: {id}")
            return

        contact = contact_df.iloc[0]
        
        print(f"\n--- Audit Report for Contact ID: {id} ---")
        
        # --- Display Promoted Fields ---
        print("\n== Main Database Fields ==")
        promoted_data = {
            "Company Name": contact.get('company_name'),
            "Phone Number": contact.get('phone_number'),
            "URL": contact.get('url'),
            "Industry": contact.get('industry'),
            "Is B2B": contact.get('is_b2b'),
            "Profile ID": contact.get('profile_id'),
            "Created At": contact.get('created_at')
        }
        for key, value in promoted_data.items():
            print(f"{key+':':<15} {value}")

        # --- Display Full Raw Data ---
        print("\n== Full Original Data (from additional_info) ==")
        additional_info_str = contact.get('additional_info')
        if additional_info_str and isinstance(additional_info_str, str):
            try:
                additional_info = json.loads(additional_info_str)
                for key, value in sorted(additional_info.items()):
                    print(f"- {key+':':<30} {value}")
            except json.JSONDecodeError:
                print("Could not decode the additional_info JSON string.")
        else:
            print("No additional information found.")

        print("\n-----------------------------------------")

    except Exception as e:
        logger.error(f"An error occurred during audit: {e}")


@cli.command()
def count_contacts():
    """Counts the total number of contacts in the database."""
    logger.info("Counting total contacts...")
    engine = get_engine()
    try:
        count = pd.read_sql("SELECT COUNT(id) FROM contacts", engine).iloc[0,0]
        print(f"Total number of contacts: {count}")
    except Exception as e:
        logger.error(f"An error occurred while counting contacts: {e}")

if __name__ == "__main__":
    cli()