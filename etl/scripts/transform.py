import logging
import os
import pandas as pd
import numpy as np
import json
from typing import Dict, List
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from typing import Dict, List, Tuple

def apply_transformations(df: pd.DataFrame, file_path: str, config: Dict) -> tuple[pd.DataFrame, List[str]]:
    """
    Applies all transformations based on data source profiles.
    1. Puts all raw data into a JSONB field.
    2. Promotes the best available data to the main columns based on promotion rules.
    """
    logger.info("Starting two-tiered transformation...")

    # --- Tier 1: Preserve raw data and get keys for profiling ---
    # Get the list of all original columns to be used for profiling
    json_keys = sorted(df.columns.tolist())
    # Create a copy of the raw data with NaNs handled for later JSON conversion
    raw_json_df = df.replace({np.nan: None})

    # --- Tier 2: Promote best data to structured columns ---
    # Identify the correct profile based on the filename
    source_profiles = config.get("data_source_profiles", {})
    profile_name = "default"
    for name, profile_data in source_profiles.items():
        if profile_data.get("file_name_contains", "") in file_path:
            profile_name = name
            break
    
    logger.info(f"Applying promotion rules for profile: '{profile_name}'")
    rules = source_profiles.get(profile_name, {}).get("promotion_rules", {})

    # Create the new structured columns based on promotion rules
    for db_col, source_options in rules.items():
        # Create a new series to hold the promoted data for this column
        new_col_series = pd.Series(index=df.index, dtype=object)
        for source_col in source_options:
            if source_col in df.columns:
                # Coalesce values into the new series from the original df
                new_col_series = new_col_series.where(new_col_series.notna(), df[source_col])
        # Assign the completed series to the DataFrame, overwriting or creating the column
        df[db_col] = new_col_series

    # --- Data Cleaning for Promoted Columns ---
    if 'is_b2b' in df.columns:
        logger.debug("Standardizing 'is_b2b' column to boolean values...")
        # Map string values to actual booleans. Unmapped values (like 'Unknown') become NaN.
        b2b_map = {'yes': True, 'true': True, '1': True, 'no': False, 'false': False, '0': False}
        
        # Ensure the column is string and lowercase for consistent mapping
        is_b2b_series = df['is_b2b'].astype(str).str.lower()
        df['is_b2b'] = is_b2b_series.map(b2b_map)
        
        # Convert the column to pandas' nullable boolean type.
        # This correctly handles True, False, and NA (which becomes NULL in the DB).
        df['is_b2b'] = df['is_b2b'].astype('boolean')
        logger.debug("Finished standardizing 'is_b2b' column.")

    # --- Final Assembly ---
    logger.debug(f"Before assembly, df columns are: {df.columns.tolist()}")
    
    # Define the final structured columns we want in our table
    structured_columns = ['company_name', 'url', 'phone_number', 'industry', 'is_b2b', 'customer_target_segments', 'tags']
    
    # Add the tag from the config as a list to the 'tags' column
    tag = config.get("tag")
    if tag:
        df['tags'] = [[tag] for _ in range(len(df))]
        logger.info(f"Applied tag '{tag}' to the dataset as a list.")
    else:
        df['tags'] = [[] for _ in range(len(df))] # Ensure the column exists with an empty list

    # Create the additional_info column from the preserved raw data
    logger.debug("Creating 'additional_info' from raw_json_df...")
    # For each row, create a dictionary and then convert it to a JSON string.
    # This ensures each row's additional_info contains only that row's data.
    df['additional_info'] = raw_json_df.apply(lambda row: row.to_dict(), axis=1).apply(json.dumps)
    logger.debug(f"Type of 'additional_info' column after json.dumps: {type(df['additional_info'].iloc[0])}")
    logger.debug(f"Sample additional_info value: {df['additional_info'].iloc[0]}")

    # Select only the final structured columns + the JSONB field
    # Ensure all structured columns exist, even if no rule was found
    for col in structured_columns:
        if col not in df.columns:
            df[col] = None
            
    # Select the final columns for the database
    final_columns = structured_columns + ['additional_info']
    final_df = df[final_columns]
    logger.debug(f"Final DataFrame columns: {final_df.columns.tolist()}")
    
    logger.info(f"Transformation complete. Final columns: {final_df.columns.tolist()}")

    return final_df, json_keys

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs various data cleaning operations.

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    logger.info("Starting data cleaning...")
    df = df.copy()

    # --- Data Validation: Ensure required fields are present ---
    # --- Data Validation and Enrichment ---
    def derive_name_from_url(url):
        if pd.isna(url):
            return None
        try:
            # Ensure URL has a scheme for proper parsing
            if '://' not in str(url):
                url = 'http://' + str(url)
            
            netloc = urlparse(url).netloc
            if not netloc:
                return None
            
            # Extract the domain name (e.g., 'google' from 'www.google.com')
            domain = netloc.replace('www.', '')
            company_name = domain.split('.')[0]
            
            # Return the capitalized name if found
            return company_name.capitalize() if company_name else None
        except Exception:
            return None

    # Attempt to fill missing company names from URLs
    if 'company_name' in df.columns and 'url' in df.columns:
        missing_name_mask = df['company_name'].isnull() | (df['company_name'].astype(str).str.strip() == '')
        
        if missing_name_mask.any():
            logger.info("Attempting to derive missing company names from URLs...")
            derived_names = df.loc[missing_name_mask, 'url'].apply(derive_name_from_url)
            df.loc[missing_name_mask, 'company_name'] = df.loc[missing_name_mask, 'company_name'].fillna(derived_names)
            
            num_derived = derived_names.notna().sum()
            if num_derived > 0:
                logger.info(f"Successfully derived {num_derived} company names.")

    # --- Final Validation: Check for rows that still have missing company names ---
    if 'company_name' in df.columns:
        invalid_rows_mask = df['company_name'].isnull() | (df['company_name'].astype(str).str.strip() == '')
        invalid_df = df[invalid_rows_mask]

        if not invalid_df.empty:
            invalid_dir = "etl/invalid_records"
            os.makedirs(invalid_dir, exist_ok=True)
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            invalid_path = os.path.join(invalid_dir, f"missing_company_name_{timestamp}.csv")
            invalid_df.to_csv(invalid_path, index=False)
            logger.warning(f"Saved {len(invalid_df)} rows with missing company name to {invalid_path}")

            # Remove any remaining invalid rows from the main dataframe
            df = df[~invalid_rows_mask]

    # Clean phone numbers: remove common characters and whitespace
    if "phone_number" in df.columns:
        # Define a function to clean each phone number individually.
        def clean_phone(phone):
            if pd.isna(phone):
                return None
            # Convert to string, remove unwanted characters, and handle empty strings.
            cleaned = str(phone).replace('nan', '').replace('None', '')
            cleaned = pd.Series(cleaned).str.replace(r"[()\-\s]", "", regex=True).iloc[0]
            return cleaned if cleaned else None

        # Apply the cleaning function to the phone_number column.
        df['phone_number'] = df['phone_number'].apply(clean_phone)
        logger.info("Cleaned 'phone_number' column, converting blanks to NULL.")

    # Trim whitespace from all object (string) columns, EXCLUDING specific columns
    for col in df.select_dtypes(include=["object"]).columns:
        if col not in ['additional_info', 'tags']:
            df[col] = df[col].str.strip()
        
    # Drop duplicates within the dataframe based on phone number
    pre_dedupe_rows = len(df)
    # Identify duplicates, keeping the first occurrence
    duplicates = df[df.duplicated(subset=['phone_number'], keep='first')]
    
    if not duplicates.empty:
        # Save duplicates to a file for review
        dropped_dir = "etl/dropped_duplicates"
        os.makedirs(dropped_dir, exist_ok=True)
        # Use a more descriptive filename, perhaps with a timestamp
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        dropped_path = os.path.join(dropped_dir, f"duplicates_{timestamp}.csv")
        duplicates.to_csv(dropped_path, index=False)
        logger.info(f"Saved {len(duplicates)} duplicate rows to {dropped_path}")

    # Drop the identified duplicates from the main dataframe
    df.drop_duplicates(subset=['phone_number'], keep='first', inplace=True)
    rows_after_dedupe = len(df)
    if pre_dedupe_rows > rows_after_dedupe:
        logger.info(f"Removed {pre_dedupe_rows - rows_after_dedupe} duplicate phone numbers from the source file.")

    logger.info("Data cleaning complete.")
    return df