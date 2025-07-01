import logging
import pandas as pd
import json
from typing import Dict, List

logger = logging.getLogger(__name__)

def apply_column_mapping(df: pd.DataFrame, column_mapping: Dict) -> pd.DataFrame:
    """
    Applies column renaming and selection based on the mapping configuration.
    Also prepares the data for the additional_info JSONB column.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_mapping (Dict): The column mapping configuration.

    Returns:
        pd.DataFrame: The DataFrame with columns renamed and structured.
    """
    logger.info("Applying column mapping...")
    
    # Invert the main mapping for easier renaming
    # e.g., {"company_name": ["Company", "Company Name"]}
    inverted_mapping = {}
    for key, value in column_mapping.items():
        if key != "additional_info":
            if isinstance(value, list):
                for v in value:
                    inverted_mapping[v] = key
            else:
                inverted_mapping[value] = key

    df = df.rename(columns=inverted_mapping)

    # Consolidate extra columns into 'additional_info'
    additional_info_cols = column_mapping.get("additional_info", [])
    # Select columns that actually exist in the dataframe
    existing_additional_cols = [col for col in additional_info_cols if col in df.columns]

    if existing_additional_cols:
        logger.info(f"Consolidating columns into 'additional_info': {existing_additional_cols}")
        # Create a dictionary from the additional columns, then convert to JSON string
        df["additional_info"] = df[existing_additional_cols].to_dict(orient="records")
        df["additional_info"] = df["additional_info"].apply(json.dumps)
    else:
        df["additional_info"] = None

    # Select only the columns that are part of our target schema
    target_columns = [v for k, v in inverted_mapping.items() if v in df.columns]
    target_columns.append("additional_info")
    
    # Ensure all target columns exist, add if they don't
    for col in target_columns:
        if col not in df.columns:
            df[col] = None
            
    final_df = df[list(set(target_columns))]
    logger.info(f"Column mapping applied. Final columns: {final_df.columns.tolist()}")
    
    return final_df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs various data cleaning operations.

    Args:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    logger.info("Starting data cleaning...")

    # Clean phone numbers: remove common characters and whitespace
    if "phone_number" in df.columns:
        df["phone_number"] = df["phone_number"].astype(str).str.replace(r"[()\-\s]", "", regex=True)
        logger.info("Cleaned 'phone_number' column.")

    # Trim whitespace from all object (string) columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()
        
    logger.info("Data cleaning complete.")
    return df