import logging
import os
from pathlib import Path
import hashlib
from typing import List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB

logger = logging.getLogger(__name__)

def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy database engine using the DATABASE_URL
    from the environment variables.

    Returns:
        Engine: The SQLAlchemy database engine.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable not set.")
        raise ValueError("DATABASE_URL is not configured.")
    
    try:
        engine = create_engine(database_url)
        logger.info("Database engine created successfully.")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise

def get_or_create_profile_id(json_keys: List[str], engine: Engine) -> Optional[int]:
    """
    Finds an existing profile or creates a new one based on the JSON keys.
    Returns the profile ID.
    """
    if not json_keys:
        return None

    # Sort keys to ensure hash is consistent
    sorted_keys = sorted(json_keys)
    
    # Create a stable hash
    m = hashlib.md5()
    m.update(str(sorted_keys).encode('utf-8'))
    profile_hash = m.hexdigest()

    with engine.connect() as connection:
        # Check if profile exists
        result = connection.execute(
            text("SELECT id FROM contact_profiles WHERE profile_hash = :hash"),
            {"hash": profile_hash}
        ).fetchone()

        if result:
            profile_id = result[0]
            # Increment the count for this profile
            connection.execute(
                text("UPDATE contact_profiles SET contact_count = contact_count + 1 WHERE id = :id"),
                {"id": profile_id}
            )
            connection.commit()  # Commit the transaction
            logger.info(f"Found existing profile_id: {profile_id} for hash: {profile_hash}")
            return profile_id
        else:
            # Create new profile
            insert_result = connection.execute(
                text("INSERT INTO contact_profiles (profile_hash, json_keys) VALUES (:hash, :keys) RETURNING id"),
                {"hash": profile_hash, "keys": sorted_keys}
            ).fetchone()
            
            if insert_result:
                profile_id = insert_result[0]
                connection.commit()  # Commit the transaction
                logger.info(f"Created new profile_id: {profile_id} for hash: {profile_hash}")
                return profile_id
            else:
                logger.error("Failed to create a new profile, insert operation returned no ID.")
                return None

def load_to_db(df: pd.DataFrame, table_name: str, engine: Engine, json_keys: List[str]):
    """
    Loads a DataFrame into a specified database table after assigning a profile ID.

    Args:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): The name of the target table.
        engine (Engine): The SQLAlchemy database engine.
        json_keys (List[str]): The list of keys in the additional_info JSON.
    """
    if df.empty:
        logger.info("DataFrame is empty. Nothing to load to the database.")
        return

    try:
        # Get the profile ID for this batch of data
        profile_id = get_or_create_profile_id(json_keys, engine)
        df['profile_id'] = profile_id

        logger.info(f"Loading {len(df)} rows with profile_id {profile_id} into '{table_name}' table...")
        
        # Ensure 'profile_id' is of a type that can handle None (e.g., float for pandas)
        if 'profile_id' in df.columns:
            df['profile_id'] = df['profile_id'].astype('Int64') # Use nullable integer

        # Using 'append' to add new records.
        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            dtype={'additional_info': JSONB}
        )
        logger.info(f"Successfully loaded data into '{table_name}'.")
    except Exception as e:
        logger.error(f"Failed to load data into '{table_name}': {e}")
        # Re-raise the exception to be handled by the main orchestrator
        raise

def move_processed_file(file_path: Path, processed_directory: str):
    """
    Moves a file to the processed directory.

    Args:
        file_path (Path): The path of the file to move.
        processed_directory (str): The destination directory.
    """
    try:
        processed_path = Path(processed_directory)
        processed_path.mkdir(parents=True, exist_ok=True)
        
        destination = processed_path / file_path.name
        file_path.rename(destination)
        logger.info(f"Moved processed file to: {destination}")
    except Exception as e:
        logger.error(f"Failed to move processed file {file_path.name}: {e}")