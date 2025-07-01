import logging
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

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

def load_to_db(df: pd.DataFrame, table_name: str, engine: Engine):
    """
    Loads a DataFrame into a specified database table.

    Args:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): The name of the target table.
        engine (Engine): The SQLAlchemy database engine.
    """
    if df.empty:
        logger.info("DataFrame is empty. Nothing to load to the database.")
        return

    try:
        logger.info(f"Loading {len(df)} rows into '{table_name}' table...")
        # Using 'append' to add new records. The deduplication logic should
        # have already filtered out existing records.
        df.to_sql(table_name, engine, if_exists="append", index=False)
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