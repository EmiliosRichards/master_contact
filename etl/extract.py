import logging
from pathlib import Path
import pandas as pd
from typing import List

logger = logging.getLogger(__name__)

def find_csv_files(source_directory: str) -> List[Path]:
    """
    Finds all CSV files in the specified source directory.

    Args:
        source_directory (str): The path to the directory containing source CSVs.

    Returns:
        List[Path]: A list of Path objects for each found CSV file.
    """
    source_path = Path(source_directory)
    if not source_path.is_dir():
        logger.error(f"Source directory not found: {source_directory}")
        return []

    csv_files = list(source_path.glob("*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files in {source_directory}.")
    return csv_files


def extract_from_csv(file_path: Path) -> pd.DataFrame:
    """
    Reads a single CSV file into a pandas DataFrame.

    Args:
        file_path (Path): The path to the CSV file.

    Returns:
        pd.DataFrame: The extracted data as a DataFrame, or an empty
                      DataFrame if an error occurs.
    """
    try:
        logger.info(f"Extracting data from {file_path.name}...")
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} rows from {file_path.name}.")
        return df
    except FileNotFoundError:
        logger.error(f"File not found during extraction: {file_path}")
    except Exception as e:
        logger.error(f"An error occurred during extraction from {file_path}: {e}")
    
    return pd.DataFrame()