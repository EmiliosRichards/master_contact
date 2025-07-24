import logging
from pathlib import Path
import pandas as pd
from typing import List

logger = logging.getLogger(__name__)

def find_files(source_directory: str) -> List[Path]:
    """
    Finds all CSV and XLSX files in the specified source directory.

    Args:
        source_directory (str): The path to the directory containing source files.

    Returns:
        List[Path]: A list of Path objects for each found file.
    """
    source_path = Path(source_directory)
    if not source_path.is_dir():
        logger.error(f"Source directory not found: {source_directory}")
        return []

    csv_files = list(source_path.glob("*.csv"))
    xlsx_files = list(source_path.glob("*.xlsx"))
    all_files = csv_files + xlsx_files
    
    logger.info(f"Found {len(all_files)} files (CSV and XLSX) in {source_directory}.")
    return all_files


def extract_data(file_path: Path) -> pd.DataFrame:
    """
    Reads a single CSV or XLSX file into a pandas DataFrame.

    Args:
        file_path (Path): The path to the file.

    Returns:
        pd.DataFrame: The extracted data as a DataFrame, or an empty
                      DataFrame if an error occurs.
    """
    try:
        logger.info(f"Extracting data from {file_path.name}...")
        if file_path.suffix == '.csv':
            df = pd.read_csv(file_path)
        elif file_path.suffix == '.xlsx':
            df = pd.read_excel(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_path.suffix}. Skipping file.")
            return pd.DataFrame()
            
        logger.info(f"Successfully extracted {len(df)} rows from {file_path.name}.")
        return df
    except FileNotFoundError:
        logger.error(f"File not found during extraction: {file_path}")
    except Exception as e:
        logger.error(f"An error occurred during extraction from {file_path}: {e}")
    
    return pd.DataFrame()