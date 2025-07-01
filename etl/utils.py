import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

def setup_logging(log_path: str):
    """
    Set up a standardized logger for the ETL pipeline.

    Args:
        log_path (str): The file path for the log file.
    """
    # Ensure the log directory exists
    log_dir = Path(log_path).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    # Root logger configuration
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Console handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(log_format)
    logger.addHandler(stdout_handler)

    # File handler (with rotation)
    file_handler = RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3  # 5 MB per file, 3 backups
    )
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    logging.info("Logging configured successfully.")
    return logger