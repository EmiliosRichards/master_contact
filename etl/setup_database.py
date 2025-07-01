import logging
from dotenv import load_dotenv
from sqlalchemy import text

from etl.load import get_db_engine
from etl.utils import setup_logging

# Initialize logger
setup_logging("etl/logs/setup.log")
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    company_name TEXT NOT NULL,
    url TEXT,
    phone_number TEXT,
    is_b2b BOOLEAN,
    industry TEXT,
    customer_target_segments TEXT,
    additional_info JSONB,
    tags TEXT[],
    status TEXT DEFAULT 'active',
    last_used TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
"""

ADD_CONSTRAINT_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'unique_phone_number'
    ) THEN
        ALTER TABLE contacts ADD CONSTRAINT unique_phone_number UNIQUE (phone_number);
    END IF;
END;
$$;
"""

def setup_database():
    """
    Sets up the database by creating the contacts table and adding constraints.
    """
    logger.info("Starting database setup...")
    load_dotenv()

    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            logger.info("Executing CREATE TABLE IF NOT EXISTS...")
            connection.execute(text(CREATE_TABLE_SQL))
            logger.info("Table 'contacts' ensured to exist.")

            logger.info("Executing ADD CONSTRAINT IF NOT EXISTS...")
            connection.execute(text(ADD_CONSTRAINT_SQL))
            logger.info("Constraint 'unique_phone_number' ensured to exist.")
            
            # Commit the transaction
            connection.commit()

        logger.info("Database setup completed successfully.")

    except Exception as e:
        logger.critical(f"An error occurred during database setup: {e}")
        raise

if __name__ == "__main__":
    setup_database()