import logging
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import text

# Add project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from etl.scripts.load import get_db_engine
from etl.scripts.utils import setup_logging

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

CREATE_PROFILES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS contact_profiles (
    id SERIAL PRIMARY KEY,
    profile_hash TEXT UNIQUE NOT NULL,
    json_keys TEXT[] NOT NULL,
    contact_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

ADD_PROFILE_ID_COLUMN_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'contacts' AND column_name = 'profile_id'
    ) THEN
        ALTER TABLE contacts ADD COLUMN profile_id INTEGER;
        ALTER TABLE contacts ADD CONSTRAINT fk_profile_id
            FOREIGN KEY (profile_id) REFERENCES contact_profiles(id);
    END IF;
END;
$$;
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
    Sets up the database by creating tables, columns, and constraints.
    """
    logger.info("Starting database setup...")
    load_dotenv()

    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            logger.info("Executing CREATE TABLE IF NOT EXISTS...")
            connection.execute(text(CREATE_TABLE_SQL))
            logger.info("Table 'contacts' ensured to exist.")

            logger.info("Executing CREATE TABLE IF NOT EXISTS for 'contact_profiles'...")
            connection.execute(text(CREATE_PROFILES_TABLE_SQL))
            logger.info("Table 'contact_profiles' ensured to exist.")

            logger.info("Executing ADD COLUMN IF NOT EXISTS for 'profile_id'...")
            connection.execute(text(ADD_PROFILE_ID_COLUMN_SQL))
            logger.info("Column 'profile_id' and its foreign key ensured to exist.")

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