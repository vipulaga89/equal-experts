# ingest.py
import duckdb
import json
from pathlib import Path
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

WAREHOUSE_PATH = "warehouse.db"
DATA_PATH = Path("uncommitted/votes.jsonl")

def ingest_votes():
    con = None
    try:
        logging.info("Connecting to DuckDB...")
        con = duckdb.connect(WAREHOUSE_PATH)
        
        logging.info("Creating schema and table if they don't exist...")
        con.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis")
        con.execute("""
            CREATE TABLE IF NOT EXISTS blog_analysis.votes (
                Id INTEGER PRIMARY KEY,
                PostId INTEGER,
                VoteTypeId INTEGER,
                CreationDate TIMESTAMP
            )
        """)
        
        if not DATA_PATH.exists():
            logging.error("Dataset not found. Please run: poetry run exercise fetch-data")
            return
        
        logging.info(f"Ingesting data from: {DATA_PATH}")
        
        # Count before
        count_before = con.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
        
        # Load data from JSONL, perform type conversion, and insert
        # only if ID doesn't exist (using primary key constraint)
        rows_affected = con.execute(f"""
            INSERT INTO blog_analysis.votes
            SELECT Id::INTEGER, PostId::INTEGER, VoteTypeId::INTEGER, CreationDate::TIMESTAMP
            FROM read_json_auto('{DATA_PATH}', format='newline_delimited')
            ON CONFLICT (Id) DO NOTHING
        """).fetchone()[0]
        
        logging.info(f"Inserted {rows_affected} new records.")
        print(f"Ingested {rows_affected} new vote records.")
        
    except Exception as e:
        logging.error(f"Error during ingestion: {e}")
        
    finally:
        if con:
            con.close()
            logging.info("Connection closed.")

if __name__ == "__main__":
    ingest_votes()