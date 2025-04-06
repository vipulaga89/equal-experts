# File: equalexperts_dataeng_exercise/__init__.py
"""
Equal Experts Data Engineering Exercise package.
"""

# File: equalexperts_dataeng_exercise/outliers.py
"""
Module for calculating outlier weeks based on vote data.
"""
import duckdb
import logging

def calculate_outliers(con=None):
    """
    Creates a view named 'outlier_weeks' in the 'blog_analysis' schema.
    The view contains weeks that are outliers based on vote count,
    where an outlier is defined as a week with vote count deviating
    from the average by more than 20%.
    """
    logging.basicConfig(level=logging.INFO)
    logging.info("Connecting to DuckDB...")
    own_connection = con is None 
    
    try:
        if own_connection:
            con = duckdb.connect("warehouse.db")
        
        logging.info("Creating outlier_weeks view...")
        
        # Create the view to detect outlier weeks
        con.execute("""
            CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
            WITH weekly_votes AS (
                -- Group votes by year and week number
                SELECT
                    EXTRACT(YEAR FROM CreationDate) AS Year,
                    EXTRACT(WEEK FROM CreationDate) AS WeekNumber,
                    COUNT(*) AS VoteCount
                FROM
                    blog_analysis.votes
                GROUP BY
                    EXTRACT(YEAR FROM CreationDate),
                    EXTRACT(WEEK FROM CreationDate)
            ),
            average_votes AS (
                -- Calculate the average votes per week across the entire dataset
                SELECT
                    AVG(VoteCount) AS MeanVotes
                FROM
                    weekly_votes
            )
            -- Select only the outlier weeks based on the formula
            SELECT
                Year,
                WeekNumber,
                VoteCount
            FROM
                weekly_votes
            CROSS JOIN
                average_votes
            WHERE
                ABS(1 - (VoteCount / MeanVotes)) > 0.2
            ORDER BY
                Year ASC,
                WeekNumber ASC;
        """)
        
        # Verify the view was created and contains data
        result = con.execute("SELECT COUNT(*) FROM blog_analysis.outlier_weeks").fetchone()[0]
        logging.info(f"Created outlier_weeks view with {result} rows")
        print(f"Created outlier_weeks view with {result} outlier weeks detected")
        
    except Exception as e:
        logging.error(f"Error creating outlier_weeks view: {e}")
        raise
    
    finally:
        if own_connection:
            con.close()
            logging.info("Connection closed.")

if __name__ == "__main__":
    calculate_outliers()

# File: equalexperts_dataeng_exercise/__main__.py
"""
Main entry point for the Equal Experts Data Engineering Exercise package.
"""
from equalexperts_dataeng_exercise.outliers import calculate_outliers

if __name__ == "__main__":
    calculate_outliers()