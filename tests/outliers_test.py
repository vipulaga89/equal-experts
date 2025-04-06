"""
Test cases for the outlier detection functionality.
"""
import os
import json
from pathlib import Path
import tempfile
import pytest
import duckdb

from equalexperts_dataeng_exercise.outliers import calculate_outliers


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    # Use NamedTemporaryFile just to get a unique file path, but close it immediately
    # so DuckDB can create a fresh database file
    with tempfile.NamedTemporaryFile(suffix='.db') as temp_file:
        db_path = temp_file.name
    
    # Now DuckDB will create a fresh database file at this path
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blog_analysis.votes (
            Id INTEGER PRIMARY KEY,
            PostId INTEGER,
            VoteTypeId INTEGER,
            CreationDate TIMESTAMP
        )
    """)
    conn.close()
    
    yield db_path
    
    # Clean up the database file after the test
    if os.path.exists(db_path):
        os.remove(db_path)


@pytest.fixture
def sample_data():
    """Return sample data for testing."""
    return [
        {"Id": 1, "PostId": 1, "VoteTypeId": 2, "CreationDate": "2022-01-02T00:00:00.000"},
        {"Id": 2, "PostId": 1, "VoteTypeId": 2, "CreationDate": "2022-01-09T00:00:00.000"},
        {"Id": 4, "PostId": 1, "VoteTypeId": 2, "CreationDate": "2022-01-09T00:00:00.000"},
        {"Id": 5, "PostId": 1, "VoteTypeId": 2, "CreationDate": "2022-01-09T00:00:00.000"},
        {"Id": 6, "PostId": 5, "VoteTypeId": 3, "CreationDate": "2022-01-16T00:00:00.000"},
        {"Id": 7, "PostId": 3, "VoteTypeId": 2, "CreationDate": "2022-01-16T00:00:00.000"},
        {"Id": 8, "PostId": 4, "VoteTypeId": 2, "CreationDate": "2022-01-16T00:00:00.000"},
        {"Id": 9, "PostId": 2, "VoteTypeId": 2, "CreationDate": "2022-01-23T00:00:00.000"},
        {"Id": 10, "PostId": 2, "VoteTypeId": 2, "CreationDate": "2022-01-23T00:00:00.000"},
        {"Id": 11, "PostId": 1, "VoteTypeId": 2, "CreationDate": "2022-01-30T00:00:00.000"},
        {"Id": 12, "PostId": 5, "VoteTypeId": 2, "CreationDate": "2022-01-30T00:00:00.000"},
        {"Id": 13, "PostId": 8, "VoteTypeId": 2, "CreationDate": "2022-02-06T00:00:00.000"},
        {"Id": 14, "PostId": 13, "VoteTypeId": 3, "CreationDate": "2022-02-13T00:00:00.000"},
        {"Id": 15, "PostId": 13, "VoteTypeId": 3, "CreationDate": "2022-02-20T00:00:00.000"},
        {"Id": 16, "PostId": 11, "VoteTypeId": 2, "CreationDate": "2022-02-20T00:00:00.000"},
        {"Id": 17, "PostId": 3, "VoteTypeId": 3, "CreationDate": "2022-02-27T00:00:00.000"}
    ]


def insert_test_data(conn, data):
    """Insert test data into the database."""
    for item in data:
        conn.execute("""
            INSERT INTO blog_analysis.votes (Id, PostId, VoteTypeId, CreationDate)
            VALUES (?, ?, ?, ?)
        """, [item["Id"], item["PostId"], item["VoteTypeId"], item["CreationDate"]])


def test_view_creation(temp_db, sample_data, monkeypatch):
    """Test that the outlier_weeks view is created correctly."""
    # Setup by inserting test data
    conn = duckdb.connect(temp_db)
    insert_test_data(conn, sample_data)
    
    # Store the original connect function
    original_connect = duckdb.connect
    
    # Create a wrapper function that only redirects for specific paths
    def mock_connect_wrapper(*args, **kwargs):
        if len(args) > 0 and args[0] == 'warehouse.db':
            return original_connect(temp_db)
        return original_connect(*args, **kwargs)
    
    # Mock the warehouse.db path
    monkeypatch.setenv("WAREHOUSE_PATH", temp_db)
    monkeypatch.setattr('equalexperts_dataeng_exercise.outliers.duckdb.connect', mock_connect_wrapper)
    
    # Run the outlier detection
    calculate_outliers(con=conn)
    
    # Check if view exists
    result = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type='VIEW' AND table_name='outlier_weeks' AND table_schema='blog_analysis'
    """).fetchall()
    
    assert len(result) == 1, "Expected view 'outlier_weeks' to exist"
    conn.close()

def test_empty_data(temp_db):
    """Test that the outlier_weeks view handles empty data gracefully."""
    # Create the connection up front
    conn = duckdb.connect(temp_db)
    
    # Create schema and table but insert no data
    conn.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blog_analysis.votes (
            Id INTEGER PRIMARY KEY,
            PostId INTEGER,
            VoteTypeId INTEGER,
            CreationDate TIMESTAMP
        )
    """)
    
    # Run the outlier detection with an empty table
    calculate_outliers(con=conn)
    
    # Check that the view exists but has no rows
    result = conn.execute("SELECT COUNT(*) FROM blog_analysis.outlier_weeks").fetchone()[0]
    
    assert result == 0, "Expected 0 rows in outlier_weeks view with empty data"
    conn.close()


def test_connection_error(monkeypatch):
    """Test handling of database connection errors."""
    # Mock the connection to fail
    def mock_connect(*args, **kwargs):
        raise Exception("Connection failed")
    
    monkeypatch.setattr('equalexperts_dataeng_exercise.outliers.duckdb.connect', mock_connect)
    
    # Expect an exception when the connection fails
    with pytest.raises(Exception):
        calculate_outliers(con=conn)


def test_formula_correctness(temp_db, monkeypatch):
    """Test the correctness of the outlier detection formula."""
    # Setup a controlled dataset
    conn = duckdb.connect(temp_db)
    
    # Create a dataset with a known average and some values that are outliers
    # Average = 10, outliers (by our formula) would be < 8 or > 12
    test_data = [
        {"Id": 1, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},  # Week 0, 10 votes
        {"Id": 2, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 3, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 4, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 5, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 6, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 7, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 8, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 9, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        {"Id": 10, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},
        
        {"Id": 11, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},  # Week 1, 7 votes (outlier)
        {"Id": 12, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},
        {"Id": 13, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},
        {"Id": 14, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},
        {"Id": 15, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},
        {"Id": 16, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},
        {"Id": 17, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},
        
        {"Id": 18, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},  # Week 2, 10 votes
        {"Id": 19, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 20, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 21, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 22, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 23, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 24, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 25, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 26, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        {"Id": 27, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-15T00:00:00.000"},
        
        {"Id": 28, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},  # Week 3, 13 votes (outlier)
        {"Id": 29, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 30, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 31, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 32, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 33, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 34, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 35, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 36, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 37, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 38, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 39, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
        {"Id": 40, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},
    ]
    
    insert_test_data(conn, test_data)
    
    # Mock the warehouse.db path
    monkeypatch.setenv("WAREHOUSE_PATH", temp_db)
    monkeypatch.setattr('equalexperts_dataeng_exercise.outliers.duckdb.connect', 
                       lambda *args, **kwargs: duckdb.connect(temp_db))
    
    # Run the outlier detection
    calculate_outliers(con=conn)
    
    # Check that only weeks 1 and 3 are identified as outliers
    result = conn.execute("""
        SELECT Year, WeekNumber, VoteCount 
        FROM blog_analysis.outlier_weeks 
        ORDER BY Year, WeekNumber
    """).fetchall()
    
    assert len(result) == 2, f"Expected 2 outliers, got {len(result)}"
    
    # Check week 1 (7 votes)
    assert result[0][1] == 1, "Expected week 1 to be an outlier"
    assert result[0][2] == 7, "Expected week 1 to have 7 votes"
    
    # Check week 3 (13 votes)
    assert result[1][1] == 3, "Expected week 3 to be an outlier"
    assert result[1][2] == 13, "Expected week 3 to have 13 votes"
    
    conn.close()


def test_sorted_output(temp_db, monkeypatch):
    """Test that the outlier_weeks view output is sorted correctly."""
    # Setup test data with different years
    conn = duckdb.connect(temp_db)
    
    test_data = [
        {"Id": 1, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2023-01-15T00:00:00.000"},  # Year 2023, Week 2
        {"Id": 2, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-22T00:00:00.000"},  # Year 2022, Week 3
        {"Id": 3, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2023-01-08T00:00:00.000"},  # Year 2023, Week 1
        {"Id": 4, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-01T00:00:00.000"},  # Year 2022, Week 0
        {"Id": 5, "PostId": 1, "VoteTypeId": 1, "CreationDate": "2022-01-08T00:00:00.000"},  # Year 2022, Week 1
    ]
    
    insert_test_data(conn, test_data)
    
    # Mock the warehouse.db path
    monkeypatch.setenv("WAREHOUSE_PATH", temp_db)
    monkeypatch.setattr('equalexperts_dataeng_exercise.outliers.duckdb.connect', 
                       lambda *args, **kwargs: duckdb.connect(temp_db))
    
    # Run the outlier detection
    calculate_outliers(con=conn)
    
    # Check that the results are sorted by year and week number
    result = conn.execute("SELECT Year, WeekNumber FROM blog_analysis.outlier_weeks").fetchall()
    
    # Check that results are in ascending order by year, then by week number
    for i in range(1, len(result)):
        current_year, current_week = result[i]
        prev_year, prev_week = result[i-1]
        
        # Either current year is greater than previous year,
        # or current year equals previous year and current week is greater than previous week
        assert (current_year > prev_year) or (current_year == prev_year and current_week > prev_week), \
               f"Results not properly sorted at position {i}: {result[i-1]} -> {result[i]}"
    
    conn.close()


def test_calculate_outliers_mock(monkeypatch):
    """Test the calculate_outliers function with a mocked database execution."""
    # Create a mock for the database connection and execution
    class MockConnection:
        def __init__(self):
            self.executed_queries = []
            self.closed = False
            
        def execute(self, query, *args, **kwargs):
            self.executed_queries.append(query)
            if "SELECT COUNT(*)" in query:
                class MockResult:
                    def fetchone(self):
                        return [6]  # Return 6 outliers
                return MockResult()
            return self
            
        def close(self):
            self.closed = True
    
    mock_conn = MockConnection()
    
    # Mock the duckdb.connect function to return our mock
    monkeypatch.setattr('equalexperts_dataeng_exercise.outliers.duckdb.connect', 
                       lambda *args, **kwargs: mock_conn)
    
    # Call the function
    calculate_outliers()
    
    # Check that the right queries were executed
    assert any("CREATE OR REPLACE VIEW blog_analysis.outlier_weeks" in query for query in mock_conn.executed_queries), \
           "Expected view creation query"
    
    assert any("SELECT COUNT(*) FROM blog_analysis.outlier_weeks" in query for query in mock_conn.executed_queries), \
           "Expected count query on the view"
    
    # Check that the connection was closed
    assert mock_conn.closed, "Expected database connection to be closed"
    
def test_execution_error(temp_db, monkeypatch, caplog):
    """Test handling of SQL execution errors."""
    conn = duckdb.connect(temp_db)
    
    # Create a class to serve as a proxy for the connection
    class ExecuteErrorConnection:
        def __init__(self, real_conn):
            self.real_conn = real_conn
            self.closed = False
        
        def execute(self, query, *args, **kwargs):
            if "CREATE OR REPLACE VIEW" in query:
                raise Exception("SQL execution failed")
            return self.real_conn.execute(query, *args, **kwargs)
        
        def close(self):
            self.closed = True
    
    # Create our proxy connection
    proxy_conn = ExecuteErrorConnection(conn)
    
    # Test that the exception is properly caught and re-raised
    with pytest.raises(Exception):
        calculate_outliers(con=proxy_conn)
    
    # Check for the error being logged (to cover the logging.error line)
    assert "Error creating outlier_weeks view" in caplog.text
    
    # Verify our proxy connection wasn't closed since we provided it externally
    assert not proxy_conn.closed, "Externally provided connection should not be closed by the function"
    
    # Clean up the real connection
    conn.close()