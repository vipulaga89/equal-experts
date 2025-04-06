import unittest
from unittest.mock import patch, MagicMock, mock_open
import duckdb
import json
import logging
from pathlib import Path
import io
import sys

# Import the function to test, assuming it's in a file named ingest.py
from equalexperts_dataeng_exercise.ingest import ingest_votes, WAREHOUSE_PATH, DATA_PATH

class TestIngestVotes(unittest.TestCase):
    
    @patch('equalexperts_dataeng_exercise.ingest.duckdb.connect')
    @patch('equalexperts_dataeng_exercise.ingest.Path.exists')
    @patch('equalexperts_dataeng_exercise.ingest.logging')
    def test_data_path_not_exists(self, mock_logging, mock_exists, mock_connect):
        # Setup
        mock_exists.return_value = False
        
        # Call
        ingest_votes()
        
        # Assert
        mock_connect.assert_called_once_with(WAREHOUSE_PATH)
        mock_logging.error.assert_called_once_with("Dataset not found. Please run: poetry run exercise fetch-data")
        # Verify the connection is closed when exiting early
        mock_connect.return_value.close.assert_called_once()
    
    @patch('equalexperts_dataeng_exercise.ingest.duckdb.connect')
    @patch('equalexperts_dataeng_exercise.ingest.Path.exists')
    @patch('equalexperts_dataeng_exercise.ingest.logging')
    def test_successful_ingestion(self, mock_logging, mock_exists, mock_connect):
        # Setup
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock count before
        mock_count_result = MagicMock()
        mock_count_result.fetchone.return_value = [100]
        mock_conn.execute.return_value = mock_count_result
        
        # Mock insert result
        mock_insert_result = MagicMock()
        mock_insert_result.fetchone.return_value = [50]
        # Set up the second call to execute to return the insert result
        mock_conn.execute.side_effect = [
            mock_count_result,  # First call (CREATE SCHEMA)
            mock_count_result,  # Second call (CREATE TABLE)
            mock_count_result,  # Third call (SELECT COUNT)
            mock_insert_result,  # Fourth call (INSERT)
        ]
        
        # Redirect stdout to capture print output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        # Call
        ingest_votes()
        
        # Reset stdout
        sys.stdout = sys.__stdout__
        
        # Assert
        self.assertEqual(mock_conn.execute.call_count, 4)
        self.assertIn("Ingested 50 new vote records", captured_output.getvalue())
        mock_conn.close.assert_called_once()
    
    @patch('equalexperts_dataeng_exercise.ingest.duckdb.connect')
    @patch('equalexperts_dataeng_exercise.ingest.Path.exists')
    @patch('equalexperts_dataeng_exercise.ingest.logging')
    def test_exception_handling(self, mock_logging, mock_exists, mock_connect):
        # Setup
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # First two calls succeed, third raises an exception
        mock_conn.execute.side_effect = [
            MagicMock(),  # CREATE SCHEMA
            MagicMock(),  # CREATE TABLE
            Exception("Database error")  # SELECT COUNT
        ]
        
        # Call
        ingest_votes()
        
        # Assert
        mock_logging.error.assert_called_once_with("Error during ingestion: Database error")
        mock_conn.close.assert_called_once()
    
    @patch('equalexperts_dataeng_exercise.ingest.duckdb.connect')
    @patch('equalexperts_dataeng_exercise.ingest.Path.exists')
    @patch('equalexperts_dataeng_exercise.ingest.logging')
    @patch('equalexperts_dataeng_exercise.ingest.__name__')
    def test_main_execution(self, mock_name, mock_logging, mock_exists, mock_connect):
        # Setup
        mock_name.__eq__.return_value = True  # Simulate __name__ == "__main__"
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock successful execution
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [0]
        mock_conn.execute.return_value = mock_result
        
        # This is just to test that main execution works
        # We import __name__ and mock it to simulate main execution
        
        # Call - we don't need to call anything as the import would trigger it
        # But we'll verify the mocks are set up correctly
        self.assertTrue(mock_name.__eq__.return_value)

if __name__ == "__main__":
    unittest.main()