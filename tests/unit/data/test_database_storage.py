# tests/unit/data/test_database_storage.py
import unittest
from unittest.mock import MagicMock, patch
import sqlalchemy as sa
from datetime import datetime
from src.data.storage.database_storage import DataHarvester

class TestDataHarvester(unittest.TestCase):
    
    def setUp(self):
        # Mock data_feed
        self.data_feed = MagicMock()
        
        # Use in-memory SQLite for testing
        with patch('sqlalchemy.create_engine') as mock_create_engine:
            # Mock the engine and connection
            self.mock_engine = MagicMock()
            self.mock_conn = MagicMock()
            self.mock_engine.connect.return_value = self.mock_conn
            self.mock_conn.begin.return_value = MagicMock()
            mock_create_engine.return_value = self.mock_engine
            
            # Create harvester instance
            self.harvester = DataHarvester(self.data_feed, db_path="sqlite:///:memory:")
            
            # Mock the metadata
            self.harvester.price_data = MagicMock()
            self.harvester.symbols_meta = MagicMock()
            self.harvester.harvest_log = MagicMock()
    
    def test_initialization(self):
        self.assertEqual(self.harvester.data_feed, self.data_feed)
        
    def test_parse_bar_timestamp(self):
        # Test parsing different timestamp formats
        
        # ISO format string
        iso_time = "2023-01-15T10:30:00"
        bar_iso = {'date': iso_time}
        timestamp_iso = self.harvester._parse_bar_timestamp(bar_iso)
        self.assertEqual(timestamp_iso.year, 2023)
        self.assertEqual(timestamp_iso.month, 1)
        self.assertEqual(timestamp_iso.day, 15)
        
        # YYYYMMDD format
        ymd_time = "20230115"
        bar_ymd = {'date': ymd_time}
        timestamp_ymd = self.harvester._parse_bar_timestamp(bar_ymd)
        self.assertEqual(timestamp_ymd.year, 2023)
        self.assertEqual(timestamp_ymd.month, 1)
        self.assertEqual(timestamp_ymd.day, 15)
        
        # Datetime object
        dt_time = datetime(2023, 1, 15, 10, 30, 0)
        bar_dt = {'date': dt_time}
        timestamp_dt = self.harvester._parse_bar_timestamp(bar_dt)
        self.assertEqual(timestamp_dt, dt_time)
    
    def test_calculate_data_quality(self):
        # High quality bar
        good_bar = {'open': 100.0, 'high': 105.0, 'low': 95.0, 'close': 102.0}
        good_quality = self.harvester._calculate_data_quality(good_bar)
        self.assertAlmostEqual(good_quality, 1.0)
        
        # Low quality bar with inconsistent values
        bad_bar = {'open': 100.0, 'high': 95.0, 'low': 105.0, 'close': 102.0}  # High < Low
        bad_quality = self.harvester._calculate_data_quality(bad_bar)
        self.assertLess(bad_quality, 0.7)  # Quality should be significantly reduced
        
        # Missing values bar
        missing_bar = {'open': 100.0, 'high': None, 'low': 95.0, 'close': 102.0}
        missing_quality = self.harvester._calculate_data_quality(missing_bar)
        self.assertLess(missing_quality, 0.8)  # Quality should be reduced
    
    @patch('src.data.storage.database_storage.DataHarvester._store_bars')
    def test_harvest_data(self, mock_store_bars):
        # Mock data feed response
        mock_bars = [
            {'date': '20230115', 'open': 100.0, 'high': 105.0, 'low': 95.0, 'close': 102.0, 'volume': 1000},
            {'date': '20230116', 'open': 102.0, 'high': 107.0, 'low': 97.0, 'close': 104.0, 'volume': 1200}
        ]
        
        self.data_feed.request_historical_data.return_value = 123
        self.data_feed.get_historical_data.return_value = mock_bars
        
        # Mock store_bars return value
        mock_store_bars.return_value = {
            "processed": 2, 
            "added": 2, 
            "updated": 0, 
            "errors": 0, 
            "status": "success"
        }
        
        # Test the harvest_data method
        result = self.harvester.harvest_data("AAPL", "1 D", "1 day", "TRADES")
        
        # Verify method calls
        self.data_feed.request_historical_data.assert_called_once_with(
            symbol="AAPL",
            duration="1 D",
            bar_size="1 day",
            what_to_show="TRADES"
        )
        self.data_feed.get_historical_data.assert_called_once_with(123)
        mock_store_bars.assert_called_once_with("AAPL", "1 day", "TRADES", mock_bars)
        
        # Check result
        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["added"], 2)
        self.assertEqual(result["status"], "success")

if __name__ == '__main__':
    unittest.main()