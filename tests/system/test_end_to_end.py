# tests/system/test_end_to_end.py
import unittest
import os
import tempfile
import sqlite3
import time
from unittest.mock import MagicMock, patch
from src.connectors.ibkr.client import IBKRClient
from src.core.bot_manager import BotManager
from src.config.settings import TradingConfig

class TestEndToEnd(unittest.TestCase):
    
    def setUp(self):
        # Create a temporary database for testing
        self.temp_db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False).name
        self.temp_config_file = tempfile.NamedTemporaryFile(suffix='.json', delete=False).name
        
        # Create mock for IBKR client
        self.patcher = patch('src.connectors.ibkr.client.IBKRClient')
        self.mock_client_class = self.patcher.start()
        self.mock_client = self.mock_client_class.return_value
        self.mock_client.connected = True
        
        # Create sample config
        self.sample_config = {
            "global": {},
            "bots": [
                {
                    "id": "test_bot_1",
                    "ibkr_host": "127.0.0.1",
                    "ibkr_port": 7497,
                    "ibkr_client_id": 1,
                    "engine_loop_interval": 1.0,
                    "paper_trading": True,
                    "max_positions": 5,
                    "max_risk_per_trade": 0.02,
                    "initial_capital": 100000.0,
                    "strategies": [
                        {
                            "id": "momentum_1",
                            "type": "MomentumStrategy",
                            "params": {
                                "symbols": ["AAPL", "MSFT", "GOOGL"],
                                "lookback_period": 20,
                                "momentum_threshold": 0.02
                            }
                        }
                    ]
                }
            ]
        }
        
        # Write config to temp file
        import json
        with open(self.temp_config_file, 'w') as f:
            json.dump(self.sample_config, f)
        
        # Set up database path
        self.db_path = f"sqlite:///{self.temp_db_file}"
        
        # Mock dependency injection for IBKRClient
        self.client_patch = patch('src.connectors.ibkr.client.IBKRClient', return_value=self.mock_client)
        self.client_patch.start()
    
    def tearDown(self):
        # Clean up temporary files
        try:
            os.unlink(self.temp_db_file)
            os.unlink(self.temp_config_file)
        except:
            pass
        
        # Stop all patches
        self.patcher.stop()
        self.client_patch.stop()
    
    @patch('src.core.bot_manager.BotManager._create_bot_from_config')
    @patch('src.core.bot_manager.TradingEngine')
    def test_bot_creation_and_startup(self, mock_engine_class, mock_create_bot):
        """Test creating a bot and starting it"""
        # Mock implementations
        mock_engine = mock_engine_class.return_value
        mock_engine.start.return_value = True
        mock_create_bot.return_value = True
        
        # Create bot manager with config
        bot_manager = BotManager(config_path=self.temp_config_file)
        
        # Create a bot manually (since we mocked _create_bot_from_config)
        config = TradingConfig()
        bot_id = bot_manager.create_bot("test_bot", config)
        
        # Start the bot
        result = bot_manager.start_bot(bot_id)
        self.assertTrue(result, "Bot should start successfully")
        
        # Verify engine.start was called
        mock_engine.start.assert_called_once()
        
        # Stop the bot
        stop_result = bot_manager.stop_bot(bot_id)
        self.assertTrue(stop_result, "Bot should stop successfully")
    
    @patch('src.data.harvester_client.HarvesterClient.get_instance')
    def test_data_harvesting_workflow(self, mock_get_instance):
        """Test the data harvesting workflow"""
        from src.data.harvester_manager import HarvesterManager
        
        # Mock harvester client
        mock_harvester = MagicMock()
        mock_get_instance.return_value = mock_harvester
        mock_harvester.start.return_value = True
        mock_harvester.start_scheduled_harvesting.return_value = True
        
        # Create harvester manager
        harvester_manager = HarvesterManager(config_path=None)
        
        # Override config with test values
        harvester_manager.config = {
            "enabled": True,
            "client_id": 10,
            "db_path": self.db_path,
            "schedule_interval_hours": 24,
            "symbols": ["AAPL", "MSFT"],
            "timeframes": [
                {"duration": "1 D", "bar_size": "1 hour"}
            ]
        }
        
        # Start harvester
        result = harvester_manager.start()
        self.assertTrue(result, "Harvester should start successfully")
        
        # Verify client methods were called
        mock_harvester.start.assert_called_once()
        mock_harvester.start_scheduled_harvesting.assert_called_once_with(
            symbols=["AAPL", "MSFT"],
            timeframes=[{"duration": "1 D", "bar_size": "1 hour"}],
            interval_hours=24
        )
        
        # Stop harvester
        stop_result = harvester_manager.stop()
        self.assertTrue(stop_result, "Harvester should stop successfully")
        mock_harvester.stop.assert_called_once()

if __name__ == '__main__':
    unittest.main()