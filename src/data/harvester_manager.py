# src/data/harvester_manager.py
"""
Manager for the data harvesting system.
"""
import logging
import threading
import time
import json
import os
from datetime import datetime

from .harvester_client import HarvesterClient
from src.config.settings import default_settings

logger = logging.getLogger(__name__)

class HarvesterManager:
    """Manages data harvesting operations."""
    
    def __init__(self, config_path="config/harvester_config.json"):
        """
        Initialize the harvester manager.
        
        Args:
            config_path: Path to harvester configuration
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.harvester_client = None
        self.started = False
    
    def _load_config(self):
        """Load harvester configuration."""
        default_config = {
            "enabled": True,
            "client_id": 10,  # Dedicated client ID for harvesting
            "db_path": "sqlite:///E:/historical_data/market_data.db",
            "schedule_interval_hours": 24,
            "symbols": ["SPY", "QQQ", "AAPL", "MSFT", "GOOGL"],
            "timeframes": [
                {"duration": "1 Y", "bar_size": "1 day", "what_to_show": "TRADES"},
                {"duration": "1 M", "bar_size": "1 hour", "what_to_show": "TRADES"},
                {"duration": "1 W", "bar_size": "5 mins", "what_to_show": "TRADES"}
            ]
        }
        
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    # Merge with defaults
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
        except Exception as e:
            logger.error(f"Error loading harvester config: {e}, using defaults")
        
        return default_config
    
    def _save_config(self):
        """Save current configuration."""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving harvester config: {e}")
    
    def start(self):
        """Start the harvester manager."""
        if self.started:
            logger.warning("Harvester manager already started")
            return True
            
        if not self.config.get("enabled", True):
            logger.info("Harvester is disabled in configuration")
            return False
        
        # Extract database path and ensure directory exists
        db_path = self.config.get('db_path', "sqlite:///historical_data.db")
        if db_path.startswith('sqlite:///'):
            file_path = db_path.replace('sqlite:///', '')
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
        try:
            # Initialize harvester client
            self.harvester_client = HarvesterClient.get_instance(
                host=default_settings.get('IBKR_HOST', '127.0.0.1'),
                port=default_settings.get('IBKR_PORT', 7497),
                client_id=self.config.get('client_id', 10),
                db_path=self.config.get('db_path', "sqlite:///historical_data.db")
            )
            
            # Start the client
            if not self.harvester_client.start():
                logger.error("Failed to start harvester client")
                return False
            
            # Start scheduled harvesting
            self.harvester_client.start_scheduled_harvesting(
                symbols=self.config.get('symbols', []),
                timeframes=self.config.get('timeframes', []),
                interval_hours=self.config.get('schedule_interval_hours', 24)
            )
            
            self.started = True
            logger.info("Harvester manager started")
            return True
            
        except Exception as e:
            logger.error(f"Error starting harvester manager: {e}")
            return False
    
    def stop(self):
        """Stop the harvester manager."""
        if not self.started:
            return True
            
        try:
            if self.harvester_client:
                self.harvester_client.stop()
            
            self.started = False
            logger.info("Harvester manager stopped")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping harvester manager: {e}")
            return False
    
    def update_config(self, new_config):
        """Update harvester configuration."""
        self.config.update(new_config)
        self._save_config()
        
        # If running, restart with new config
        if self.started:
            self.stop()
            self.start()