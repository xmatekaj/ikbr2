# src/data/harvester_manager.py
"""
Manager for the data harvesting system using centralized configuration.
"""
import logging
import threading
import time
from datetime import datetime

from .harvester_client import HarvesterClient
from src.config.config_manager import get_config

logger = logging.getLogger(__name__)

class HarvesterManager:
    """Manages data harvesting operations using centralized configuration."""
    
    def __init__(self):
        """Initialize the harvester manager."""
        self.config_manager = get_config()
        self.harvester_client = None
        self.started = False
    
    def _get_harvester_config(self):
        """Get harvester configuration from centralized config."""
        return self.config_manager.get_section('data_harvesting')
    
    def start(self):
        """Start the harvester manager."""
        if self.started:
            logger.warning("Harvester manager already started")
            return True
            
        config = self._get_harvester_config()
        
        if not config.get("enabled", True):
            logger.info("Data harvesting is disabled in configuration")
            return False
        
        try:
            # Get database configuration
            db_connection = self.config_manager.get_database_connection()
            if not db_connection:
                logger.error("Database not enabled or configured for harvesting")
                return False
            
            if not db_connection.startswith('postgresql://'):
                logger.error("TimescaleDB requires a PostgreSQL connection string")
                return False
            
            # Get IBKR connection info for harvester
            ibkr_config = self.config_manager.get_section('ibkr')
            client_ids = ibkr_config.get('client_ids', {})
            harvester_client_id = client_ids.get('harvester', 10)
            
            # Use the same connection mode as the main bot
            conn_info = self.config_manager.get_ibkr_connection_info()
            
            # Initialize harvester client
            self.harvester_client = HarvesterClient.get_instance(
                host=conn_info['host'],
                port=conn_info['port'],
                client_id=harvester_client_id,
                db_path=db_connection
            )
            
            # Start the client
            if not self.harvester_client.start():
                logger.error("Failed to start harvester client")
                return False
            
            # Start scheduled harvesting
            symbols = config.get('symbols', [])
            timeframes = config.get('timeframes', [])
            interval_hours = config.get('schedule_interval_hours', 24)
            
            self.harvester_client.start_scheduled_harvesting(
                symbols=symbols,
                timeframes=timeframes,
                interval_hours=interval_hours
            )
            
            self.started = True
            logger.info(f"Harvester manager started for {len(symbols)} symbols")
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
    
    def get_status(self):
        """Get the status of the harvester manager."""
        config = self._get_harvester_config()
        
        return {
            'enabled': config.get('enabled', False),
            'started': self.started,
            'symbols': config.get('symbols', []),
            'timeframes': config.get('timeframes', []),
            'interval_hours': config.get('schedule_interval_hours', 24),
            'database_connected': self.config_manager.get_database_connection() is not None
        }
    
    def update_config(self, new_config):
        """Update harvester configuration in the centralized config."""
        # Validate the configuration
        if 'symbols' in new_config and not isinstance(new_config['symbols'], list):
            raise ValueError("'symbols' must be a list")
        
        if 'timeframes' in new_config and not isinstance(new_config['timeframes'], list):
            raise ValueError("'timeframes' must be a list")
        
        # Update the configuration
        for key, value in new_config.items():
            self.config_manager.update_config(f'data_harvesting.{key}', value)
        
        # Save the configuration
        self.config_manager.save_config()
        
        # If running, restart with new config
        if self.started:
            logger.info("Restarting harvester with new configuration")
            self.stop()
            self.start()
        
        logger.info("Harvester configuration updated")