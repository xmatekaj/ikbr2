# src/data/harvester_client.py
"""
Dedicated client for data harvesting operations.
"""
import logging
import threading
import time
from typing import Optional

from src.connectors.ibkr.client import IBKRClient
from src.connectors.ibkr.data_feed import IBKRDataFeed
from .storage.database_storage import DataHarvester

logger = logging.getLogger(__name__)

class HarvesterClient:
    """Manages a dedicated IBKR connection for data harvesting."""
    
    _instance = None  # Singleton instance
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls, **kwargs):
        """Get the singleton instance of the harvester client."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(**kwargs)
        return cls._instance
    
    def __init__(self, 
                host: str = "127.0.0.1", 
                port: int = 7497,
                client_id: int = 10,  # Use a dedicated client ID
                db_path: str = "postgresql://username:password@localhost:5432/market_data"):
        """
        Initialize the harvester client.
        
        Args:
            host: IBKR host
            port: IBKR port
            client_id: Client ID (use a unique ID different from trading)
            db_path: Database path
        """
        # Validate database connection string
        if not db_path.startswith('postgresql://'):
            raise ValueError("TimescaleDB requires a PostgreSQL connection string")
        self.client = IBKRClient(host=host, port=port, client_id=client_id)
        self.data_feed = None
        self.harvester = None
        self.db_path = db_path
        self.running = False
        self.harvesting_thread = None
        self.stop_event = threading.Event()
    
    def start(self):
        """
        Start the harvester client.
        
        Returns:
            True if started successfully
        """
        if self.running:
            logger.warning("Harvester client already running")
            return True
            
        try:
            logger.info("Starting harvester client")
            
            # Connect to IBKR
            self.client.connect_and_run()
            
            # Create data feed with connection parameters
            self.data_feed = IBKRDataFeed(
                host=self.client.host,
                port=self.client.port,
                client_id=self.client.client_id + 100  # Use client_id+100 to avoid conflict
            )
            
            # Connect the data feed
            self.data_feed.connect_and_run()
            
            # Wait briefly to ensure connection is established
            time.sleep(5)
            
            if not self.data_feed.connected:
                logger.error("Failed to connect data feed to IBKR")
                self.stop()
                return False
            
            # Create harvester
            self.harvester = DataHarvester(self.data_feed, self.db_path)
            
            self.running = True
            self.stop_event.clear()
            
            logger.info("Harvester client started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start harvester client: {e}")
            self.stop()
            return False

    def stop(self) -> bool:
        """
        Stop the harvester client.
        
        Returns:
            True if stopped successfully
        """
        if not self.running:
            return True
            
        try:
            logger.info("Stopping harvester client")
            
            # Signal harvesting to stop
            self.stop_event.set()
            
            # Wait for harvesting thread to finish
            if self.harvesting_thread and self.harvesting_thread.is_alive():
                self.harvesting_thread.join(timeout=10)
            
            # Disconnect from IBKR
            if self.client:
                self.client.disconnect_and_stop()
            
            self.running = False
            logger.info("Harvester client stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping harvester client: {e}")
            return False
    
    def harvest_data(self, **kwargs) -> bool:
        """
        Harvest data using the configured harvester.
        
        Args:
            **kwargs: Arguments to pass to the harvester
            
        Returns:
            True if successful
        """
        if not self.running:
            logger.error("Harvester client not running")
            return False
            
        if not self.harvester:
            logger.error("Harvester not initialized")
            return False
            
        try:
            return self.harvester.harvest_data(**kwargs)
        except Exception as e:
            logger.error(f"Error harvesting data: {e}")
            return False
    
    def start_scheduled_harvesting(self, 
                                 symbols: list, 
                                 timeframes: list, 
                                 interval_hours: int = 24) -> bool:
        """
        Start scheduled data harvesting in a separate thread.
        
        Args:
            symbols: List of symbols to harvest
            timeframes: List of timeframe configs
            interval_hours: How often to harvest data
            
        Returns:
            True if started successfully
        """
        if not self.running:
            logger.error("Harvester client not running")
            return False
        
        self.symbols = symbols
        self.timeframes = timeframes
        self.interval_hours = interval_hours


        # Start the harvesting thread
        self.harvesting_thread = threading.Thread(
                target=self.harvesting_loop,  # Make this a class method
                name="HarvestingThread",
                daemon=True
            )
        self.harvesting_thread.start()
        
        logger.info("Scheduled harvesting started")
        return True

    def harvesting_loop(self):
            """Background thread for scheduled data harvesting."""
            logger.info(f"Starting scheduled harvesting for {len(self.symbols)} symbols")
            
            while not self.stop_event.is_set():
                try:
                    # Harvest data for each symbol and timeframe
                    for symbol in self.symbols:
                        if self.stop_event.is_set():
                            break
                            
                        for tf in self.timeframes:
                            if self.stop_event.is_set():
                                break
                                
                            try:
                                result = self.harvester.harvest_data(
                                    symbol=symbol,
                                    duration=tf["duration"],
                                    bar_size=tf["bar_size"],
                                    what_to_show=tf.get("what_to_show", "TRADES")
                                )
                                
                                # Log the result
                                if result.get("status") == "success":
                                    logger.info(f"Successfully harvested {symbol} {tf['bar_size']} data")
                                elif result.get("status") == "no_data":
                                    logger.info(f"No data found for {symbol} {tf['bar_size']}")
                                else:
                                    logger.warning(f"Partial harvest for {symbol} {tf['bar_size']}: {result}")
                                
                                # Wait between requests to avoid overwhelming the server
                                time.sleep(1)
                            
                            except Exception as e:
                                logger.error(f"Error harvesting {symbol} {tf['bar_size']} data: {e}")
                                time.sleep(1)  # Wait briefly before continuing
                    
                    logger.info("Scheduled harvesting cycle completed")
                    
                    # Wait for the next interval or until stopped
                    for _ in range(self.interval_hours * 3600):  # Convert hours to seconds
                        if self.stop_event.is_set():
                            break
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error in harvesting loop: {e}")
                    # Wait a bit before retrying
                    time.sleep(60)