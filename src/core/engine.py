"""
Trading Engine for IKBR Trader Bot.

This module implements the core trading engine that coordinates the different
components of the trading system, including strategy execution, order management,
data feeds, and performance tracking.
"""
import logging
import time
from threading import Thread, Event
from typing import Dict, List, Optional, Type, Union

from ..config.settings import TradingConfig
from ..connectors.ibkr.client import IBKRClient
from ..connectors.ibkr.data_feed import IBKRDataFeed
from ..connectors.ibkr.order_manager import IBKROrderManager
from ..strategies.base_strategy import BaseStrategy
from ..trading.trade_manager import TradeManager
from ..utils.logging import system_logger

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    The main trading engine that coordinates all components of the trading system.
    
    This class is responsible for initializing and managing the connections to IBKR,
    running trading strategies, executing trades, and monitoring performance.
    """
    
    def __init__(self, config: TradingConfig):
        """
        Initialize the trading engine with the given configuration.
        
        Args:
            config: A TradingConfig object containing the trading parameters
        """
        self.config = config
        self.running = False
        self.stop_event = Event()
        
        # Initialize IBKR components
        self.client = IBKRClient(
            host=config.ibkr_host,
            port=config.ibkr_port,
            client_id=config.ibkr_client_id
        )
        self.data_feed = IBKRDataFeed(self.client)
        self.order_manager = IBKROrderManager(self.client)
        
        # Initialize trade manager
        self.trade_manager = TradeManager(self.order_manager)
        
        # Strategies container
        self.strategies: Dict[str, BaseStrategy] = {}
        
        # Engine thread
        self.engine_thread: Optional[Thread] = None
        
        logger.info("Trading engine initialized")
    
    def add_strategy(self, strategy_id: str, strategy: BaseStrategy) -> None:
        """
        Add a strategy to the engine.
        
        Args:
            strategy_id: A unique identifier for the strategy
            strategy: The strategy instance to add
        """
        if strategy_id in self.strategies:
            logger.warning(f"Strategy with ID {strategy_id} already exists, overwriting")
        
        # Connect the strategy to the trade manager
        strategy.set_trade_manager(self.trade_manager)
        # Connect the strategy to the data feed
        strategy.set_data_feed(self.data_feed)
        
        self.strategies[strategy_id] = strategy
        logger.info(f"Strategy {strategy_id} added to engine")
    
    def remove_strategy(self, strategy_id: str) -> None:
        """
        Remove a strategy from the engine.
        
        Args:
            strategy_id: The ID of the strategy to remove
        """
        if strategy_id in self.strategies:
            del self.strategies[strategy_id]
            logger.info(f"Strategy {strategy_id} removed from engine")
        else:
            logger.warning(f"Strategy {strategy_id} not found in engine")
    
    def connect(self) -> bool:
        """
        Connect to the IBKR TWS or Gateway.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            connected = self.client.connect()
            if connected:
                logger.info("Successfully connected to IBKR")
                # Wait for contract and account details to be fully loaded
                time.sleep(2)
                return True
            else:
                logger.error("Failed to connect to IBKR")
                return False
        except Exception as e:
            logger.exception(f"Error connecting to IBKR: {e}")
            return False
    
    def disconnect(self) -> None:
        """Disconnect from the IBKR TWS or Gateway."""
        try:
            self.client.disconnect()
            logger.info("Disconnected from IBKR")
        except Exception as e:
            logger.exception(f"Error disconnecting from IBKR: {e}")
    
    def _run_engine_loop(self) -> None:
        """Main engine loop that runs trading strategies."""
        logger.info("Starting trading engine loop")
        
        try:
            while not self.stop_event.is_set():
                # Check connection status
                if not self.client.is_connected():
                    logger.warning("IBKR connection lost, attempting to reconnect")
                    self.connect()
                    if not self.client.is_connected():
                        logger.error("Failed to reconnect to IBKR, stopping engine")
                        self.stop()
                        break
                
                # Execute each strategy
                for strategy_id, strategy in self.strategies.items():
                    try:
                        if strategy.should_update():
                            strategy.update()
                    except Exception as e:
                        logger.exception(f"Error executing strategy {strategy_id}: {e}")
                
                # Process any pending orders
                self.order_manager.process_pending_orders()
                
                # Sleep to avoid excessive CPU usage
                time.sleep(self.config.engine_loop_interval)
                
        except Exception as e:
            logger.exception(f"Unhandled exception in engine loop: {e}")
        
        logger.info("Trading engine loop stopped")
    
    def start(self) -> bool:
        """
        Start the trading engine.
        
        Returns:
            True if engine started successfully, False otherwise
        """
        if self.running:
            logger.warning("Trading engine is already running")
            return False
        
        # Connect to IBKR
        if not self.client.is_connected():
            if not self.connect():
                logger.error("Failed to start engine due to connection failure")
                return False
        
        # Reset stop event
        self.stop_event.clear()
        
        # Start engine thread
        self.engine_thread = Thread(target=self._run_engine_loop, daemon=True)
        self.engine_thread.start()
        
        self.running = True
        logger.info("Trading engine started")
        return True
    
    def stop(self) -> None:
        """Stop the trading engine."""
        if not self.running:
            logger.warning("Trading engine is not running")
            return
        
        logger.info("Stopping trading engine")
        
        # Signal the engine loop to stop
        self.stop_event.set()
        
        # Wait for the engine thread to finish
        if self.engine_thread and self.engine_thread.is_alive():
            self.engine_thread.join(timeout=10)
        
        # Disconnect from IBKR
        self.disconnect()
        
        self.running = False
        logger.info("Trading engine stopped")
    
    def is_running(self) -> bool:
        """
        Check if the trading engine is running.
        
        Returns:
            True if engine is running, False otherwise
        """
        return self.running
    
    def get_strategy_status(self, strategy_id: str) -> dict:
        """
        Get the status of a specific strategy.
        
        Args:
            strategy_id: The ID of the strategy to check
            
        Returns:
            A dictionary containing strategy status information
        """
        if strategy_id not in self.strategies:
            return {"error": f"Strategy {strategy_id} not found"}
        
        strategy = self.strategies[strategy_id]
        return {
            "id": strategy_id,
            "name": strategy.__class__.__name__,
            "active": strategy.is_active(),
            "last_update": strategy.last_update_time,
            "symbols": strategy.get_symbols(),
            "positions": strategy.get_positions(),
        }
    
    def get_engine_status(self) -> dict:
        """
        Get the current status of the trading engine.
        
        Returns:
            A dictionary containing engine status information
        """
        return {
            "running": self.running,
            "ibkr_connected": self.client.is_connected(),
            "strategies": {
                strategy_id: {
                    "name": strategy.__class__.__name__,
                    "active": strategy.is_active(),
                }
                for strategy_id, strategy in self.strategies.items()
            },
            "pending_orders": self.order_manager.get_pending_orders_count(),
        }