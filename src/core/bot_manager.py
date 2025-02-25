"""
Bot Manager module for coordinating the trading bot's activities.
This module serves as the central controller for the trading bot.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import os
import json

# Import IBKR connector components
from src.connectors.ibkr.data_feed import IBKRDataFeed
from src.connectors.ibkr.order_manager import IBKROrderManager

# Import configuration
from src.config.settings import default_settings

# Set up logger
logger = logging.getLogger(__name__)
trade_logger = logging.getLogger('trades')

class BotManager:
    """
    Central manager for the trading bot.
    Coordinates data feeds, order management, and trading strategies.
    """
    
    def __init__(self, 
                data_feed: IBKRDataFeed,
                order_manager: IBKROrderManager,
                settings: Optional[Dict[str, Any]] = None):
        """
        Initialize the bot manager.
        
        Args:
            data_feed: The IBKR data feed client
            order_manager: The IBKR order manager
            settings: Optional settings dictionary (uses default_settings if None)
        """
        self.data_feed = data_feed
        self.order_manager = order_manager
        self.settings = settings if settings is not None else default_settings.get_all()
        
        # Trading state
        self.running = False
        self.trading_enabled = False
        self.maintenance_mode = False
        
        # Strategy tracking
        self.strategies = {}
        self.active_strategy = None
        
        # Position tracking
        self.positions = {}
        self.orders = {}
        
        # Performance tracking
        self.trades = []
        self.daily_stats = {}
        
        # Lock for thread-safe operations
        self.lock = threading.Lock()
        
        # Event for signaling shutdown
        self.shutdown_event = threading.Event()
        
        # Threads
        self.main_thread = None
        self.data_thread = None
        self.strategy_thread = None
        
        logger.info("Bot manager initialized")
    
    def start(self) -> None:
        """Start the bot manager and its components."""
        if self.running:
            logger.warning("Bot manager is already running")
            return
        
        logger.info("Starting bot manager")
        
        # Verify connections
        if not self.data_feed.connected:
            logger.error("Data feed is not connected")
            return
        
        if not self.order_manager.connected:
            logger.error("Order manager is not connected")
            return
        
        # Initialize state
        self.running = True
        self.shutdown_event.clear()
        
        # Start in maintenance mode (no trading) by default
        self.maintenance_mode = True
        self.trading_enabled = False
        
        # Start threads
        self.main_thread = threading.Thread(target=self._main_loop, daemon=True)
        self.main_thread.start()
        
        self.data_thread = threading.Thread(target=self._data_loop, daemon=True)
        self.data_thread.start()
        
        logger.info("Bot manager started in maintenance mode (trading disabled)")
    
    def stop(self) -> None:
        """Stop the bot manager and its components."""
        if not self.running:
            logger.warning("Bot manager is not running")
            return
        
        logger.info("Stopping bot manager")
        
        # Signal threads to stop
        self.running = False
        self.shutdown_event.set()
        
        # Wait for threads to complete
        if self.main_thread and self.main_thread.is_alive():
            self.main_thread.join(timeout=5)
        
        if self.data_thread and self.data_thread.is_alive():
            self.data_thread.join(timeout=5)
        
        if self.strategy_thread and self.strategy_thread.is_alive():
            self.strategy_thread.join(timeout=5)
        
        logger.info("Bot manager stopped")
    
    def enable_trading(self) -> None:
        """Enable trading for the bot."""
        with self.lock:
            if not self.running:
                logger.warning("Cannot enable trading, bot is not running")
                return
            
            self.trading_enabled = True
            self.maintenance_mode = False
            logger.info("Trading enabled")
            trade_logger.info("Trading enabled")
    
    def disable_trading(self) -> None:
        """Disable trading for the bot."""
        with self.lock:
            self.trading_enabled = False
            logger.info("Trading disabled")
            trade_logger.info("Trading disabled")
    
    def enter_maintenance_mode(self) -> None:
        """Enter maintenance mode (disables trading and performs cleanup)."""
        with self.lock:
            self.trading_enabled = False
            self.maintenance_mode = True
            logger.info("Entered maintenance mode")
    
    def exit_maintenance_mode(self) -> None:
        """Exit maintenance mode (trading remains disabled until explicitly enabled)."""
        with self.lock:
            self.maintenance_mode = False
            logger.info("Exited maintenance mode")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the bot.
        
        Returns:
            Dict[str, Any]: Status information
        """
        with self.lock:
            return {
                'running': self.running,
                'trading_enabled': self.trading_enabled,
                'maintenance_mode': self.maintenance_mode,
                'active_strategy': self.active_strategy,
                'positions_count': len(self.positions),
                'orders_count': len(self.orders),
                'trades_count': len(self.trades)
            }
    
    def _main_loop(self) -> None:
        """Main control loop for the bot manager."""
        logger.info("Main control loop started")
        
        check_interval = 1.0  # seconds
        market_status_interval = 60  # seconds
        account_update_interval = 300  # seconds
        
        last_market_check = 0
        last_account_update = 0
        
        while self.running and not self.shutdown_event.is_set():
            current_time = time.time()
            
            try:
                # Check market status periodically
                if current_time - last_market_check >= market_status_interval:
                    self._check_market_status()
                    last_market_check = current_time
                
                # Update account information periodically
                if current_time - last_account_update >= account_update_interval:
                    self._update_account_info()
                    last_account_update = current_time
                
                # Run strategy if trading is enabled and not in maintenance
                if self.trading_enabled and not self.maintenance_mode:
                    self._check_trading_signals()
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
            
            # Sleep until next check
            time.sleep(check_interval)
        
        logger.info("Main control loop stopped")
    
    def _data_loop(self) -> None:
        """Data management loop for the bot manager."""
        logger.info("Data management loop started")
        
        update_interval = 5.0  # seconds
        watchlist_update_interval = 60  # seconds
        
        last_watchlist_update = 0
        
        while self.running and not self.shutdown_event.is_set():
            current_time = time.time()
            
            try:
                # Update watchlist prices periodically
                if current_time - last_watchlist_update >= watchlist_update_interval:
                    self._update_watchlist()
                    last_watchlist_update = current_time
                
                # Monitor existing positions
                self._monitor_positions()
                
                # Monitor open orders
                self._monitor_orders()
                
            except Exception as e:
                logger.error(f"Error in data loop: {e}", exc_info=True)
            
            # Sleep until next update
            time.sleep(update_interval)
        
        logger.info("Data management loop stopped")
    
    def _check_market_status(self) -> None:
        """Check the current market status."""
        # For now, just a placeholder
        logger.debug("Checking market status")
        # In a real implementation, would check if market is open, etc.
    
    def _update_account_info(self) -> None:
        """Update account information."""
        logger.debug("Updating account information")
        
        try:
            # Request account summary
            req_id = self.order_manager.request_account_summary()
            time.sleep(2)  # Wait for data
            
            # Get account summary results
            account_summary = self.order_manager.get_account_summary_result(req_id)
            
            # Process account information
            for item in account_summary:
                if item['tag'] == 'NetLiquidation':
                    logger.info(f"Account value: {item['value']} {item['currency']}")
                elif item['tag'] == 'AvailableFunds':
                    logger.info(f"Available funds: {item['value']} {item['currency']}")
        
        except Exception as e:
            logger.error(f"Error updating account info: {e}")
    
    def _update_watchlist(self) -> None:
        """Update prices for watchlist symbols."""
        # For now, just a placeholder
        logger.debug("Updating watchlist")
        # In a real implementation, would track prices of symbols of interest
    
    def _monitor_positions(self) -> None:
        """Monitor existing positions."""
        # For now, just a placeholder
        logger.debug("Monitoring positions")
        # In a real implementation, would check for stop loss, take profit, etc.
    
    def _monitor_orders(self) -> None:
        """Monitor open orders."""
        # For now, just a placeholder
        logger.debug("Monitoring orders")
        # In a real implementation, would check order status, etc.
    
    def _check_trading_signals(self) -> None:
        """Check for trading signals from strategies."""
        # For now, just a placeholder
        logger.debug("Checking trading signals")
        # In a real implementation, would run active strategies
    
    # Trading operations
    def place_market_order(self, 
                          symbol: str, 
                          quantity: int, 
                          action: str) -> Optional[int]:
        """
        Place a market order.
        
        Args:
            symbol: The stock symbol
            quantity: Number of shares (positive)
            action: "BUY" or "SELL"
            
        Returns:
            Optional[int]: The order ID if successful, None otherwise
        """
        if not self.trading_enabled:
            logger.warning(f"Trading is disabled, cannot place {action} order for {symbol}")
            return None
        
        try:
            logger.info(f"Placing {action} market order for {quantity} shares of {symbol}")
            
            # Create order objects
            contract, order = self.order_manager.create_market_order(
                symbol=symbol,
                quantity=quantity,
                action=action
            )
            
            # Define callbacks
            def on_order_status(order_id, order_data):
                logger.info(f"Order {order_id} status update: {order_data['status']}")
                trade_logger.info(f"Order {order_id} status update: {order_data['status']}")
            
            def on_execution(order_id, execution, order_data):
                logger.info(f"Execution for order {order_id}: {execution['shares']} shares @ ${execution['price']}")
                trade_logger.info(f"Execution for order {order_id}: {execution['shares']} shares @ ${execution['price']}")
            
            # Place the order
            order_id = self.order_manager.place_order(
                contract=contract,
                order=order,
                status_callback=on_order_status,
                execution_callback=on_execution
            )
            
            # Track the order
            with self.lock:
                self.orders[order_id] = {
                    'symbol': symbol,
                    'quantity': quantity,
                    'action': action,
                    'order_type': 'MKT',
                    'time_placed': datetime.now(),
                    'status': 'PLACED'
                }
            
            logger.info(f"Order placed with ID: {order_id}")
            trade_logger.info(f"Order placed: {action} {quantity} {symbol} (ID: {order_id})")
            
            return order_id
            
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return None

# Example usage if this module is run directly
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create mock objects for testing
    class MockDataFeed:
        connected = True
    
    class MockOrderManager:
        connected = True
        
        def request_account_summary(self):
            return 1
            
        def get_account_summary_result(self, req_id):
            return [
                {'account': 'DU12345', 'tag': 'NetLiquidation', 'value': '100000', 'currency': 'USD'},
                {'account': 'DU12345', 'tag': 'AvailableFunds', 'value': '50000', 'currency': 'USD'}
            ]
    
    # Create and start the bot manager
    data_feed = MockDataFeed()
    order_manager = MockOrderManager()
    
    bot = BotManager(data_feed, order_manager)
    
    try:
        print("Starting bot manager...")
        bot.start()
        
        # Run for a while
        time.sleep(2)
        
        print("Bot status:", bot.get_status())
        
        print("Enabling trading...")
        bot.enable_trading()
        
        # Run for a while longer
        time.sleep(5)
        
        print("Bot status:", bot.get_status())
        
        print("Entering maintenance mode...")
        bot.enter_maintenance_mode()
        
        # Run for a while longer
        time.sleep(2)
        
        print("Bot status:", bot.get_status())
        
    finally:
        print("Stopping bot manager...")
        bot.stop()