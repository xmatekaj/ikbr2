"""
Base Strategy module for defining the interface for trading strategies.
All trading strategies should inherit from this base class.
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import time
import threading

# Import IBKR connector components
from src.connectors.ibkr.data_feed import IBKRDataFeed
from src.connectors.ibkr.order_manager import IBKROrderManager

# Set up logger
logger = logging.getLogger(__name__)

class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.
    
    This class defines the interface that all trading strategies must implement.
    It provides common functionality for strategy initialization, data handling,
    and signal generation.
    """
    
    def __init__(self, 
                name: str,
                data_feed: IBKRDataFeed,
                order_manager: IBKROrderManager,
                config: Dict[str, Any] = None):
        """
        Initialize the strategy.
        
        Args:
            name: Name of the strategy
            data_feed: The IBKR data feed client
            order_manager: The IBKR order manager
            config: Strategy configuration parameters
        """
        self.name = name
        self.data_feed = data_feed
        self.order_manager = order_manager
        self.config = config or {}
        
        # Strategy state
        self.running = False
        self.initialized = False
        self.last_run_time = None
        
        # Strategy metrics
        self.metrics = {
            'trades_total': 0,
            'trades_won': 0,
            'trades_lost': 0,
            'profit_total': 0.0,
            'profit_average': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0
        }
        
        # Strategy data
        self.symbols = self.config.get('symbols', [])
        self.timeframes = self.config.get('timeframes', ['1 min'])
        self.position_size = self.config.get('position_size', 1.0)
        self.max_positions = self.config.get('max_positions', 1)
        
        # Trading parameters
        self.stop_loss_pct = self.config.get('stop_loss_pct', 0.05)
        self.take_profit_pct = self.config.get('take_profit_pct', 0.1)
        self.max_hold_time = self.config.get('max_hold_time', 24*60*60)  # in seconds
        
        # Data storage
        self.market_data = {}
        self.positions = {}
        self.orders = {}
        self.signals = []
        
        # Thread control
        self.stop_event = threading.Event()
        self.strategy_thread = None
        
        logger.info(f"Strategy '{name}' initialized")
    
    def start(self) -> bool:
        """
        Start the strategy.
        
        Returns:
            bool: True if the strategy was started, False otherwise
        """
        if self.running:
            logger.warning(f"Strategy '{self.name}' is already running")
            return False
        
        if not self.initialized:
            success = self.initialize()
            if not success:
                logger.error(f"Failed to initialize strategy '{self.name}'")
                return False
        
        logger.info(f"Starting strategy '{self.name}'")
        self.running = True
        self.stop_event.clear()
        
        # Start the strategy thread
        self.strategy_thread = threading.Thread(
            target=self._run_loop,
            name=f"Strategy-{self.name}",
            daemon=True
        )
        self.strategy_thread.start()
        
        return True
    
    def stop(self) -> bool:
        """
        Stop the strategy.
        
        Returns:
            bool: True if the strategy was stopped, False otherwise
        """
        if not self.running:
            logger.warning(f"Strategy '{self.name}' is not running")
            return False
        
        logger.info(f"Stopping strategy '{self.name}'")
        self.running = False
        self.stop_event.set()
        
        # Wait for the strategy thread to end
        if self.strategy_thread and self.strategy_thread.is_alive():
            self.strategy_thread.join(timeout=10)
            if self.strategy_thread.is_alive():
                logger.warning(f"Strategy '{self.name}' thread did not terminate")
        
        return True
    
    def initialize(self) -> bool:
        """
        Initialize the strategy. Called once before starting.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            # Subscribe to market data for symbols
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    # For real-time data, we'll request streaming data
                    # For historical data, we'll request bars
                    self._subscribe_to_market_data(symbol, timeframe)
            
            self.initialized = True
            logger.info(f"Strategy '{self.name}' initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing strategy '{self.name}': {e}")
            return False
    
    def _run_loop(self) -> None:
        """Main strategy execution loop."""
        logger.info(f"Strategy '{self.name}' loop started")
        
        run_interval = self.config.get('run_interval', 60)  # seconds
        
        while self.running and not self.stop_event.is_set():
            try:
                # Run the strategy
                start_time = time.time()
                self.last_run_time = datetime.now()
                
                # Update market data
                self._update_market_data()
                
                # Generate signals
                signals = self.generate_signals()
                if signals:
                    self.signals.extend(signals)
                    logger.info(f"Strategy '{self.name}' generated {len(signals)} signals")
                
                # Process signals
                self._process_signals()
                
                # Manage positions
                self._manage_positions()
                
                # Calculate execution time
                execution_time = time.time() - start_time
                logger.debug(f"Strategy '{self.name}' execution took {execution_time:.2f} seconds")
                
                # Sleep until next run
                sleep_time = max(0, run_interval - execution_time)
                if sleep_time > 0:
                    for _ in range(int(sleep_time)):
                        if self.stop_event.is_set():
                            break
                        time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in strategy '{self.name}' loop: {e}", exc_info=True)
                time.sleep(10)  # Wait a bit longer after an error
        
        logger.info(f"Strategy '{self.name}' loop stopped")
    
    def _subscribe_to_market_data(self, symbol: str, timeframe: str) -> None:
        """
        Subscribe to market data for a symbol and timeframe.
        
        Args:
            symbol: The stock symbol
            timeframe: The data timeframe (e.g., "1 min", "1 hour", "1 day")
        """
        logger.info(f"Subscribing to market data for {symbol} ({timeframe})")
        
        # For real-time data
        if timeframe == "1 min":
            req_id = self.data_feed.request_market_data(symbol)
            
            # Store the request ID for later reference
            self.market_data.setdefault(symbol, {})
            self.market_data[symbol]['real_time_req_id'] = req_id
        
        # For historical data
        duration = "1 D"  # Default to 1 day of data
        if timeframe == "1 hour":
            duration = "5 D"  # Get 5 days for hourly data
        elif timeframe == "1 day":
            duration = "30 D"  # Get 30 days for daily data
        
        req_id = self.data_feed.request_historical_data(
            symbol=symbol,
            duration=duration,
            bar_size=timeframe
        )
        
        # Wait for the data to be received
        bars = self.data_feed.get_historical_data(req_id)
        
        # Store the historical data
        self.market_data.setdefault(symbol, {})
        self.market_data[symbol][timeframe] = bars
        
        logger.info(f"Received {len(bars)} historical bars for {symbol} ({timeframe})")
    
    def _update_market_data(self) -> None:
        """Update market data for all subscribed symbols and timeframes."""
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                # Skip real-time data, which is updated automatically
                if timeframe == "1 min":
                    continue
                
                # Request new historical data
                try:
                    # Determine appropriate duration based on timeframe
                    duration = "1 D"  # Default
                    if timeframe == "1 hour":
                        duration = "5 D"
                    elif timeframe == "1 day":
                        duration = "30 D"
                    
                    req_id = self.data_feed.request_historical_data(
                        symbol=symbol,
                        duration=duration,
                        bar_size=timeframe
                    )
                    
                    # Wait for the data to be received
                    bars = self.data_feed.get_historical_data(req_id)
                    
                    # Update the stored data
                    self.market_data.setdefault(symbol, {})
                    self.market_data[symbol][timeframe] = bars
                    
                    logger.debug(f"Updated {len(bars)} historical bars for {symbol} ({timeframe})")
                
                except Exception as e:
                    logger.error(f"Error updating market data for {symbol} ({timeframe}): {e}")
    
    def _process_signals(self) -> None:
        """Process pending trading signals."""
        # Filter out processed or expired signals
        current_time = datetime.now()
        valid_signals = []
        
        for signal in self.signals:
            # Skip processed signals
            if signal.get('processed', False):
                continue
            
            # Check for signal expiration
            signal_time = signal.get('timestamp')
            if signal_time:
                time_diff = (current_time - signal_time).total_seconds()
                if time_diff > self.config.get('signal_expiry_seconds', 300):  # 5 min default
                    signal['processed'] = True
                    signal['result'] = 'expired'
                    logger.info(f"Signal for {signal['symbol']} expired")
                    continue
            
            valid_signals.append(signal)
        
        # Process valid signals
        for signal in valid_signals:
            try:
                self._execute_signal(signal)
                signal['processed'] = True
                signal['result'] = 'executed'
            except Exception as e:
                logger.error(f"Error executing signal for {signal['symbol']}: {e}")
                signal['error'] = str(e)
        
        # Update signals list with only unprocessed signals
        self.signals = [s for s in self.signals if not s.get('processed', False)]
    
    def _execute_signal(self, signal: Dict[str, Any]) -> None:
        """
        Execute a trading signal.
        
        Args:
            signal: The trading signal dictionary
        """
        symbol = signal['symbol']
        action = signal['action']
        quantity = signal.get('quantity', 1)
        signal_type = signal.get('type', 'market')
        
        logger.info(f"Executing {action} signal for {quantity} shares of {symbol}")
        
        # For a simple market order
        if signal_type == 'market':
            # Create order objects
            contract, order = self.order_manager.create_market_order(
                symbol=symbol,
                quantity=quantity,
                action=action
            )
            
            # Place the order
            order_id = self.order_manager.place_order(contract, order)
            
            # Log the order
            logger.info(f"Placed {action} market order for {quantity} shares of {symbol}, order ID: {order_id}")
            
            # Store the order
            self.orders[order_id] = {
                'symbol': symbol,
                'action': action,
                'quantity': quantity,
                'order_type': 'market',
                'status': 'placed',
                'time': datetime.now(),
                'signal': signal
            }
    
    def _manage_positions(self) -> None:
        """Manage existing positions."""
        # This would include checking for stop-loss, take-profit, etc.
        pass
    
    @abstractmethod
    def generate_signals(self) -> List[Dict[str, Any]]:
        """
        Generate trading signals based on market data and strategy logic.
        Must be implemented by each specific strategy.
        
        Returns:
            List[Dict[str, Any]]: A list of trading signals
        """
        pass
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get strategy performance metrics.
        
        Returns:
            Dict[str, Any]: Strategy metrics
        """
        return self.metrics
    
    def get_positions(self) -> Dict[str, Any]:
        """
        Get current positions.
        
        Returns:
            Dict[str, Any]: Current positions
        """
        return self.positions
    
    def get_market_data(self, symbol: str, timeframe: str = None) -> Any:
        """
        Get market data for a symbol and optional timeframe.
        
        Args:
            symbol: The stock symbol
            timeframe: Optional timeframe (if None, returns all timeframes)
            
        Returns:
            Any: Market data
        """
        if symbol not in self.market_data:
            return None
            
        if timeframe:
            return self.market_data[symbol].get(timeframe)
        else:
            return self.market_data[symbol]
    
    def get_last_price(self, symbol: str) -> Optional[float]:
        """
        Get the last price for a symbol.
        
        Args:
            symbol: The stock symbol
            
        Returns:
            Optional[float]: The last price, or None if unavailable
        """
        return self.data_feed.get_last_price(symbol)