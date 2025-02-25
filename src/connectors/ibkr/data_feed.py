"""
IBKR Data Feed module for requesting and processing market data from Interactive Brokers.
This module extends the base IBKRClient to provide specialized market data functionality.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timedelta

# Import IB API
from ibapi.contract import Contract
from ibapi.common import BarData, TickerId, TickAttrib

# Import base client
from .client import IBKRClient

# Set up logger
logger = logging.getLogger(__name__)

class IBKRDataFeed(IBKRClient):
    """
    Specialized client for requesting and processing market data from Interactive Brokers.
    Extends the base IBKRClient with market data functionality.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the IBKR data feed client.
        
        Args:
            **kwargs: Arguments to pass to the base IBKRClient
        """
        super().__init__(**kwargs)
        
        # Market data storage
        self.market_data = {}
        self.historical_data = {}
        
        # Callbacks
        self.tick_callbacks = {}
        self.bar_callbacks = {}
    
    # Real-time market data methods
    def request_market_data(self, 
                           symbol: str, 
                           exchange: str = "SMART",
                           data_type: str = "",
                           snapshot: bool = False,
                           callback: Optional[Callable] = None) -> int:
        """
        Request real-time market data for a symbol.
        
        Args:
            symbol: The stock symbol
            exchange: The exchange (default: "SMART")
            data_type: Type of market data requested (default: "")
            snapshot: Whether to get a snapshot (True) or continuous updates (False)
            callback: Optional callback function to process tick data
            
        Returns:
            int: The request ID
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        # Create contract
        contract = self.create_stock_contract(symbol, exchange)
        
        # Get a new request ID
        req_id = self.get_next_req_id()
        
        # Initialize storage for this request
        self.market_data[req_id] = {
            'symbol': symbol,
            'last_price': None,
            'bid': None,
            'ask': None,
            'volume': None,
            'last_timestamp': None,
            'raw_ticks': []
        }
        
        # Register callback if provided
        if callback:
            self.tick_callbacks[req_id] = callback
        
        # Request market data
        logger.info(f"Requesting market data for {symbol}")
        self.reqMktData(req_id, contract, data_type, snapshot, False, [])
        
        return req_id
    
    def cancel_market_data(self, req_id: int) -> None:
        """
        Cancel a market data subscription.
        
        Args:
            req_id: The request ID from request_market_data
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        # Cancel the market data request
        logger.info(f"Canceling market data request {req_id}")
        self.cancelMktData(req_id)
        
        # Clean up
        self.market_data.pop(req_id, None)
        self.tick_callbacks.pop(req_id, None)
    
    # Historical data methods
    def request_historical_data(self,
                              symbol: str,
                              duration: str = "1 D",
                              bar_size: str = "1 min",
                              what_to_show: str = "MIDPOINT",
                              use_rth: bool = True,
                              end_datetime: str = "",
                              callback: Optional[Callable] = None) -> int:
        """
        Request historical price data for a symbol.
        
        Args:
            symbol: The stock symbol
            duration: Time period (e.g., "1 D", "2 W", "1 M", "1 Y")
            bar_size: Bar size (e.g., "1 min", "5 mins", "1 hour", "1 day")
            what_to_show: Type of data ("MIDPOINT", "BID", "ASK", "TRADES", etc.)
            use_rth: Use regular trading hours only
            end_datetime: End date and time (empty string for now)
            callback: Optional callback function to process bar data
            
        Returns:
            int: The request ID
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        # Create contract
        contract = self.create_stock_contract(symbol)
        
        # Get a new request ID
        req_id = self.get_next_req_id()
        
        # Set up storage and event for this request
        self.historical_data[req_id] = {
            'symbol': symbol,
            'bars': [],
            'completed': threading.Event()
        }
        
        # Register callback if provided
        if callback:
            self.bar_callbacks[req_id] = callback
        
        # Request historical data
        logger.info(f"Requesting historical data for {symbol} ({duration}, {bar_size})")
        self.reqHistoricalData(
            req_id, 
            contract, 
            end_datetime, 
            duration, 
            bar_size, 
            what_to_show, 
            use_rth, 
            1,  # formatDate (1 = show dates as YYYYMMDD)
            False,  # keepUpToDate
            []  # chartOptions
        )
        
        return req_id
    
    def get_historical_data(self, req_id: int, timeout: float = None) -> List[Dict]:
        """
        Get historical data for a request, waiting for completion if necessary.
        
        Args:
            req_id: The request ID from request_historical_data
            timeout: Maximum time to wait in seconds (None = default max_wait_time)
            
        Returns:
            List[Dict]: The historical data bars
        """
        if timeout is None:
            timeout = self.max_wait_time
            
        if req_id not in self.historical_data:
            raise ValueError(f"Invalid request ID: {req_id}")
        
        # Wait for the request to complete
        is_completed = self.historical_data[req_id]['completed'].wait(timeout)
        
        if not is_completed:
            logger.warning(f"Timed out waiting for historical data (request {req_id})")
        
        # Return the bars (even if incomplete)
        return self.historical_data[req_id]['bars']
    
    # IB API callback overrides for market data
    def tickPrice(self, req_id: TickerId, field: int, price: float, attrib: TickAttrib) -> None:
        """Called when price tick data is received."""
        super().tickPrice(req_id, field, price, attrib)
        
        if req_id not in self.market_data:
            return
            
        data = self.market_data[req_id]
        timestamp = datetime.now()
        
        # Store tick data based on field type
        # Field values: 1=bid, 2=ask, 4=last, 6=high, 7=low, 9=close, etc.
        if field == 1:  # Bid price
            data['bid'] = price
        elif field == 2:  # Ask price
            data['ask'] = price
        elif field == 4:  # Last price
            data['last_price'] = price
            data['last_timestamp'] = timestamp
        
        # Store raw tick for complete history
        data['raw_ticks'].append({
            'timestamp': timestamp,
            'field': field,
            'price': price
        })
        
        # Call the callback if registered
        if req_id in self.tick_callbacks:
            # Make a copy of the data to avoid modification during callback
            callback_data = data.copy()
            try:
                self.tick_callbacks[req_id](req_id, callback_data)
            except Exception as e:
                logger.error(f"Error in tick callback: {e}")
    
    def tickSize(self, req_id: TickerId, field: int, size: int) -> None:
        """Called when size tick data is received."""
        super().tickSize(req_id, field, size)
        
        if req_id not in self.market_data:
            return
            
        data = self.market_data[req_id]
        
        # Field values: 0=bid size, 3=ask size, 5=last size, 8=volume
        if field == 8:  # Volume
            data['volume'] = size
        
        # Store raw tick
        data['raw_ticks'].append({
            'timestamp': datetime.now(),
            'field': field,
            'size': size
        })
    
    # IB API callback overrides for historical data
    def historicalData(self, req_id: int, bar: BarData) -> None:
        """Called when historical data is received."""
        super().historicalData(req_id, bar)
        
        if req_id not in self.historical_data:
            return
        
        # Convert bar to dictionary
        bar_dict = {
            'date': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume,
            'wap': bar.wap,
            'count': bar.barCount
        }
        
        # Store the bar
        self.historical_data[req_id]['bars'].append(bar_dict)
        
        # Call the callback if registered
        if req_id in self.bar_callbacks:
            try:
                self.bar_callbacks[req_id](req_id, bar_dict)
            except Exception as e:
                logger.error(f"Error in bar callback: {e}")
    
    def historicalDataEnd(self, req_id: int, start: str, end: str) -> None:
        """Called when historical data retrieval is completed."""
        super().historicalDataEnd(req_id, start, end)
        
        if req_id in self.historical_data:
            # Signal completion
            self.historical_data[req_id]['completed'].set()
            
            logger.info(f"Historical data request {req_id} completed ({start} to {end})")
    
    def get_last_price(self, symbol: str, timeout: float = 5.0) -> Optional[float]:
        """
        Get the last price for a symbol. 
        Makes a new request if no existing data is available.
        
        Args:
            symbol: The stock symbol
            timeout: Maximum time to wait for data in seconds
            
        Returns:
            Optional[float]: The last price, or None if unavailable
        """
        # Check if we already have data for this symbol
        for req_id, data in self.market_data.items():
            if data['symbol'] == symbol and data['last_price'] is not None:
                return data['last_price']
        
        # Request new data
        req_id = self.request_market_data(symbol, snapshot=True)
        
        # Wait for data to arrive
        start_time = time.time()
        while time.time() - start_time < timeout:
            if req_id in self.market_data and self.market_data[req_id]['last_price'] is not None:
                price = self.market_data[req_id]['last_price']
                self.cancel_market_data(req_id)
                return price
            time.sleep(0.1)
        
        # Cancel the request if timed out
        self.cancel_market_data(req_id)
        return None


# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create and connect the data feed client
    data_feed = IBKRDataFeed()
    
    try:
        data_feed.connect_and_run()
        
        # Real-time data example
        def on_tick(req_id, data):
            print(f"Tick for {data['symbol']}: Last={data['last_price']}, Bid={data['bid']}, Ask={data['ask']}")
        
        # Request streaming data for Apple
        aapl_req_id = data_feed.request_market_data("AAPL", callback=on_tick)
        
        # Historical data example
        spy_req_id = data_feed.request_historical_data(
            "SPY", 
            duration="5 D", 
            bar_size="1 hour",
            what_to_show="TRADES"
        )
        
        # Wait for historical data
        historical_data = data_feed.get_historical_data(spy_req_id)
        
        print("\nHistorical data for SPY:")
        for bar in historical_data[:5]:  # Show the first 5 bars
            print(f"{bar['date']}: Open={bar['open']}, Close={bar['close']}, Volume={bar['volume']}")
        
        # Get a snapshot of the current price
        msft_price = data_feed.get_last_price("MSFT")
        print(f"\nMicrosoft last price: {msft_price}")
        
        # Keep receiving real-time data for a while
        print("\nReceiving real-time data for 30 seconds...\n")
        time.sleep(30)
        
        # Cancel market data subscription
        data_feed.cancel_market_data(aapl_req_id)
        
    finally:
        # Disconnect when done
        data_feed.disconnect_and_stop()