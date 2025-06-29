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
        # Extract use_delayed_data from kwargs before passing to parent
        self.use_delayed_data = kwargs.pop('use_delayed_data', True)

        super().__init__(**kwargs)
        
        # Market data storage
        self.market_data = {}
        self.historical_data = {}
        
        # Callbacks
        self.tick_callbacks = {}
        self.bar_callbacks = {}
        
        # Flags for data types
        self.use_delayed_data = kwargs.get('use_delayed_data', True)
        self.data_type_flags = {
            'real_time_available': False,
            'delayed_available': False
        }
    
    def request_market_data(self, 
                           symbol: str, 
                           exchange: str = "SMART",
                           data_type: str = "",
                           snapshot: bool = False,
                           callback: Optional[Callable] = None) -> int:
        """
        Request real-time or delayed market data for a symbol.
        
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
            'raw_ticks': [],
            'is_delayed': False,  # Flag to track if this data is delayed
            'error_messages': []  # Store error messages related to this request
        }
        
        # Register callback if provided
        if callback:
            self.tick_callbacks[req_id] = callback
        
        # Request market data
        logger.info(f"Requesting market data for {symbol}")
        self.reqMktData(req_id, contract, data_type, snapshot, False, [])
        
        return req_id
    
    def error(self, reqId, timeNow, errorCode, errorString, advancedOrderRejectJson=""):
        """Handle error messages including those related to market data."""
        # Call the parent error handler
        super().error(reqId, timeNow, errorCode, errorString, advancedOrderRejectJson)
        
        # Handle specific market data errors
        if reqId >= 0 and reqId in self.market_data:
            # Store error message
            self.market_data[reqId]['error_messages'].append({
                'code': errorCode,
                'message': errorString
            })
            
            # Check for specific error codes
            if errorCode == 10090:  # No real-time market data subscription
                if "Delayed market data is available" in errorString and self.use_delayed_data:
                    logger.info(f"Switching to delayed data for request {reqId}")
                    self._request_delayed_data(reqId)
                    self.market_data[reqId]['is_delayed'] = True
                    self.data_type_flags['delayed_available'] = True
            elif errorCode >= 200 and errorCode < 300:  # Market data related errors
                if "Delayed market data is available" in errorString and self.use_delayed_data:
                    logger.info(f"Switching to delayed data for request {reqId}")
                    self._request_delayed_data(reqId)
                    self.market_data[reqId]['is_delayed'] = True
                    self.data_type_flags['delayed_available'] = True
    
    def _request_delayed_data(self, req_id: int) -> None:
        """
        Request delayed market data for an existing request.
        
        Args:
            req_id: The original request ID
        """
        if req_id not in self.market_data:
            logger.warning(f"Cannot request delayed data for unknown request {req_id}")
            return
        
        symbol = self.market_data[req_id]['symbol']
        contract = self.create_stock_contract(symbol)
        
        # Set market data type to delayed
        self.reqMarketDataType(3)  # 3 = Delayed, 1 = Live, 2 = Frozen
        
        # Request delayed data using the same request ID
        logger.info(f"Requesting delayed market data for {symbol}")
        self.reqMktData(req_id, contract, "", False, False, [])
    
    # Update tickPrice to handle delayed data
    def tickPrice(self, req_id: TickerId, field: int, price: float, attrib: TickAttrib) -> None:
        """Called when price tick data is received."""
        super().tickPrice(req_id, field, price, attrib)
        
        if req_id not in self.market_data:
            return
            
        data = self.market_data[req_id]
        timestamp = datetime.now()
        
        # Determine if this is a delayed tick
        is_delayed_tick = (field >= 33 and field <= 57)  # Delayed tick fields are 33-57
        
        # If this is our first price tick, set the delayed flag
        if data['last_price'] is None and (is_delayed_tick or data['is_delayed']):
            data['is_delayed'] = True
            logger.info(f"Receiving delayed data for {data['symbol']}")
        
        # Store tick data based on field type
        # Handle both real-time and delayed equivalents
        if field == 1 or field == 33:  # Bid price (live or delayed)
            data['bid'] = price
        elif field == 2 or field == 34:  # Ask price (live or delayed)
            data['ask'] = price
        elif field == 4 or field == 35:  # Last price (live or delayed)
            data['last_price'] = price
            data['last_timestamp'] = timestamp
        
        # Store raw tick for complete history
        data['raw_ticks'].append({
            'timestamp': timestamp,
            'field': field,
            'price': price,
            'is_delayed': is_delayed_tick or data['is_delayed']
        })
        
        # Call the callback if registered
        if req_id in self.tick_callbacks:
            # Make a copy of the data to avoid modification during callback
            callback_data = data.copy()
            try:
                self.tick_callbacks[req_id](req_id, callback_data)
            except Exception as e:
                logger.error(f"Error in tick callback: {e}")
    
    # Update tickSize to handle delayed data
    def tickSize(self, req_id: TickerId, field: int, size: int) -> None:
        """Called when size tick data is received."""
        super().tickSize(req_id, field, size)
        
        if req_id not in self.market_data:
            return
            
        data = self.market_data[req_id]
        
        # Determine if this is a delayed tick
        is_delayed_tick = (field >= 33 and field <= 57)  # Delayed tick fields
        
        # Field values for both real-time and delayed
        if field == 8 or field == 41:  # Volume (live or delayed)
            data['volume'] = size
        
        # Store raw tick
        data['raw_ticks'].append({
            'timestamp': datetime.now(),
            'field': field,
            'size': size,
            'is_delayed': is_delayed_tick or data['is_delayed']
        })
    
    def get_last_price(self, symbol: str, timeout: float = 5.0, accept_delayed: bool = True) -> Optional[float]:
        """
        Get the last price for a symbol. 
        Makes a new request if no existing data is available.
        
        Args:
            symbol: The stock symbol
            timeout: Maximum time to wait for data in seconds
            accept_delayed: Whether to accept delayed data
            
        Returns:
            Optional[float]: The last price, or None if unavailable
        """
        # Check if we already have data for this symbol
        for req_id, data in self.market_data.items():
            if data['symbol'] == symbol and data['last_price'] is not None:
                # Check if we should accept delayed data
                if data['is_delayed'] and not accept_delayed:
                    continue
                return data['last_price']
        
        # Save original delayed data setting
        original_setting = self.use_delayed_data
        self.use_delayed_data = accept_delayed
        
        # Request new data
        req_id = self.request_market_data(symbol, snapshot=True)
        
        # Wait for data to arrive
        start_time = time.time()
        while time.time() - start_time < timeout:
            if req_id in self.market_data and self.market_data[req_id]['last_price'] is not None:
                price = self.market_data[req_id]['last_price']
                is_delayed = self.market_data[req_id]['is_delayed']
                
                # Log if using delayed data
                if is_delayed:
                    logger.info(f"Using delayed price data for {symbol}: ${price}")
                
                self.cancel_market_data(req_id)
                self.use_delayed_data = original_setting
                return price
            time.sleep(0.1)
        
        # Cancel the request if timed out
        self.cancel_market_data(req_id)
        self.use_delayed_data = original_setting
        return None
    
    # Add a method to set market data type globally
    def set_market_data_type(self, data_type: int) -> None:
        """
        Set the market data type for all subsequent requests.
        
        Args:
            data_type: 1 for real-time, 2 for frozen, 3 for delayed, 4 for delayed frozen
        """
        logger.info(f"Setting market data type to: {data_type}")
        self.reqMarketDataType(data_type)
        
        # Update flags
        if data_type == 1:
            self.use_delayed_data = False
        elif data_type == 3:
            self.use_delayed_data = True

    def cancel_market_data(self, req_id: int) -> None:
        """
        Cancel a market data subscription.
        
        Args:
            req_id: The request ID from request_market_data
        """
        if not self.connected:
            logger.warning("Not connected to IBKR when trying to cancel market data")
            return
        
        # Cancel the market data request
        logger.info(f"Canceling market data request {req_id}")
        self.cancelMktData(req_id)
        
        # Remove from storage if found
        if req_id in self.market_data:
            del self.market_data[req_id]
        
        # Remove callback if registered
        if req_id in self.tick_callbacks:
            del self.tick_callbacks[req_id]