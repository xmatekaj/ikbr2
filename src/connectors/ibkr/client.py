"""
IBKR Client module for establishing and managing connection to Interactive Brokers.
This module handles authentication, connection management, and serves as the base
for all IBKR API interactions.
"""
import logging
from typing import Optional, Dict, List, Tuple, Any
import time
import threading

# Import IB API
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.execution import Execution

# Set up logger
logger = logging.getLogger(__name__)

class IBKRClient(EWrapper, EClient):
    """
    Primary client for connecting to Interactive Brokers.
    Combines the EWrapper and EClient functionality from the IB API.
    """
    
    def __init__(self, 
                 host: str = "127.0.0.1", 
                 port: int = 7497,  # 7497 for TWS Paper, 4002 for IB Gateway Paper
                 client_id: int = 1,
                 max_wait_time: int = 10,
                 auto_reconnect: bool = True):
        """
        Initialize the IBKR client.
        
        Args:
            host: The host address where TWS/IB Gateway is running
            port: The port number (7497 for TWS Paper, 4002 for IB Gateway Paper)
            client_id: Unique client ID for this connection
            max_wait_time: Maximum time to wait for responses in seconds
            auto_reconnect: Whether to attempt reconnection if disconnected
        """
        EClient.__init__(self, self)
        
        self.host = host
        self.port = port
        self.client_id = client_id
        self.max_wait_time = max_wait_time
        self.auto_reconnect = auto_reconnect
        
        # Connection status
        self.connected = False
        self.connection_thread = None
        
        # Request ID management
        self._req_id = 0
        self._req_id_lock = threading.Lock()
        
        # Response tracking
        self.responses = {}
        self.response_events = {}
        
        logger.info(f"IBKR Client initialized with host={host}, port={port}, client_id={client_id}")
    
    def get_next_req_id(self) -> int:
        """Get a unique request ID in a thread-safe manner."""
        with self._req_id_lock:
            self._req_id += 1
            return self._req_id
    
    def connect_and_run(self) -> None:
        """Connect to TWS/IB Gateway and start the message processing thread."""
        if self.connected:
            logger.warning("Already connected to IBKR")
            return
        
        logger.info(f"Connecting to IBKR at {self.host}:{self.port} with client ID {self.client_id}")
        
        # Connect to TWS/IB Gateway
        self.connect(self.host, self.port, self.client_id)
        
        # Start the message processing thread
        self.connection_thread = threading.Thread(target=self._run_client, daemon=True)
        self.connection_thread.start()
        
        # Wait for connection to be established
        timeout = time.time() + self.max_wait_time
        while time.time() < timeout and not self.connected:
            time.sleep(0.1)
        
        if not self.connected:
            logger.error("Failed to connect to IBKR within the timeout period")
            self.disconnect()
            raise ConnectionError("Timed out while connecting to IBKR")
        
        logger.info("Successfully connected to IBKR")
    
    def _run_client(self) -> None:
        """Run the client message loop in a separate thread."""
        try:
            self.run()
        except Exception as e:
            logger.error(f"Error in IBKR client thread: {e}")
            self.connected = False
            
            # Attempt reconnection if enabled
            if self.auto_reconnect:
                logger.info("Attempting to reconnect...")
                time.sleep(5)  # Wait before reconnecting
                self.connect_and_run()
    
    def disconnect_and_stop(self) -> None:
        """Disconnect from TWS/IB Gateway and stop the message processing thread."""
        logger.info("Disconnecting from IBKR")
        
        if self.connected:
            self.disconnect()
            self.connected = False
        
        # Wait for the connection thread to terminate
        if self.connection_thread and self.connection_thread.is_alive():
            self.connection_thread.join(timeout=2)
    
    # EWrapper method overrides for connection management
    def connectAck(self) -> None:
        """Called when connection is acknowledged."""
        super().connectAck()
        self.connected = True
        logger.info("Connection to IBKR acknowledged")
    
    def connectionClosed(self) -> None:
        """Called when connection is closed."""
        super().connectionClosed()
        self.connected = False
        logger.info("Connection to IBKR closed")
        
        # Attempt reconnection if enabled
        if self.auto_reconnect:
            logger.info("Attempting to reconnect...")
            time.sleep(5)  # Wait before reconnecting
            self.connect_and_run()
    
    def error(self, req_id: int, error_code: int, error_string: str, advanced_order_reject_json="") -> None:
        """Called when an error occurs."""
        super().error(req_id, error_code, error_string, advanced_order_reject_json)
        
        # Some error codes indicate normal events rather than actual errors
        normal_errors = {2104, 2106, 2158}  # Connection successful, connection broken, etc.
        
        if error_code in normal_errors:
            logger.info(f"IBKR message: {error_string} (code: {error_code})")
        else:
            logger.error(f"IBKR error for request {req_id}: {error_string} (code: {error_code})")
    
    def nextValidId(self, order_id: int) -> None:
        """Called by TWS/IB Gateway with the next valid order ID."""
        super().nextValidId(order_id)
        with self._req_id_lock:
            self._req_id = order_id
        logger.info(f"Next valid order ID received: {order_id}")
    
    # Utility method to create basic stock contract
    def create_stock_contract(self, symbol: str, exchange: str = "SMART", currency: str = "USD") -> Contract:
        """
        Create a stock contract for the specified symbol.
        
        Args:
            symbol: The stock symbol
            exchange: The exchange to route through (default: "SMART")
            currency: The currency (default: "USD")
            
        Returns:
            Contract: An IB API Contract object
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = currency
        return contract
    
    # Basic order placement method
    def place_market_order(self, symbol: str, quantity: int, action: str) -> int:
        """
        Place a simple market order.
        
        Args:
            symbol: The stock symbol
            quantity: Number of shares (positive)
            action: "BUY" or "SELL"
            
        Returns:
            int: The order ID
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        # Validate parameters
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if action not in ["BUY", "SELL"]:
            raise ValueError("Action must be either 'BUY' or 'SELL'")
        
        # Create contract and order
        contract = self.create_stock_contract(symbol)
        
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        order.transmit = True
        
        # Get the next order ID
        order_id = self.get_next_req_id()
        
        # Place the order
        logger.info(f"Placing {action} market order for {quantity} shares of {symbol}")
        self.placeOrder(order_id, contract, order)
        
        return order_id
    
    # Method to check account balance
    def request_account_summary(self) -> int:
        """
        Request account summary information.
        
        Returns:
            int: The request ID
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        req_id = self.get_next_req_id()
        self.responses[req_id] = []
        self.response_events[req_id] = threading.Event()
        
        # Request account summary
        logger.info("Requesting account summary")
        self.reqAccountSummary(req_id, "All", "TotalCashValue,NetLiquidation,AvailableFunds")
        
        return req_id
    
    def accountSummary(self, req_id: int, account: str, tag: str, value: str, currency: str) -> None:
        """Called when account summary information is received."""
        super().accountSummary(req_id, account, tag, value, currency)
        
        if req_id in self.responses:
            self.responses[req_id].append({
                'account': account,
                'tag': tag,
                'value': value,
                'currency': currency
            })
            
            logger.debug(f"Account summary: {account} - {tag}: {value} {currency}")
    
    def accountSummaryEnd(self, req_id: int) -> None:
        """Called when account summary end is received."""
        super().accountSummaryEnd(req_id)
        
        if req_id in self.response_events:
            self.response_events[req_id].set()
            
        logger.debug(f"Account summary request {req_id} completed")
    
    def get_account_summary_result(self, req_id: int, timeout: float = None) -> List[Dict]:
        """
        Get the account summary result for the specified request ID.
        
        Args:
            req_id: The request ID from request_account_summary
            timeout: Maximum time to wait in seconds (None = default max_wait_time)
            
        Returns:
            List[Dict]: The account summary information
        """
        if timeout is None:
            timeout = self.max_wait_time
            
        if req_id not in self.response_events:
            raise ValueError(f"Invalid request ID: {req_id}")
            
        # Wait for the response
        is_completed = self.response_events[req_id].wait(timeout)
        
        if not is_completed:
            logger.warning(f"Timed out waiting for account summary (request {req_id})")
            return []
            
        # Get the response data
        result = self.responses.pop(req_id, [])
        self.response_events.pop(req_id)
        
        return result


# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create and connect the client
    client = IBKRClient()
    
    try:
        client.connect_and_run()
        
        # Request account information
        req_id = client.request_account_summary()
        
        # Wait for the result
        time.sleep(5)
        
        # Get the result
        account_summary = client.get_account_summary_result(req_id)
        print("\nAccount Summary:")
        for item in account_summary:
            print(f"{item['tag']}: {item['value']} {item['currency']}")
            
        # Place a test order (commented out for safety)
        # order_id = client.place_market_order("AAPL", 1, "BUY")
        # print(f"\nPlaced order with ID: {order_id}")
        
        # Keep the program running to receive responses
        input("\nPress Enter to exit...\n")
        
    finally:
        # Disconnect when done
        client.disconnect_and_stop()