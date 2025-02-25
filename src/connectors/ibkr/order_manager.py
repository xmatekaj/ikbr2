"""
IBKR Order Manager module for submitting and tracking orders with Interactive Brokers.
This module extends the base IBKRClient to provide specialized order management functionality.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Callable, Any, Tuple, Union
from datetime import datetime
from enum import Enum

# Import IB API
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.execution import Execution
from ibapi.commission_report import CommissionReport

# Import base client
from .client import IBKRClient

# Set up logger
logger = logging.getLogger(__name__)

class OrderStatus(Enum):
    """Enum for order status values."""
    CREATED = "Created"
    SUBMITTED = "Submitted"
    PENDING_SUBMIT = "PendingSubmit"
    PRESUBMITTED = "PreSubmitted"
    PENDING_CANCEL = "PendingCancel"
    CANCELLED = "Cancelled"
    FILLED = "Filled"
    PARTIALLY_FILLED = "PartiallyFilled"
    API_CANCELLED = "ApiCancelled"
    API_PENDING = "ApiPending"
    INACTIVE = "Inactive"
    ERROR = "Error"
    UNKNOWN = "Unknown"

class IBKROrderManager(IBKRClient):
    """
    Specialized client for submitting and tracking orders with Interactive Brokers.
    Extends the base IBKRClient with order management functionality.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the IBKR order manager.
        
        Args:
            **kwargs: Arguments to pass to the base IBKRClient
        """
        super().__init__(**kwargs)
        
        # Order tracking
        self.orders = {}
        self.executions = {}
        self.order_status_callbacks = {}
        self.execution_callbacks = {}
        
        # Lock for order operations
        self.orders_lock = threading.Lock()
    
    def place_order(self, 
                   contract: Contract, 
                   order: Order, 
                   status_callback: Optional[Callable] = None,
                   execution_callback: Optional[Callable] = None) -> int:
        """
        Place an order with IBKR.
        
        Args:
            contract: The contract to trade
            order: The order specifications
            status_callback: Optional callback for order status updates
            execution_callback: Optional callback for order executions
            
        Returns:
            int: The order ID
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        # Get the next order ID
        order_id = self.get_next_req_id()
        
        # Initialize order tracking
        with self.orders_lock:
            self.orders[order_id] = {
                'contract': contract,
                'order': order,
                'status': OrderStatus.CREATED.value,
                'filled_quantity': 0,
                'avg_fill_price': 0.0,
                'remaining_quantity': order.totalQuantity,
                'last_update_time': datetime.now(),
                'is_complete': False,
                'executions': []
            }
            
            # Register callbacks if provided
            if status_callback:
                self.order_status_callbacks[order_id] = status_callback
            if execution_callback:
                self.execution_callbacks[order_id] = execution_callback
        
        # Submit the order
        logger.info(f"Placing order {order_id}: {order.action} {order.totalQuantity} {contract.symbol} {order.orderType}")
        self.placeOrder(order_id, contract, order)
        
        return order_id
    
    def cancel_order(self, order_id: int) -> None:
        """
        Cancel an open order.
        
        Args:
            order_id: The order ID to cancel
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        # Verify the order exists
        with self.orders_lock:
            if order_id not in self.orders:
                raise ValueError(f"Order ID {order_id} not found")
            
            # Check if order can be cancelled
            status = self.orders[order_id]['status']
            if status in [OrderStatus.FILLED.value, OrderStatus.CANCELLED.value]:
                logger.warning(f"Cannot cancel order {order_id} with status {status}")
                return
        
        # Send cancel request
        logger.info(f"Cancelling order {order_id}")
        self.cancelOrder(order_id)
    
    def get_order_status(self, order_id: int) -> Dict:
        """
        Get the current status of an order.
        
        Args:
            order_id: The order ID to query
            
        Returns:
            Dict: The current order status information
        """
        with self.orders_lock:
            if order_id not in self.orders:
                raise ValueError(f"Order ID {order_id} not found")
            
            # Return a copy to avoid modification
            return self.orders[order_id].copy()
    
    def get_open_orders(self) -> List[int]:
        """
        Get a list of all open order IDs.
        
        Returns:
            List[int]: List of open order IDs
        """
        open_orders = []
        
        with self.orders_lock:
            for order_id, order_data in self.orders.items():
                if not order_data['is_complete']:
                    open_orders.append(order_id)
        
        return open_orders
    
    def request_open_orders(self) -> None:
        """
        Request all open orders from IBKR.
        This will trigger orderStatus callbacks for all open orders.
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        logger.info("Requesting all open orders")
        self.reqOpenOrders()
    
    def request_all_open_orders(self) -> None:
        """
        Request all open orders for all clients from IBKR.
        This includes orders placed by other clients if you have appropriate permissions.
        """
        if not self.connected:
            raise ConnectionError("Not connected to IBKR")
        
        logger.info("Requesting all open orders for all clients")
        self.reqAllOpenOrders()
    
    # Helper methods for creating different order types
    def create_market_order(self, 
                           symbol: str, 
                           quantity: int, 
                           action: str,
                           transmit: bool = True) -> Tuple[Contract, Order]:
        """
        Create a market order for the specified symbol.
        
        Args:
            symbol: The stock symbol
            quantity: Number of shares (positive)
            action: "BUY" or "SELL"
            transmit: Whether to transmit the order immediately
            
        Returns:
            Tuple[Contract, Order]: The contract and order objects
        """
        # Validate parameters
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if action not in ["BUY", "SELL"]:
            raise ValueError("Action must be either 'BUY' or 'SELL'")
        
        # Create contract
        contract = self.create_stock_contract(symbol)
        
        # Create order
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        order.transmit = transmit
        
        return contract, order
    
    def create_limit_order(self, 
                          symbol: str, 
                          quantity: int, 
                          action: str, 
                          limit_price: float,
                          transmit: bool = True) -> Tuple[Contract, Order]:
        """
        Create a limit order for the specified symbol.
        
        Args:
            symbol: The stock symbol
            quantity: Number of shares (positive)
            action: "BUY" or "SELL"
            limit_price: The limit price
            transmit: Whether to transmit the order immediately
            
        Returns:
            Tuple[Contract, Order]: The contract and order objects
        """
        # Validate parameters
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if action not in ["BUY", "SELL"]:
            raise ValueError("Action must be either 'BUY' or 'SELL'")
        if limit_price <= 0:
            raise ValueError("Limit price must be positive")
        
        # Create contract
        contract = self.create_stock_contract(symbol)
        
        # Create order
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limit_price
        order.transmit = transmit
        
        return contract, order
    
    def create_stop_order(self, 
                         symbol: str, 
                         quantity: int, 
                         action: str, 
                         stop_price: float,
                         transmit: bool = True) -> Tuple[Contract, Order]:
        """
        Create a stop order for the specified symbol.
        
        Args:
            symbol: The stock symbol
            quantity: Number of shares (positive)
            action: "BUY" or "SELL"
            stop_price: The stop price
            transmit: Whether to transmit the order immediately
            
        Returns:
            Tuple[Contract, Order]: The contract and order objects
        """
        # Validate parameters
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if action not in ["BUY", "SELL"]:
            raise ValueError("Action must be either 'BUY' or 'SELL'")
        if stop_price <= 0:
            raise ValueError("Stop price must be positive")
        
        # Create contract
        contract = self.create_stock_contract(symbol)
        
        # Create order
        order = Order()
        order.action = action
        order.orderType = "STP"
        order.totalQuantity = quantity
        order.auxPrice = stop_price  # For stop orders, use auxPrice
        order.transmit = transmit
        
        return contract, order
    
    # IB API callback overrides for order management
    def orderStatus(self, 
                   order_id: int, 
                   status: str, 
                   filled: float, 
                   remaining: float, 
                   avg_fill_price: float, 
                   perm_id: int, 
                   parent_id: int, 
                   last_fill_price: float, 
                   client_id: int, 
                   why_held: str,
                   mkt_cap_price: float) -> None:
        """Called when the status of an order changes."""
        super().orderStatus(order_id, status, filled, remaining, avg_fill_price, 
                          perm_id, parent_id, last_fill_price, client_id, why_held, mkt_cap_price)
        
        logger.info(f"Order {order_id} status: {status}, filled: {filled}, remaining: {remaining}, avg price: {avg_fill_price}")
        
        with self.orders_lock:
            if order_id not in self.orders:
                # This could be an order placed outside our system
                logger.warning(f"Received status for unknown order {order_id}: {status}")
                self.orders[order_id] = {
                    'contract': None,
                    'order': None,
                    'status': status,
                    'filled_quantity': filled,
                    'avg_fill_price': avg_fill_price,
                    'remaining_quantity': remaining,
                    'last_update_time': datetime.now(),
                    'is_complete': status in ['Filled', 'Cancelled', 'ApiCancelled'],
                    'executions': []
                }
            else:
                # Update our record
                self.orders[order_id].update({
                    'status': status,
                    'filled_quantity': filled,
                    'avg_fill_price': avg_fill_price,
                    'remaining_quantity': remaining,
                    'last_update_time': datetime.now(),
                    'is_complete': status in ['Filled', 'Cancelled', 'ApiCancelled']
                })
        
            # Get a copy for the callback
            order_data = self.orders[order_id].copy()
        
        # Call the status callback if registered
        if order_id in self.order_status_callbacks:
            try:
                self.order_status_callbacks[order_id](order_id, order_data)
            except Exception as e:
                logger.error(f"Error in order status callback: {e}")
    
    def execDetails(self, req_id: int, contract: Contract, execution: Execution) -> None:
        """Called when an execution occurs."""
        super().execDetails(req_id, contract, execution)
        
        order_id = execution.orderId
        exec_id = execution.execId
        
        logger.info(f"Execution for order {order_id}: {execution.shares} shares at ${execution.price}")
        
        # Store execution details
        exec_details = {
            'exec_id': exec_id,
            'order_id': order_id,
            'time': execution.time,
            'account': execution.acctNumber,
            'exchange': execution.exchange,
            'side': execution.side,
            'shares': execution.shares,
            'price': execution.price,
            'perm_id': execution.permId,
            'client_id': execution.clientId,
            'liquidation': execution.liquidation,
            'cum_qty': execution.cumQty,
            'avg_price': execution.avgPrice
        }
        
        # Track executions
        with self.orders_lock:
            self.executions[exec_id] = exec_details
            
            if order_id in self.orders:
                self.orders[order_id]['executions'].append(exec_details)
            
            # Get a copy for the callback
            if order_id in self.orders:
                order_data = self.orders[order_id].copy()
            else:
                order_data = None
        
        # Call the execution callback if registered
        if order_id in self.execution_callbacks:
            try:
                self.execution_callbacks[order_id](order_id, exec_details, order_data)
            except Exception as e:
                logger.error(f"Error in execution callback: {e}")
    
    def commissionReport(self, commission_report: CommissionReport) -> None:
        """Called when commission information is received."""
        super().commissionReport(commission_report)
        
        exec_id = commission_report.execId
        
        logger.info(f"Commission for execution {exec_id}: ${commission_report.commission}")
        
        # Add commission info to the execution record
        if exec_id in self.executions:
            self.executions[exec_id]['commission'] = commission_report.commission
            self.executions[exec_id]['commission_currency'] = commission_report.currency
            self.executions[exec_id]['realized_pnl'] = commission_report.realizedPNL

# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create and connect the order manager
    order_manager = IBKROrderManager()
    
    try:
        order_manager.connect_and_run()
        
        # Define callbacks
        def on_order_status(order_id, order_data):
            print(f"Order {order_id} status update: {order_data['status']}")
            print(f"Filled: {order_data['filled_quantity']} @ ${order_data['avg_fill_price']}")
            print(f"Remaining: {order_data['remaining_quantity']}")
            print("")
        
        def on_execution(order_id, execution, order_data):
            print(f"Execution for order {order_id}:")
            print(f"  {execution['shares']} shares @ ${execution['price']}")
            print(f"  Exchange: {execution['exchange']}")
            print(f"  Time: {execution['time']}")
            print("")
        
        # Create order specifications
        # For demonstration, we'll create a small limit order that's unlikely to execute immediately
        symbol = "AAPL"  # Example using Apple
        
        # Get current price first
        print(f"Getting current price for {symbol}...")
        current_price = order_manager.get_last_price(symbol)
        if current_price is None:
            print(f"Could not get current price for {symbol}")
            exit(1)
        
        print(f"Current price: ${current_price}")
        
        # Create a buy limit order 5% below current price
        limit_price = round(current_price * 0.95, 2)
        contract, order = order_manager.create_limit_order(
            symbol=symbol,
            quantity=1,  # Just 1 share for testing
            action="BUY",
            limit_price=limit_price
        )
        
        # Place the order
        print(f"Placing limit order to buy 1 share of {symbol} at ${limit_price}...")
        order_id = order_manager.place_order(
            contract=contract,
            order=order,
            status_callback=on_order_status,
            execution_callback=on_execution
        )
        
        print(f"Order placed with ID: {order_id}")
        
        # Wait for a moment to let order processing happen
        time.sleep(5)
        
        # Check order status
        status = order_manager.get_order_status(order_id)
        print(f"Current status: {status['status']}")
        
        # Cancel the order after 10 seconds if it's still open
        print("Waiting 10 seconds before cancelling the order...")
        time.sleep(10)
        
        # Check again if order is still open
        status = order_manager.get_order_status(order_id)
        if not status['is_complete']:
            print(f"Cancelling order {order_id}...")
            order_manager.cancel_order(order_id)
            
            # Wait for cancellation to process
            time.sleep(2)
            
            # Verify cancellation
            status = order_manager.get_order_status(order_id)
            print(f"Final status: {status['status']}")
        else:
            print(f"Order is already complete with status: {status['status']}")
        
        # Show open orders if any
        open_orders = order_manager.get_open_orders()
        if open_orders:
            print(f"Open orders: {open_orders}")
        else:
            print("No open orders")
        
        # Keep the program running for a while to receive and process messages
        print("\nRunning for 30 more seconds to process any messages...\n")
        time.sleep(30)
        
    finally:
        # Disconnect when done
        order_manager.disconnect_and_stop()