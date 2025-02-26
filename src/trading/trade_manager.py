"""
Trade manager module for handling trade execution at a high level.
Integrates with the order manager and provides trade tracking functionality.
"""
import logging
from typing import Dict, List, Optional, Any

# Set up logger
logger = logging.getLogger(__name__)

class TradeManager:
    """
    Manages trade execution and tracking at a higher level than the order manager.
    Coordinates with strategies and tracks trade statistics.
    """
    
    def __init__(self, order_manager, risk_manager=None):
        """
        Initialize the trade manager.
        
        Args:
            order_manager: The order manager to use for executing trades
            risk_manager: Optional risk manager for trade validation
        """
        self.order_manager = order_manager
        self.risk_manager = risk_manager
        self.active_trades = {}
        self.trade_history = []
        
    def place_trade(self, strategy_id, symbol, direction, quantity, order_type="MARKET", price=None, stop_price=None):
        """
        Place a trade for a strategy.
        
        Args:
            strategy_id: ID of the strategy placing the trade
            symbol: The stock symbol
            direction: "BUY" or "SELL"
            quantity: Number of shares
            order_type: Type of order (e.g., "MARKET", "LIMIT", "STOP")
            price: Limit price (required for LIMIT orders)
            stop_price: Stop price (required for STOP orders)
            
        Returns:
            The trade ID if successful
        """
        logger.info(f"Strategy {strategy_id} requesting to {direction} {quantity} shares of {symbol}")
        
        # Create a trade record
        trade_id = self._generate_trade_id()
        
        # Track the trade
        trade = {
            'id': trade_id,
            'strategy_id': strategy_id,
            'symbol': symbol,
            'direction': direction,
            'quantity': quantity,
            'order_type': order_type,
            'price': price,
            'stop_price': stop_price,
            'status': 'PENDING',
            'order_id': None,
            'fill_price': None,
            'commission': None,
            'timestamp': None
        }
        
        self.active_trades[trade_id] = trade
        
        # Place the order through the order manager
        try:
            # Implementation depends on your order manager interface
            if order_type == "MARKET":
                order_id = self.order_manager.place_market_order(symbol, direction, quantity)
            elif order_type == "LIMIT" and price is not None:
                order_id = self.order_manager.place_limit_order(symbol, direction, quantity, price)
            elif order_type == "STOP" and stop_price is not None:
                order_id = self.order_manager.place_stop_order(symbol, direction, quantity, stop_price)
            else:
                raise ValueError(f"Invalid order type or missing required parameters: {order_type}")
            
            # Update the trade record with the order ID
            trade['order_id'] = order_id
            trade['status'] = 'OPEN'
            
            logger.info(f"Trade {trade_id} placed successfully, order ID: {order_id}")
            return trade_id
            
        except Exception as e:
            logger.error(f"Failed to place trade: {e}")
            trade['status'] = 'FAILED'
            self.trade_history.append(trade)
            self.active_trades.pop(trade_id, None)
            raise
    
    def update_trade(self, order_id, status, fill_price=None, filled_quantity=None, commission=None):
        """
        Update a trade based on order status changes.
        
        Args:
            order_id: The order ID to update
            status: New status of the order
            fill_price: Execution price if filled
            filled_quantity: Quantity filled
            commission: Trade commission
        """
        # Find the trade by order ID
        for trade_id, trade in self.active_trades.items():
            if trade['order_id'] == order_id:
                # Update trade information
                trade['status'] = status
                
                if fill_price is not None:
                    trade['fill_price'] = fill_price
                
                if commission is not None:
                    trade['commission'] = commission
                
                # If the trade is complete, move it to history
                if status in ['FILLED', 'CANCELLED', 'REJECTED']:
                    logger.info(f"Trade {trade_id} is now {status}")
                    
                    # Make a copy of the trade for history
                    self.trade_history.append(trade.copy())
                    
                    # Remove from active trades if fully filled
                    if status == 'FILLED':
                        self.active_trades.pop(trade_id, None)
                
                return trade_id
        
        logger.warning(f"No active trade found for order ID {order_id}")
        return None
    
    def cancel_trade(self, trade_id):
        """
        Cancel a trade.
        
        Args:
            trade_id: The trade ID to cancel
            
        Returns:
            True if successfully cancelled, False otherwise
        """
        if trade_id not in self.active_trades:
            logger.warning(f"No active trade found with ID {trade_id}")
            return False
        
        trade = self.active_trades[trade_id]
        
        if trade['status'] not in ['OPEN', 'PENDING']:
            logger.warning(f"Cannot cancel trade {trade_id} with status {trade['status']}")
            return False
        
        # Cancel through the order manager
        try:
            self.order_manager.cancel_order(trade['order_id'])
            logger.info(f"Cancellation requested for trade {trade_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel trade {trade_id}: {e}")
            return False
    
    def get_trade(self, trade_id):
        """Get a trade by ID from either active trades or history."""
        # Check active trades first
        if trade_id in self.active_trades:
            return self.active_trades[trade_id]
        
        # Check trade history
        for trade in self.trade_history:
            if trade['id'] == trade_id:
                return trade
        
        return None
    
    def get_trades_by_strategy(self, strategy_id):
        """Get all trades (active and historical) for a strategy."""
        strategy_trades = []
        
        # Check active trades
        for trade in self.active_trades.values():
            if trade['strategy_id'] == strategy_id:
                strategy_trades.append(trade)
        
        # Check trade history
        for trade in self.trade_history:
            if trade['strategy_id'] == strategy_id:
                strategy_trades.append(trade)
        
        return strategy_trades
    
    def _generate_trade_id(self):
        """Generate a unique trade ID."""
        import uuid
        return str(uuid.uuid4())