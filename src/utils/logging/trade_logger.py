"""
Trade logger for the IKBR Trader Bot.

This module provides specialized logging functions for trade-related events.
"""
import logging
import datetime
from logging.handlers import RotatingFileHandler
import os
import json
from typing import Dict, Any, Optional


class TradeLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds strategy identifier to log records.
    """
    def process(self, msg, kwargs):
        """
        Add strategy information to log record.
        
        Args:
            msg: Log message
            kwargs: Keyword arguments for the log record
            
        Returns:
            Tuple of modified message and keyword arguments
        """
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        if 'strategy' not in kwargs['extra']:
            kwargs['extra']['strategy'] = self.extra.get('strategy', 'UNKNOWN')
        return msg, kwargs


class TradeLogger:
    """
    Specialized logger for trading activities.
    
    This class handles logging of trades, orders, and other trading-related
    events with appropriate context and formatting.
    """
    
    def __init__(self, strategy_id: str, log_dir: str = 'logs/trades'):
        """
        Initialize a trade logger for a specific strategy.
        
        Args:
            strategy_id: Identifier for the strategy
            log_dir: Directory for log files
        """
        self.strategy_id = strategy_id
        self.log_dir = log_dir
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Generate log file name with strategy ID and timestamp
        timestamp = datetime.datetime.now().strftime('%Y%m%d')
        self.log_file = os.path.join(log_dir, f'{strategy_id}_{timestamp}.log')
        
        # Create logger
        logger = logging.getLogger(f'ikbr_trader.trades.{strategy_id}')
        logger.setLevel(logging.INFO)
        
        # Clear any existing handlers
        logger.handlers = []
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(strategy)s] - %(message)s')
        
        # Set up file handler
        file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=10*1024*1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Set up console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Create adapter to add strategy context
        self.logger = TradeLoggerAdapter(logger, {'strategy': strategy_id})
        
        self.logger.info(f"Trade logger initialized for strategy '{strategy_id}'")
    
    def log_trade_entry(self, 
                       symbol: str, 
                       quantity: int, 
                       price: float, 
                       trade_type: str,
                       trade_id: str, 
                       extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a trade entry event.
        
        Args:
            symbol: Symbol being traded
            quantity: Number of shares/contracts
            price: Entry price
            trade_type: Type of trade (e.g., 'LONG', 'SHORT')
            trade_id: Unique identifier for the trade
            extra_info: Additional trade information
        """
        info = {
            'event': 'TRADE_ENTRY',
            'symbol': symbol,
            'quantity': quantity,
            'price': price,
            'type': trade_type,
            'trade_id': trade_id,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if extra_info:
            info.update(extra_info)
        
        self.logger.info(f"ENTRY: {symbol} {trade_type} {quantity} @ ${price:.2f} [ID: {trade_id}]", 
                        extra={'trade_data': info})
    
    def log_trade_exit(self, 
                      symbol: str, 
                      quantity: int, 
                      price: float, 
                      profit_loss: float,
                      profit_loss_pct: float,
                      trade_id: str, 
                      extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a trade exit event.
        
        Args:
            symbol: Symbol being traded
            quantity: Number of shares/contracts
            price: Exit price
            profit_loss: Profit or loss amount
            profit_loss_pct: Profit or loss percentage
            trade_id: Unique identifier for the trade
            extra_info: Additional trade information
        """
        info = {
            'event': 'TRADE_EXIT',
            'symbol': symbol,
            'quantity': quantity,
            'price': price,
            'profit_loss': profit_loss,
            'profit_loss_pct': profit_loss_pct,
            'trade_id': trade_id,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if extra_info:
            info.update(extra_info)
        
        pl_str = f"+${profit_loss:.2f}" if profit_loss >= 0 else f"-${abs(profit_loss):.2f}"
        pl_pct_str = f"+{profit_loss_pct:.2f}%" if profit_loss_pct >= 0 else f"-{abs(profit_loss_pct):.2f}%"
        
        self.logger.info(f"EXIT: {symbol} {quantity} @ ${price:.2f} P&L: {pl_str} ({pl_pct_str}) [ID: {trade_id}]", 
                        extra={'trade_data': info})
    
    def log_order_submitted(self, 
                           order_id: str, 
                           symbol: str, 
                           quantity: int, 
                           order_type: str,
                           price: Optional[float] = None, 
                           extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log an order submission event.
        
        Args:
            order_id: Order identifier
            symbol: Symbol being traded
            quantity: Number of shares/contracts
            order_type: Type of order (e.g., 'MARKET', 'LIMIT')
            price: Order price (for limit orders)
            extra_info: Additional order information
        """
        info = {
            'event': 'ORDER_SUBMITTED',
            'order_id': order_id,
            'symbol': symbol,
            'quantity': quantity,
            'order_type': order_type,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if price is not None:
            info['price'] = price
        
        if extra_info:
            info.update(extra_info)
        
        price_str = f" @ ${price:.2f}" if price is not None else ""
        self.logger.info(f"ORDER SUBMITTED: {symbol} {quantity} {order_type}{price_str} [ID: {order_id}]", 
                        extra={'order_data': info})
    
    def log_order_filled(self, 
                        order_id: str, 
                        symbol: str, 
                        quantity: int, 
                        price: float,
                        extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log an order fill event.
        
        Args:
            order_id: Order identifier
            symbol: Symbol being traded
            quantity: Number of shares/contracts filled
            price: Fill price
            extra_info: Additional fill information
        """
        info = {
            'event': 'ORDER_FILLED',
            'order_id': order_id,
            'symbol': symbol,
            'quantity': quantity,
            'price': price,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if extra_info:
            info.update(extra_info)
        
        self.logger.info(f"ORDER FILLED: {symbol} {quantity} @ ${price:.2f} [ID: {order_id}]", 
                        extra={'order_data': info})
    
    def log_order_canceled(self, 
                          order_id: str, 
                          symbol: str,
                          reason: str = 'UNKNOWN',
                          extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log an order cancellation event.
        
        Args:
            order_id: Order identifier
            symbol: Symbol being traded
            reason: Reason for cancellation
            extra_info: Additional cancellation information
        """
        info = {
            'event': 'ORDER_CANCELED',
            'order_id': order_id,
            'symbol': symbol,
            'reason': reason,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if extra_info:
            info.update(extra_info)
        
        self.logger.info(f"ORDER CANCELED: {symbol} - Reason: {reason} [ID: {order_id}]", 
                        extra={'order_data': info})
    
    def log_strategy_update(self, 
                           message: str, 
                           extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a strategy update event.
        
        Args:
            message: The update message
            extra_info: Additional update information
        """
        info = {
            'event': 'STRATEGY_UPDATE',
            'message': message,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if extra_info:
            info.update(extra_info)
        
        self.logger.info(f"STRATEGY: {message}", extra={'strategy_data': info})
    
    def log_error(self, 
                 message: str,
                 error: Optional[Exception] = None,
                 extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log an error event.
        
        Args:
            message: Error description
            error: Exception object
            extra_info: Additional error information
        """
        info = {
            'event': 'ERROR',
            'message': message,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if error:
            info['error_type'] = type(error).__name__
            info['error_msg'] = str(error)
        
        if extra_info:
            info.update(extra_info)
        
        error_detail = f": {str(error)}" if error else ""
        self.logger.error(f"ERROR: {message}{error_detail}", extra={'error_data': info})
    
    def log_warning(self, 
                   message: str,
                   extra_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a warning event.
        
        Args:
            message: Warning description
            extra_info: Additional warning information
        """
        info = {
            'event': 'WARNING',
            'message': message,
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if extra_info:
            info.update(extra_info)
        
        self.logger.warning(f"WARNING: {message}", extra={'warning_data': info})
    
    def log_to_file(self, data: Dict[str, Any], event_type: str) -> None:
        """
        Write structured data to a JSON log file.
        
        Args:
            data: Data to log
            event_type: Type of event (for filename)
        """
        # Create event logs directory if it doesn't exist
        event_dir = os.path.join(self.log_dir, 'events')
        os.makedirs(event_dir, exist_ok=True)
        
        # Add timestamp if not present
        if 'timestamp' not in data:
            data['timestamp'] = datetime.datetime.now().isoformat()
        
        # Generate filename with timestamp and event type
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        filename = f"{self.strategy_id}_{event_type}_{timestamp}.json"
        file_path = os.path.join(event_dir, filename)
        
        # Write data to file
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to write event log to file: {e}")