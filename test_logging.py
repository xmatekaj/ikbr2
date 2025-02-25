"""
Test script for logging setup
"""
import os
import logging
from logging.handlers import RotatingFileHandler

print("Starting logging test")

try:
    # From src/main.py
    def setup_logging():
        """Setup logging configuration."""
        print("Setting up logging...")
        
        # Create log directories if they don't exist
        system_log_dir = os.path.join('logs', 'system')
        trade_log_dir = os.path.join('logs', 'trades')
        
        print(f"System log dir: {system_log_dir}")
        print(f"Trade log dir: {trade_log_dir}")
        
        os.makedirs(system_log_dir, exist_ok=True)
        os.makedirs(trade_log_dir, exist_ok=True)
        
        # Configure root logger
        log_level = logging.INFO
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # System log file handler
        system_file_handler = RotatingFileHandler(
            os.path.join(system_log_dir, 'system.log'),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        system_file_handler.setLevel(log_level)
        system_formatter = logging.Formatter(log_format)
        system_file_handler.setFormatter(system_formatter)
        root_logger.addHandler(system_file_handler)
        
        # Trade log file handler
        trade_logger = logging.getLogger('trades')
        trade_logger.setLevel(log_level)
        trade_file_handler = RotatingFileHandler(
            os.path.join(trade_log_dir, 'trades.log'),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=10
        )
        trade_file_handler.setLevel(log_level)
        trade_formatter = logging.Formatter(log_format)
        trade_file_handler.setFormatter(trade_formatter)
        trade_logger.addHandler(trade_file_handler)
        
        print("Logging setup completed")
        return root_logger

    # Set up logging
    logger = setup_logging()
    
    # Test the logger
    print("Testing logger...")
    logger.info("This is an info message from the root logger")
    logger.warning("This is a warning message from the root logger")
    
    # Test trade logger
    trade_logger = logging.getLogger('trades')
    trade_logger.info("This is an info message from the trade logger")
    
    print("Logging test completed successfully")

except Exception as e:
    print(f"Error in logging setup: {e}")
    import traceback
    traceback.print_exc()

print("Test script completed")