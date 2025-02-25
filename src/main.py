"""
Main entry point for the IBKR trading bot.
"""
import os
import sys
import time
import signal
import argparse
import logging
from logging.handlers import RotatingFileHandler
import threading

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import modules
from src.config.settings import default_settings
from src.connectors.ibkr.client import IBKRClient
from src.connectors.ibkr.data_feed import IBKRDataFeed
from src.connectors.ibkr.order_manager import IBKROrderManager
from src.core.bot_manager import BotManager  # We'll implement this later

def setup_logging():
    """Setup logging configuration."""
    # Create log directories if they don't exist
    system_log_dir = os.path.dirname(default_settings.get('SYSTEM_LOG_FILE'))
    trade_log_dir = os.path.dirname(default_settings.get('TRADE_LOG_FILE'))
    
    os.makedirs(system_log_dir, exist_ok=True)
    os.makedirs(trade_log_dir, exist_ok=True)
    
    # Configure root logger
    log_level = default_settings.get('LOG_LEVEL')
    log_format = default_settings.get('LOG_FORMAT')
    
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
        default_settings.get('SYSTEM_LOG_FILE'),
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
        default_settings.get('TRADE_LOG_FILE'),
        maxBytes=10*1024*1024,  # 10MB
        backupCount=10
    )
    trade_file_handler.setLevel(log_level)
    trade_formatter = logging.Formatter(log_format)
    trade_file_handler.setFormatter(trade_formatter)
    trade_logger.addHandler(trade_file_handler)
    
    return root_logger

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='IBKR Trading Bot')
    
    parser.add_argument('-c', '--config', 
                        help='Path to the configuration file')
    
    parser.add_argument('-m', '--mode', 
                        choices=['live', 'paper', 'backtest'], 
                        default='paper',
                        help='Trading mode (live, paper, or backtest)')
    
    parser.add_argument('-s', '--strategy', 
                        help='Strategy to use')
    
    parser.add_argument('-t', '--test-connection', 
                        action='store_true',
                        help='Test the connection to IBKR and exit')
    
    parser.add_argument('-v', '--verbose', 
                        action='store_true',
                        help='Enable verbose logging')
                        
    return parser.parse_args()

def main():
    """Main entry point for the trading bot."""
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging()
    
    # Set log level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    logger.info("Starting IBKR Trading Bot")
    
    # Load custom configuration if provided
    if args.config:
        from src.config.settings import Settings
        logger.info(f"Loading configuration from {args.config}")
        settings = Settings(args.config)
    else:
        settings = default_settings
    
    # Configure IBKR connection based on mode
    host = settings.get('IBKR_HOST')
    if args.mode == 'live':
        port = 7496  # Default port for TWS Live
        logger.warning("LIVE TRADING MODE ENABLED - Real money will be used!")
    else:  # paper or backtest
        port = 7497  # Default port for TWS Paper Trading
        logger.info("Paper trading mode enabled")
    
    client_id = settings.get('IBKR_CLIENT_ID')
    
    # Test connection only if requested
    if args.test_connection:
        logger.info("Testing connection to IBKR")
        client = IBKRClient(host=host, port=port, client_id=client_id, auto_reconnect=False)
        
        try:
            client.connect_and_run()
            time.sleep(2)  # Wait a bit for connection messages
            
            if client.connected:
                logger.info("Successfully connected to IBKR")
                # Get account information
                req_id = client.request_account_summary()
                time.sleep(3)  # Wait for data
                
                account_summary = client.get_account_summary_result(req_id)
                for item in account_summary:
                    logger.info(f"Account {item['account']}: {item['tag']} = {item['value']} {item['currency']}")
            else:
                logger.error("Failed to connect to IBKR")
            
            # Keep connection open a bit longer to get full responses
            logger.info("Waiting for responses...")
            time.sleep(5)
            
            client.disconnect_and_stop()
        except Exception as e:
            logger.error(f"Error testing connection: {e}")
        
        logger.info("Connection test completed")
        return
    
    # Set up signal handling for graceful shutdown
    running = threading.Event()
    running.set()
    
    def signal_handler(sig, frame):
        logger.info("Shutdown signal received")
        running.clear()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create necessary components
    data_feed = IBKRDataFeed(host=host, port=port, client_id=client_id)
    order_manager = IBKROrderManager(host=host, port=port, client_id=client_id+1)
    
    try:
        # Connect to IBKR
        logger.info(f"Connecting to IBKR at {host}:{port}")
        
        # First connect data feed
        data_feed.connect_and_run()
        if not data_feed.connected:
            logger.error("Failed to connect data feed to IBKR")
            return
        
        # Then connect order manager with a different client ID
        order_manager.connect_and_run()
        if not order_manager.connected:
            logger.error("Failed to connect order manager to IBKR")
            data_feed.disconnect_and_stop()
            return
        
        logger.info("Successfully connected to IBKR")
        
        # Paper trading test - get current price of a symbol
        test_symbol = "AAPL"
        logger.info(f"Testing market data by getting price for {test_symbol}")
        
        price = data_feed.get_last_price(test_symbol)
        if price:
            logger.info(f"Current price of {test_symbol}: ${price}")
        else:
            logger.warning(f"Could not get price for {test_symbol}")
        
        # For now, we'll implement a simple loop that keeps the bot running
        # In a full implementation, we'd initialize the BotManager here
        logger.info("Bot is now running. Press Ctrl+C to exit.")
        
        # Simple heartbeat loop
        heartbeat_interval = 60  # seconds
        last_heartbeat = time.time()
        
        while running.is_set():
            current_time = time.time()
            
            # Heartbeat log every minute
            if current_time - last_heartbeat >= heartbeat_interval:
                logger.info("Bot heartbeat - still running")
                last_heartbeat = current_time
            
            # Sleep a bit to avoid CPU spinning
            time.sleep(1)
        
        logger.info("Shutdown initiated")
    
    except Exception as e:
        logger.exception(f"Error in main loop: {e}")
    
    finally:
        # Clean shutdown
        logger.info("Disconnecting from IBKR")
        try:
            order_manager.disconnect_and_stop()
            data_feed.disconnect_and_stop()
        except Exception as e:
            logger.error(f"Error during disconnection: {e}")
        
        logger.info("Bot shut down successfully")

if __name__ == "__main__":
    main()