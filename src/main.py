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

# Import the new configuration system
from src.config.config_manager import ConfigManager, get_config
from src.connectors.ibkr.client import IBKRClient

def setup_logging(config_manager: ConfigManager):
    """Setup logging configuration from the config manager."""
    log_config = config_manager.get_log_config()
    
    # Get configuration values
    log_level = getattr(logging, log_config.get('level', 'INFO').upper())
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_enabled = log_config.get('console_enabled', True)
    file_enabled = log_config.get('file_enabled', True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
    
    # File handlers
    if file_enabled:
        log_paths = log_config.get('paths', {})
        rotation_config = log_config.get('rotation', {})
        max_bytes = rotation_config.get('max_bytes', 10*1024*1024)  # 10MB
        backup_count = rotation_config.get('backup_count', 5)
        
        # System log file handler
        system_log_path = log_paths.get('system_logs', 'logs/system/')
        system_log_file = os.path.join(system_log_path, 'system.log')
        
        system_file_handler = RotatingFileHandler(
            system_log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        system_file_handler.setLevel(log_level)
        system_formatter = logging.Formatter(log_format)
        system_file_handler.setFormatter(system_formatter)
        root_logger.addHandler(system_file_handler)
        
        # Trade log file handler
        trade_logger = logging.getLogger('trades')
        trade_log_path = log_paths.get('trade_logs', 'logs/trades/')
        trade_log_file = os.path.join(trade_log_path, 'trades.log')
        
        trade_file_handler = RotatingFileHandler(
            trade_log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
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
                        help='Path to the configuration file (default: config.json)')
    
    parser.add_argument('-m', '--mode', 
                        choices=['gateway_paper', 'gateway_live', 'tws_paper', 'tws_live'], 
                        help='IBKR connection mode (overrides config file)')
    
    parser.add_argument('-s', '--strategy', 
                        help='Strategy to use')
    
    parser.add_argument('-t', '--test-connection', 
                        action='store_true',
                        help='Test the connection to IBKR and exit')
    
    parser.add_argument('-v', '--verbose', 
                        action='store_true',
                        help='Enable verbose logging')
                        
    parser.add_argument('-d', '--use-delayed-data',
                       action='store_true',
                       help='Use delayed market data when real-time is not available')
                       
    parser.add_argument('--paper', 
                        action='store_true',
                        help='Force paper trading mode')
                        
    parser.add_argument('--live', 
                        action='store_true',
                        help='Force live trading mode (USE WITH CAUTION!)')
                        
    return parser.parse_args()

def test_connection(config_manager: ConfigManager, mode: str, use_delayed_data: bool, logger):
    """Test connection to IBKR."""
    logger.info("Testing connection to IBKR")
    
    # Get connection info from config
    conn_info = config_manager.get_ibkr_connection_info(mode)
    host = conn_info['host']
    port = conn_info['port']
    client_id = conn_info['client_id']
    
    logger.info(f"Testing connection to {host}:{port} (mode: {mode}) with client ID {client_id}")
    
    # Create client with no auto-reconnect for testing
    client = IBKRClient(
        host=host, 
        port=port, 
        client_id=client_id, 
        auto_reconnect=False
    )
    
    try:
        # Test basic connection
        client.connect_and_run()
        time.sleep(3)  # Wait for connection to establish
        
        if client.connected:
            logger.info("✓ Successfully connected to IBKR!")
            
            # Test market data
            logger.info("Testing market data connection...")
            from src.connectors.ibkr.data_feed import IBKRDataFeed
            
            data_feed = IBKRDataFeed(
                host=host, 
                port=port, 
                client_id=client_id + 1, 
                use_delayed_data=use_delayed_data
            )
            data_feed.connect_and_run()
            
            # Try to get a test price
            test_symbol = "AAPL"
            logger.info(f"Getting price for {test_symbol}...")
            
            price = data_feed.get_last_price(test_symbol)
            if price:
                # Check if delayed data is being used
                is_delayed = False
                for req_id, data in data_feed.market_data.items():
                    if data['symbol'] == test_symbol:
                        is_delayed = data.get('is_delayed', False)
                        break
                
                data_type = "DELAYED" if is_delayed else "REAL-TIME"
                logger.info(f"✓ Current {data_type} price of {test_symbol}: ${price}")
            else:
                logger.warning(f"⚠ Could not get price for {test_symbol}")
            
            # Clean up
            data_feed.disconnect_and_stop()
            logger.info("✓ Market data test completed")
            
        else:
            logger.error(f"✗ Failed to connect to IBKR on {host}:{port}")
            
            # Try alternative modes
            alternative_modes = ['gateway_paper', 'tws_paper', 'gateway_live', 'tws_live']
            alternative_modes = [m for m in alternative_modes if m != mode]
            
            for alt_mode in alternative_modes:
                logger.info(f"Trying alternative mode: {alt_mode}")
                alt_conn_info = config_manager.get_ibkr_connection_info(alt_mode)
                
                client.disconnect_and_stop()
                client = IBKRClient(
                    host=alt_conn_info['host'],
                    port=alt_conn_info['port'],
                    client_id=alt_conn_info['client_id'],
                    auto_reconnect=False
                )
                
                client.connect_and_run()
                time.sleep(3)
                
                if client.connected:
                    logger.info(f"✓ Successfully connected using {alt_mode} mode!")
                    logger.info(f"Consider updating your config to use mode: {alt_mode}")
                    break
            else:
                logger.error("✗ Failed to connect with any available mode")
                logger.error("Please ensure IBKR TWS or Gateway is running with API enabled")
        
    except Exception as e:
        logger.error(f"✗ Connection test failed: {e}")
    
    finally:
        # Always clean up
        client.disconnect_and_stop()
        logger.info("Connection test completed")

def main():
    """Main entry point for the trading bot."""
    args = parse_arguments()
    
    # Initialize configuration manager
    config_manager = ConfigManager(args.config)
    
    # Setup logging
    logger = setup_logging(config_manager)
    
    # Set verbose logging if requested
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    logger.info("Starting IKBR Trading Bot")
    logger.info(f"Configuration loaded from: {config_manager.config_path}")
    
    # Determine trading mode
    if args.live and args.paper:
        logger.error("Cannot specify both --live and --paper modes")
        return
    
    # Override trading mode if specified
    if args.live:
        config_manager.update_config('trading.paper_trading', False)
        logger.warning("🚨 LIVE TRADING MODE ENABLED - Real money will be used! 🚨")
    elif args.paper:
        config_manager.update_config('trading.paper_trading', True)
        logger.info("📝 Paper trading mode enabled")
    
    # Determine IBKR connection mode
    if args.mode:
        ibkr_mode = args.mode
    else:
        # Auto-select mode based on paper trading setting
        is_paper = config_manager.is_paper_trading()
        default_mode = config_manager.get('ibkr.default_mode', 'gateway_paper')
        
        if is_paper and 'live' in default_mode:
            ibkr_mode = default_mode.replace('live', 'paper')
        elif not is_paper and 'paper' in default_mode:
            ibkr_mode = default_mode.replace('paper', 'live')
        else:
            ibkr_mode = default_mode
    
    logger.info(f"Using IBKR connection mode: {ibkr_mode}")
    
    # Handle test connection
    if args.test_connection:
        test_connection(config_manager, ibkr_mode, args.use_delayed_data, logger)
        return
    
    # Initialize data harvester if enabled
    harvester_manager = None
    if config_manager.get('data_harvesting.enabled', False):
        logger.info("Initializing data harvester")
        try:
            from src.data.harvester_manager import HarvesterManager
            harvester_manager = HarvesterManager()
            if harvester_manager.start():
                logger.info("✓ Data harvester started successfully")
            else:
                logger.warning("⚠ Data harvester failed to start")
                harvester_manager = None
        except Exception as e:
            logger.error(f"✗ Error starting harvester: {e}")
            harvester_manager = None
    
    # Get connection information
    conn_info = config_manager.get_ibkr_connection_info(ibkr_mode)
    
    # Set up signal handling for graceful shutdown
    running = threading.Event()
    running.set()
    
    def signal_handler(sig, frame):
        logger.info("Shutdown signal received")
        running.clear()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize IBKR components
    from src.connectors.ibkr.data_feed import IBKRDataFeed
    from src.connectors.ibkr.order_manager import IBKROrderManager
    
    # Use different client IDs for different components
    client_ids = config_manager.get('ibkr.client_ids', {})
    
    data_feed = IBKRDataFeed(
        host=conn_info['host'],
        port=conn_info['port'],
        client_id=client_ids.get('data_feed', 2),
        use_delayed_data=args.use_delayed_data or config_manager.get('market_data.use_delayed_data', True)
    )
    
    order_manager = IBKROrderManager(
        host=conn_info['host'],
        port=conn_info['port'],
        client_id=client_ids.get('order_manager', 3)
    )
    
    try:
        # Connect to IBKR
        logger.info(f"Connecting to IBKR at {conn_info['host']}:{conn_info['port']}")
        
        # Connect data feed
        data_feed.connect_and_run()
        if not data_feed.connected:
            logger.error("Failed to connect data feed to IBKR")
            return
        
        # Connect order manager
        order_manager.connect_and_run()
        if not order_manager.connected:
            logger.error("Failed to connect order manager to IBKR")
            data_feed.disconnect_and_stop()
            return
        
        logger.info("✓ Successfully connected to IBKR")
        
        # Test market data
        test_symbol = "AAPL"
        logger.info(f"Testing market data with {test_symbol}")
        
        price = data_feed.get_last_price(test_symbol)
        if price:
            # Check data type
            is_delayed = any(
                data.get('is_delayed', False) 
                for data in data_feed.market_data.values() 
                if data.get('symbol') == test_symbol
            )
            data_type = "DELAYED" if is_delayed else "REAL-TIME"
            logger.info(f"✓ Current {data_type} price of {test_symbol}: ${price}")
        else:
            logger.warning(f"⚠ Could not get price for {test_symbol}")
        
        # Main bot loop
        logger.info("🤖 Bot is now running. Press Ctrl+C to exit.")
        
        heartbeat_interval = 60  # seconds
        last_heartbeat = time.time()
        
        while running.is_set():
            current_time = time.time()
            
            # Heartbeat
            if current_time - last_heartbeat >= heartbeat_interval:
                logger.info("💓 Bot heartbeat - system running normally")
                last_heartbeat = current_time
            
            time.sleep(1)
        
        logger.info("Shutdown initiated")
    
    except Exception as e:
        logger.exception(f"Error in main loop: {e}")
    
    finally:
        # Clean shutdown
        logger.info("🛑 Shutting down...")
        
        try:
            if harvester_manager:
                logger.info("Stopping data harvester")
                harvester_manager.stop()
                
            logger.info("Disconnecting from IBKR")
            order_manager.disconnect_and_stop()
            data_feed.disconnect_and_stop()
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        
        logger.info("✓ Bot shut down successfully")

if __name__ == "__main__":
    main()