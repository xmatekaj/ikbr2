"""
Global settings for the IBKR trading bot.
"""
import os
import json
from typing import Dict, Any, Optional
import logging

# Logging configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
SYSTEM_LOG_FILE = os.path.join('logs', 'system', 'system.log')
TRADE_LOG_FILE = os.path.join('logs', 'trades', 'trades.log')

# IBKR connection settings
IBKR_HOST = "127.0.0.1"  # TWS/IB Gateway host
IBKR_PORT = 7497         # 7497 for TWS Paper, 4002 for IB Gateway Paper
IBKR_CLIENT_ID = 1       # Client ID for this bot
IBKR_ACCOUNT = ""        # Account number (if empty, will use the first available)
IBKR_TIMEOUT = 20        # Connection timeout in seconds
IBKR_AUTO_RECONNECT = True  # Automatically attempt to reconnect if disconnected

# Trading parameters
MAX_POSITIONS = 5           # Maximum number of concurrent positions
MAX_POSITION_SIZE = 0.1     # Maximum position size as fraction of account value
DEFAULT_ORDER_TYPE = "MKT"  # Default order type ("MKT", "LMT", "STP", etc.)
ENABLE_STOP_LOSS = True     # Whether to use stop-loss orders
STOP_LOSS_PERCENT = 0.05    # Stop-loss percentage (e.g., 0.05 = 5% below entry)
ENABLE_TAKE_PROFIT = True   # Whether to use take-profit orders
TAKE_PROFIT_PERCENT = 0.1   # Take-profit percentage (e.g., 0.1 = 10% above entry)

# Data settings
HISTORICAL_DATA_DIR = "historical_data"  # Directory for storing historical data
PRICE_BAR_SIZE = "1 min"                 # Default bar size for price data
PRICE_DURATION = "1 D"                   # Default lookback duration

# Strategy parameters
STRATEGY_CONFIG_DIR = os.path.join("src", "config", "strategy_configs")  # Strategy configuration directory

# Trading schedule
TRADING_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
MARKET_OPEN_TIME = "09:30"   # Market open time (Eastern Time)
MARKET_CLOSE_TIME = "16:00"  # Market close time (Eastern Time)
PRE_MARKET_START = "08:00"   # Pre-market start time (Eastern Time)
AFTER_MARKET_END = "20:00"   # After-market end time (Eastern Time)

# Performance tracking
TRACK_PERFORMANCE = True     # Whether to track and record performance metrics
BENCHMARK_SYMBOL = "SPY"     # Benchmark symbol for performance comparison

# Backtesting parameters
BACKTEST_START_DATE = "2023-01-01"  # Default start date for backtesting
BACKTEST_END_DATE = "2023-12-31"    # Default end date for backtesting
COMMISSION_PER_SHARE = 0.005        # Commission per share for backtesting (in USD)
MINIMUM_COMMISSION = 1.0            # Minimum commission per trade for backtesting (in USD)

class Settings:
    """
    Settings manager for the trading bot.
    Loads settings from the settings module and optional config files.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize settings manager.
        
        Args:
            config_file: Optional path to a JSON config file to override defaults
        """
        # Start with default settings from this module
        self._settings = {key: value for key, value in globals().items() 
                         if key.isupper() and not key.startswith('_')}
        
        # Load settings from environment variables
        self._load_from_env()
        
        # Load settings from config file if provided
        if config_file:
            self._load_from_file(config_file)
    
    def _load_from_env(self) -> None:
        """Load settings from environment variables."""
        for key in self._settings:
            env_var = f"IBKR_BOT_{key}"
            if env_var in os.environ:
                # Convert environment variable to the right type
                value = os.environ[env_var]
                orig_type = type(self._settings[key])
                
                if orig_type == bool:
                    self._settings[key] = value.lower() in ('true', 'yes', '1', 'y')
                elif orig_type == int:
                    self._settings[key] = int(value)
                elif orig_type == float:
                    self._settings[key] = float(value)
                elif orig_type == list:
                    self._settings[key] = json.loads(value)
                else:
                    self._settings[key] = value
    
    def _load_from_file(self, config_file: str) -> None:
        """
        Load settings from a JSON config file.
        
        Args:
            config_file: Path to the JSON config file
        """
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            # Update settings with values from the config file
            for key, value in config.items():
                if key in self._settings:
                    self._settings[key] = value
                else:
                    # Add new settings not in the defaults
                    self._settings[key] = value
        except Exception as e:
            print(f"Error loading config file: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a setting value.
        
        Args:
            key: The setting name
            default: Default value if the setting doesn't exist
            
        Returns:
            The setting value
        """
        return self._settings.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a setting value.
        
        Args:
            key: The setting name
            value: The new value
        """
        self._settings[key] = value
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get all settings.
        
        Returns:
            Dict[str, Any]: All settings
        """
        return self._settings.copy()
    
    def save_to_file(self, file_path: str) -> None:
        """
        Save current settings to a JSON file.
        
        Args:
            file_path: Path to save the settings
        """
        try:
            # Filter out non-serializable values
            serializable_settings = {}
            for key, value in self._settings.items():
                # Skip non-serializable types
                if isinstance(value, (str, int, float, bool, list, dict, tuple, type(None))):
                    serializable_settings[key] = value
            
            with open(file_path, 'w') as f:
                json.dump(serializable_settings, f, indent=4)
                
            print(f"Settings saved to {file_path}")
        except Exception as e:
            print(f"Error saving settings to file: {e}")


# Create a default settings instance
default_settings = Settings()

# Example of how to use the settings in other modules:
# from config.settings import default_settings
# log_level = default_settings.get('LOG_LEVEL')