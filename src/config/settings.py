"""
Global configuration settings for the IKBR Trader Bot.

This module provides configuration classes and default settings for the trading system.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
import os
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class TradingConfig:
    """Configuration settings for trading."""
    
    # IBKR connection parameters
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497  # 7497 for TWS paper trading, 7496 for TWS live, 4002 for Gateway paper, 4001 for Gateway live
    ibkr_client_id: int = 1
    
    # Trading parameters
    paper_trading: bool = True
    max_positions: int = 10
    max_risk_per_trade: float = 0.02  # Maximum risk per trade as a fraction of account value
    initial_capital: float = 100000.0
    
    # System parameters
    engine_loop_interval: float = 1.0  # Seconds between engine loop iterations
    
    # Additional parameters with default values
    commission_per_share: float = 0.005  # IBKR commission per share (simplified)
    minimum_commission: float = 1.0      # Minimum commission per trade
    slippage_model: str = "fixed"        # "fixed", "percentage", or "custom"
    slippage_value: float = 0.01         # Fixed slippage in dollars or percentage
    market_data_type: str = "real-time"  # "real-time" or "delayed"
    
    # Advanced parameters
    reconnect_attempts: int = 3
    reconnect_wait_time: int = 5  # Seconds to wait between reconnect attempts
    order_timeout: int = 30       # Seconds to wait for order acknowledgment
    
    # Strategy-specific parameters
    strategy_params: Dict[str, Dict] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Validate trading values
        if self.max_risk_per_trade <= 0 or self.max_risk_per_trade > 1:
            logger.warning(f"Invalid max_risk_per_trade: {self.max_risk_per_trade}. Setting to default 0.02")
            self.max_risk_per_trade = 0.02
        
        if self.initial_capital <= 0:
            logger.warning(f"Invalid initial_capital: {self.initial_capital}. Setting to default 100000.0")
            self.initial_capital = 100000.0
        
        if self.max_positions <= 0:
            logger.warning(f"Invalid max_positions: {self.max_positions}. Setting to default 10")
            self.max_positions = 10
        
        # Validate system parameters
        if self.engine_loop_interval <= 0:
            logger.warning(f"Invalid engine_loop_interval: {self.engine_loop_interval}. Setting to default 1.0")
            self.engine_loop_interval = 1.0
    
    @classmethod
    def from_dict(cls, config_dict: Dict) -> 'TradingConfig':
        """
        Create a TradingConfig from a dictionary.
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            A TradingConfig instance
        """
        # Filter out keys that are not fields in TradingConfig
        valid_keys = {field.name for field in cls.__dataclass_fields__.values()}
        filtered_dict = {k: v for k, v in config_dict.items() if k in valid_keys}
        
        return cls(**filtered_dict)
    
    @classmethod
    def from_json(cls, json_file: str) -> 'TradingConfig':
        """
        Load configuration from a JSON file.
        
        Args:
            json_file: Path to the JSON configuration file
            
        Returns:
            A TradingConfig instance
        """
        try:
            with open(json_file, 'r') as f:
                config_dict = json.load(f)
            
            return cls.from_dict(config_dict)
        except Exception as e:
            logger.error(f"Failed to load configuration from {json_file}: {e}")
            return cls()  # Return default configuration
    
    def to_dict(self) -> Dict:
        """
        Convert configuration to a dictionary.
        
        Returns:
            A dictionary representation of the configuration
        """
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }
    
    def to_json(self, json_file: str) -> bool:
        """
        Save configuration to a JSON file.
        
        Args:
            json_file: Path to save the JSON configuration file
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(json_file), exist_ok=True)
            
            with open(json_file, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save configuration to {json_file}: {e}")
            return False


@dataclass
class BacktestConfig(TradingConfig):
    """Configuration settings for backtesting."""
    
    # Backtest-specific parameters
    start_date: str = "2023-01-01"
    end_date: str = "2023-12-31"
    data_source: str = "csv"  # "csv", "database", "ibkr"
    data_path: str = "historical_data/"
    
    # Simulation parameters
    simulate_slippage: bool = True
    simulate_commission: bool = True
    simulate_latency: bool = False
    latency_ms: int = 100  # Simulated latency in milliseconds
    
    # Results parameters
    save_results: bool = True
    results_path: str = "backtest_results/"
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        super().__post_init__()
        
        # Always set paper_trading to True for backtesting
        self.paper_trading = True


def load_strategy_config(strategy_name: str, config_dir: str = 'src/config/strategy_configs') -> Dict:
    """
    Load configuration for a specific strategy.
    
    Args:
        strategy_name: Name of the strategy
        config_dir: Directory containing strategy configuration files
        
    Returns:
        A dictionary containing strategy configuration
    """
    config_file = os.path.join(config_dir, f"{strategy_name}.json")
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
        else:
            logger.warning(f"Strategy configuration file not found: {config_file}")
            return {}
    except Exception as e:
        logger.error(f"Failed to load strategy configuration: {e}")
        return {}


def save_strategy_config(strategy_name: str, config: Dict, config_dir: str = 'src/config/strategy_configs') -> bool:
    """
    Save configuration for a specific strategy.
    
    Args:
        strategy_name: Name of the strategy
        config: Strategy configuration dictionary
        config_dir: Directory to save strategy configuration files
        
    Returns:
        True if saved successfully, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(config_dir, exist_ok=True)
        
        config_file = os.path.join(config_dir, f"{strategy_name}.json")
        
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Saved strategy configuration to {config_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to save strategy configuration: {e}")
        return False

# Create a default configuration
default_config = TradingConfig()

# Create default_settings dictionary that includes all needed configurations
default_settings = {
    # IBKR connection settings
    'IBKR_HOST': default_config.ibkr_host,
    'IBKR_PORT': default_config.ibkr_port,
    'IBKR_CLIENT_ID': default_config.ibkr_client_id,
    
    # Trading settings
    'PAPER_TRADING': default_config.paper_trading,
    'MAX_POSITIONS': default_config.max_positions,
    'MAX_RISK_PER_TRADE': default_config.max_risk_per_trade,
    'INITIAL_CAPITAL': default_config.initial_capital,
    
    # Logging settings
    'LOG_LEVEL': 'INFO',
    'LOG_FORMAT': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'SYSTEM_LOG_FILE': 'logs/system/system.log',
    'TRADE_LOG_FILE': 'logs/trades/trades.log',
    
    # Dashboard settings
    'ENABLE_DASHBOARD': True,
    'DASHBOARD_HOST': '0.0.0.0',
    'DASHBOARD_PORT': 8050
}

# Expose the settings as a dictionary for backward compatibility
settings = default_settings

# You can also create a function to get the full config object
def get_config():
    """Returns the full TradingConfig object"""
    return default_config