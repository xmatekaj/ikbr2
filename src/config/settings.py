"""
Settings module for backward compatibility.
This module provides compatibility with the old settings system
while redirecting to the new centralized configuration.
"""
import logging
from .config_manager import get_config

logger = logging.getLogger(__name__)

# Get the configuration manager instance
config_manager = get_config()

# Legacy TradingConfig class for backward compatibility
class TradingConfig:
    """Legacy TradingConfig class that now uses the centralized configuration."""
    
    def __init__(self, **kwargs):
        """Initialize with values from centralized config or provided kwargs."""
        self.config_manager = get_config()
        
        # IBKR connection parameters
        ibkr_config = self.config_manager.get_section('ibkr')
        conn_info = self.config_manager.get_ibkr_connection_info()
        
        self.ibkr_host = kwargs.get('ibkr_host', conn_info['host'])
        self.ibkr_port = kwargs.get('ibkr_port', conn_info['port'])
        self.ibkr_client_id = kwargs.get('ibkr_client_id', conn_info['client_id'])
        
        # Trading parameters
        trading_config = self.config_manager.get_section('trading')
        self.paper_trading = kwargs.get('paper_trading', trading_config.get('paper_trading', True))
        self.max_positions = kwargs.get('max_positions', trading_config.get('max_positions', 10))
        self.max_risk_per_trade = kwargs.get('max_risk_per_trade', trading_config.get('max_risk_per_trade', 0.02))
        self.initial_capital = kwargs.get('initial_capital', trading_config.get('initial_capital', 100000.0))
        
        # System parameters
        self.engine_loop_interval = kwargs.get('engine_loop_interval', 1.0)
        
        # Additional parameters
        self.commission_per_share = kwargs.get('commission_per_share', trading_config.get('commission', {}).get('per_share', 0.005))
        self.minimum_commission = kwargs.get('minimum_commission', trading_config.get('commission', {}).get('minimum', 1.0))
        
    @classmethod
    def from_dict(cls, config_dict):
        """Create a TradingConfig from a dictionary."""
        return cls(**config_dict)
    
    @classmethod
    def from_json(cls, json_file):
        """Load configuration from a JSON file."""
        try:
            import json
            with open(json_file, 'r') as f:
                config_dict = json.load(f)
            return cls.from_dict(config_dict)
        except Exception as e:
            logger.error(f"Failed to load configuration from {json_file}: {e}")
            return cls()
    
    def to_dict(self):
        """Convert configuration to a dictionary."""
        return {
            'ibkr_host': self.ibkr_host,
            'ibkr_port': self.ibkr_port,
            'ibkr_client_id': self.ibkr_client_id,
            'paper_trading': self.paper_trading,
            'max_positions': self.max_positions,
            'max_risk_per_trade': self.max_risk_per_trade,
            'initial_capital': self.initial_capital,
            'engine_loop_interval': self.engine_loop_interval,
            'commission_per_share': self.commission_per_share,
            'minimum_commission': self.minimum_commission
        }

# Legacy BacktestConfig class
class BacktestConfig(TradingConfig):
    """Legacy BacktestConfig class for backward compatibility."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Backtest-specific parameters from centralized config
        backtest_config = self.config_manager.get_section('backtesting')
        
        self.start_date = kwargs.get('start_date', backtest_config.get('default_start_date', '2023-01-01'))
        self.end_date = kwargs.get('end_date', backtest_config.get('default_end_date', '2023-12-31'))
        self.data_source = kwargs.get('data_source', backtest_config.get('data_source', 'database'))
        self.data_path = kwargs.get('data_path', self.config_manager.get('paths.historical_data', 'historical_data/'))
        
        self.simulate_slippage = kwargs.get('simulate_slippage', backtest_config.get('simulate_slippage', True))
        self.simulate_commission = kwargs.get('simulate_commission', backtest_config.get('simulate_commission', True))
        self.simulate_latency = kwargs.get('simulate_latency', False)
        
        # Always paper trading for backtests
        self.paper_trading = True

# Create default_settings dictionary for backward compatibility
def _create_default_settings():
    """Create the default_settings dictionary from the centralized configuration."""
    ibkr_conn = config_manager.get_ibkr_connection_info()
    log_config = config_manager.get_log_config()
    trading_config = config_manager.get_section('trading')
    monitoring_config = config_manager.get_section('monitoring')
    db_config = config_manager.get_section('database')
    
    return {
        # IBKR connection settings
        'IBKR_HOST': ibkr_conn['host'],
        'IBKR_PORT': ibkr_conn['port'],
        'IBKR_CLIENT_ID': ibkr_conn['client_id'],
        
        # Trading settings
        'PAPER_TRADING': trading_config.get('paper_trading', True),
        'MAX_POSITIONS': trading_config.get('max_positions', 10),
        'MAX_RISK_PER_TRADE': trading_config.get('max_risk_per_trade', 0.02),
        'INITIAL_CAPITAL': trading_config.get('initial_capital', 100000.0),
        
        # Logging settings
        'LOG_LEVEL': log_config.get('level', 'INFO'),
        'LOG_FORMAT': log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        'SYSTEM_LOG_FILE': os.path.join(log_config.get('paths', {}).get('system_logs', 'logs/system/'), 'system.log'),
        'TRADE_LOG_FILE': os.path.join(log_config.get('paths', {}).get('trade_logs', 'logs/trades/'), 'trades.log'),
        
        # Dashboard settings
        'ENABLE_DASHBOARD': monitoring_config.get('dashboard', {}).get('enabled', True),
        'DASHBOARD_HOST': monitoring_config.get('dashboard', {}).get('host', '0.0.0.0'),
        'DASHBOARD_PORT': monitoring_config.get('dashboard', {}).get('port', 8050),

        # Database settings
        'DB_HOST': 'localhost',  # Extracted from connection string if needed
        'DB_PORT': 5432,
        'DB_NAME': 'market_data',
        'DB_USER': 'user_bot',
        'DB_PASSWORD': 'user_bot',
        'DB_POOL_SIZE': db_config.get('pool_size', 5),
        'DB_CONNECTION_STRING': config_manager.get_database_connection()
    }

# Import os for path operations
import os

# Create the default_settings and settings dictionaries
default_settings = _create_default_settings()
settings = default_settings

# Create default config instance for backward compatibility
default_config = TradingConfig()

# Legacy functions for backward compatibility
def load_strategy_config(strategy_name, config_dir=None):
    """Load configuration for a specific strategy."""
    return config_manager.get_strategy_config(strategy_name)

def save_strategy_config(strategy_name, config, config_dir=None):
    """Save configuration for a specific strategy."""
    config_manager.update_config(f'strategies.{strategy_name}', config)
    return config_manager.save_config()

def get_config():
    """Returns the configuration manager (updated for new system)."""
    return config_manager