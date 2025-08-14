"""
Centralized Configuration Manager for IKBR Trading Bot.

This module provides a single point of configuration management,
loading all settings from a single JSON file.
"""
import json
import os
import logging
from typing import Dict, Any, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Centralized configuration manager that loads and manages all bot settings
    from a single configuration file.
    """
    
    _instance = None
    _config = None
    
    def __new__(cls, config_path: Optional[str] = None):
        """Singleton pattern to ensure only one config manager exists."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the configuration manager."""
        if self._initialized:
            return
            
        if config_path is None:
            # Look for config.json in project root
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config.json"
        
        self.config_path = Path(config_path)
        self._config = self._load_config()
        self._initialized = True
        
        # Create necessary directories
        self._create_directories()
        
        logger.info(f"Configuration loaded from {self.config_path}")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    self._validate_config(config)
                    return config
            else:
                logger.warning(f"Config file not found at {self.config_path}, using defaults")
                return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            logger.info("Using default configuration")
            return self._get_default_config()
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate the configuration structure."""
        required_sections = ['ibkr', 'trading', 'logging', 'paths']
        
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate IBKR configuration
        ibkr_config = config['ibkr']
        if 'host' not in ibkr_config or 'ports' not in ibkr_config:
            raise ValueError("Invalid IBKR configuration")
        
        # Validate database connection string if database is enabled
        if config.get('database', {}).get('enabled', False):
            db_config = config['database']
            if 'connection_string' not in db_config:
                raise ValueError("Database enabled but no connection string provided")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration if file doesn't exist."""
        return {
            "ibkr": {
                "host": "127.0.0.1",
                "ports": {
                    "gateway_paper": 4002,
                    "gateway_live": 4001,
                    "tws_paper": 7497,
                    "tws_live": 7496
                },
                "default_mode": "gateway_paper",
                "client_ids": {
                    "main": 1,
                    "data_feed": 2,
                    "order_manager": 3,
                    "harvester": 10
                }
            },
            "trading": {
                "paper_trading": True,
                "initial_capital": 100000.0,
                "max_positions": 10
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "paths": {
                    "system_logs": "logs/system/",
                    "trade_logs": "logs/trades/"
                }
            },
            "paths": {
                "data": "data/",
                "historical_data": "historical_data/"
            }
        }
    
    def _create_directories(self) -> None:
        """Create necessary directories from configuration."""
        try:
            # Create logging directories
            log_paths = self._config.get('logging', {}).get('paths', {})
            for path in log_paths.values():
                os.makedirs(path, exist_ok=True)
            
            # Create general paths
            paths = self._config.get('paths', {})
            for path in paths.values():
                os.makedirs(path, exist_ok=True)
                
        except Exception as e:
            logger.error(f"Error creating directories: {e}")
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to the configuration value (e.g., 'ibkr.host')
            default: Default value if key is not found
            
        Returns:
            The configuration value or default
            
        Example:
            config.get('ibkr.host')  # Returns '127.0.0.1'
            config.get('ibkr.ports.gateway_paper')  # Returns 4002
        """
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get an entire configuration section.
        
        Args:
            section: The section name (e.g., 'ibkr', 'trading')
            
        Returns:
            Dictionary containing the section configuration
        """
        return self._config.get(section, {})
    
    def get_ibkr_connection_info(self, mode: Optional[str] = None) -> Dict[str, Any]:
        """
        Get IBKR connection information for the specified mode.
        
        Args:
            mode: Connection mode ('gateway_paper', 'gateway_live', 'tws_paper', 'tws_live')
                 If None, uses default_mode from config
                 
        Returns:
            Dictionary with host, port, and client_id information
        """
        ibkr_config = self.get_section('ibkr')
        
        if mode is None:
            mode = ibkr_config.get('default_mode', 'gateway_paper')
        
        host = ibkr_config.get('host', '127.0.0.1')
        port = ibkr_config.get('ports', {}).get(mode, 4002)
        client_id = ibkr_config.get('client_ids', {}).get('main', 1)
        
        return {
            'host': host,
            'port': port,
            'client_id': client_id,
            'mode': mode
        }
    
    def get_database_connection(self) -> Optional[str]:
        """
        Get database connection string if database is enabled.
        
        Returns:
            Database connection string or None if disabled
        """
        db_config = self.get_section('database')
        if db_config.get('enabled', False):
            return db_config.get('connection_string')
        return None
    
    def is_paper_trading(self) -> bool:
        """Check if paper trading mode is enabled."""
        return self.get('trading.paper_trading', True)
    
    def get_log_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.get_section('logging')
    
    def get_strategy_config(self, strategy_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific strategy.
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            Strategy configuration dictionary
        """
        strategies_config = self.get_section('strategies')
        strategy_config = strategies_config.get(strategy_name, {})
        
        # Merge with default strategy config
        default_config = strategies_config.get('default_config', {})
        merged_config = {**default_config, **strategy_config}
        
        return merged_config
    
    def save_config(self, config_path: Optional[str] = None) -> bool:
        """
        Save current configuration to file.
        
        Args:
            config_path: Path to save configuration (uses current path if None)
            
        Returns:
            True if saved successfully
        """
        try:
            save_path = Path(config_path) if config_path else self.config_path
            
            with open(save_path, 'w') as f:
                json.dump(self._config, f, indent=2)
            
            logger.info(f"Configuration saved to {save_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False
    
    def update_config(self, key_path: str, value: Any) -> None:
        """
        Update a configuration value.
        
        Args:
            key_path: Dot-separated path to the configuration value
            value: New value to set
        """
        keys = key_path.split('.')
        config = self._config
        
        # Navigate to the parent of the target key
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        # Set the value
        config[keys[-1]] = value
        
        logger.info(f"Configuration updated: {key_path} = {value}")
    
    def reload_config(self) -> bool:
        """
        Reload configuration from file.
        
        Returns:
            True if reloaded successfully
        """
        try:
            self._config = self._load_config()
            self._create_directories()
            logger.info("Configuration reloaded successfully")
            return True
        except Exception as e:
            logger.error(f"Error reloading configuration: {e}")
            return False

# Global configuration instance
config = ConfigManager()

# Convenience functions for backward compatibility
def get_config() -> ConfigManager:
    """Get the global configuration instance."""
    return config

def get_ibkr_config(mode: Optional[str] = None) -> Dict[str, Any]:
    """Get IBKR connection configuration."""
    return config.get_ibkr_connection_info(mode)

def get_trading_config() -> Dict[str, Any]:
    """Get trading configuration."""
    return config.get_section('trading')

def is_paper_trading() -> bool:
    """Check if paper trading is enabled."""
    return config.is_paper_trading()