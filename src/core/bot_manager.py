"""
Bot Manager for IKBR Trader Bot.

This module provides a high-level manager for trading bots, allowing multiple
trading strategies to run simultaneously with their own configurations.
It coordinates the trading engines, performance tracking, and logging for all bots.
"""
import logging
import os
import json
import datetime
from typing import Dict, List, Optional, Type, Union
import uuid

from ..config.settings import TradingConfig
from ..strategies.base_strategy import BaseStrategy
from .engine import TradingEngine
from .performance import PerformanceTracker
from ..utils.logging import system_logger
from ..utils.logging.trade_logger import TradeLogger

logger = logging.getLogger(__name__)


class BotManager:
    """
    Manages multiple trading bots and strategies.
    
    This class is responsible for creating, configuring, starting, and stopping
    trading bots that run different strategies.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the bot manager.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.engines: Dict[str, TradingEngine] = {}
        self.performance_trackers: Dict[str, PerformanceTracker] = {}
        self.trade_loggers: Dict[str, TradeLogger] = {}
        self.strategies: Dict[str, Dict] = {}  # Maps strategy_id to strategy info
        
        # Load configuration if path is provided
        if config_path and os.path.exists(config_path):
            self._load_config()
        
        logger.info("Bot manager initialized")
    
    def _load_config(self) -> None:
        """Load configuration from file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Process global configuration
            global_config = config.get('global', {})
            
            # Process individual bot configurations
            bots_config = config.get('bots', [])
            for bot_config in bots_config:
                bot_id = bot_config.get('id', str(uuid.uuid4()))
                self._create_bot_from_config(bot_id, bot_config)
            
            logger.info(f"Loaded configuration from {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
    
    def _create_bot_from_config(self, bot_id: str, bot_config: dict) -> None:
        """
        Create a new trading bot from configuration.
        
        Args:
            bot_id: Unique identifier for the bot
            bot_config: Bot configuration dictionary
        """
        try:
            # Create trading config
            trading_config = TradingConfig(
                ibkr_host=bot_config.get('ibkr_host', 'localhost'),
                ibkr_port=bot_config.get('ibkr_port', 7497),
                ibkr_client_id=bot_config.get('ibkr_client_id', 1),
                engine_loop_interval=bot_config.get('engine_loop_interval', 1.0),
                paper_trading=bot_config.get('paper_trading', True),
                max_positions=bot_config.get('max_positions', 10),
                max_risk_per_trade=bot_config.get('max_risk_per_trade', 0.02),
                initial_capital=bot_config.get('initial_capital', 100000.0)
            )
            
            # Create engine and performance tracker
            engine = TradingEngine(trading_config)
            performance_tracker = PerformanceTracker(trading_config.initial_capital)
            
            # Store in dictionaries
            self.engines[bot_id] = engine
            self.performance_trackers[bot_id] = performance_tracker
            
            # Load strategies for this bot
            strategies_config = bot_config.get('strategies', [])
            for strategy_config in strategies_config:
                self._add_strategy_from_config(bot_id, strategy_config)
            
            logger.info(f"Created bot {bot_id} with {len(strategies_config)} strategies")
        except Exception as e:
            logger.error(f"Failed to create bot {bot_id}: {e}")
    
    def _add_strategy_from_config(self, bot_id: str, strategy_config: dict) -> None:
        """
        Add a strategy to a bot from configuration.
        
        Args:
            bot_id: Bot identifier
            strategy_config: Strategy configuration dictionary
        """
        try:
            strategy_id = strategy_config.get('id', str(uuid.uuid4()))
            strategy_type = strategy_config.get('type')
            strategy_params = strategy_config.get('params', {})
            
            # Create a unique strategy_inst_id that combines bot_id and strategy_id
            strategy_inst_id = f"{bot_id}_{strategy_id}"
            
            # Create strategy instance - this is a simplified example
            # In a real implementation, you would dynamically import and instantiate strategies
            # based on the strategy_type
            
            # For now, let's just store the configuration
            self.strategies[strategy_inst_id] = {
                'bot_id': bot_id,
                'strategy_id': strategy_id,
                'type': strategy_type,
                'params': strategy_params,
                'active': False
            }
            
            # Create a trade logger for this strategy
            self.trade_loggers[strategy_inst_id] = TradeLogger(strategy_inst_id)
            
            logger.info(f"Added strategy {strategy_id} to bot {bot_id}")
        except Exception as e:
            logger.error(f"Failed to add strategy to bot {bot_id}: {e}")
    
    def create_bot(self, bot_id: str, config: TradingConfig) -> str:
        """
        Create a new trading bot.
        
        Args:
            bot_id: Unique identifier for the bot
            config: Trading configuration
            
        Returns:
            The bot ID
        """
        # Generate a unique ID if not provided
        if not bot_id:
            bot_id = str(uuid.uuid4())
        
        # Check if bot already exists
        if bot_id in self.engines:
            logger.warning(f"Bot {bot_id} already exists, overwriting")
        
        # Create engine and performance tracker
        engine = TradingEngine(config)
        performance_tracker = PerformanceTracker(config.initial_capital)
        
        # Store in dictionaries
        self.engines[bot_id] = engine
        self.performance_trackers[bot_id] = performance_tracker
        
        logger.info(f"Created bot {bot_id}")
        return bot_id
    
    def add_strategy(self, 
                    bot_id: str, 
                    strategy_id: str,
                    strategy: BaseStrategy) -> str:
        """
        Add a strategy to a bot.
        
        Args:
            bot_id: Bot identifier
            strategy_id: Strategy identifier
            strategy: Strategy instance
            
        Returns:
            The strategy instance ID
        """
        if bot_id not in self.engines:
            raise ValueError(f"Bot {bot_id} does not exist")
        
        # Create a unique strategy instance ID
        strategy_inst_id = f"{bot_id}_{strategy_id}"
        
        # Add strategy to engine
        engine = self.engines[bot_id]
        engine.add_strategy(strategy_id, strategy)
        
        # Store strategy info
        self.strategies[strategy_inst_id] = {
            'bot_id': bot_id,
            'strategy_id': strategy_id,
            'type': strategy.__class__.__name__,
            'params': {},  # Could extract from strategy instance
            'active': False
        }
        
        # Create a trade logger for this strategy
        self.trade_loggers[strategy_inst_id] = TradeLogger(strategy_inst_id)
        
        logger.info(f"Added strategy {strategy_id} to bot {bot_id}")
        return strategy_inst_id
    
    def start_bot(self, bot_id: str) -> bool:
        """
        Start a trading bot.
        
        Args:
            bot_id: Bot identifier
            
        Returns:
            True if the bot was started successfully, False otherwise
        """
        if bot_id not in self.engines:
            logger.error(f"Bot {bot_id} does not exist")
            return False
        
        engine = self.engines[bot_id]
        result = engine.start()
        
        if result:
            # Update strategy statuses
            for strategy_inst_id, strategy_info in self.strategies.items():
                if strategy_info['bot_id'] == bot_id:
                    strategy_info['active'] = True
            
            logger.info(f"Started bot {bot_id}")
        else:
            logger.error(f"Failed to start bot {bot_id}")
        
        return result
    
    def stop_bot(self, bot_id: str) -> bool:
        """
        Stop a trading bot.
        
        Args:
            bot_id: Bot identifier
            
        Returns:
            True if the bot was stopped successfully, False otherwise
        """
        if bot_id not in self.engines:
            logger.error(f"Bot {bot_id} does not exist")
            return False
        
        engine = self.engines[bot_id]
        engine.stop()
        
        # Update strategy statuses
        for strategy_inst_id, strategy_info in self.strategies.items():
            if strategy_info['bot_id'] == bot_id:
                strategy_info['active'] = False
        
        logger.info(f"Stopped bot {bot_id}")
        return True
    
    def stop_all_bots(self) -> None:
        """Stop all running trading bots."""
        for bot_id in self.engines:
            self.stop_bot(bot_id)
        
        logger.info("Stopped all bots")
    
    def get_bot_status(self, bot_id: str) -> Dict:
        """
        Get the status of a bot.
        
        Args:
            bot_id: Bot identifier
            
        Returns:
            A dictionary containing the bot's status information
        """
        if bot_id not in self.engines:
            return {"error": f"Bot {bot_id} does not exist"}
        
        engine = self.engines[bot_id]
        engine_status = engine.get_engine_status()
        
        # Get strategies for this bot
        strategies = {}
        for strategy_inst_id, strategy_info in self.strategies.items():
            if strategy_info['bot_id'] == bot_id:
                strategies[strategy_info['strategy_id']] = {
                    "type": strategy_info['type'],
                    "active": strategy_info['active']
                }
        
        # Get performance metrics if available
        performance = {}
        if bot_id in self.performance_trackers:
            performance = self.performance_trackers[bot_id].get_performance_summary()
        
        return {
            "bot_id": bot_id,
            "engine_status": engine_status,
            "strategies": strategies,
            "performance": performance
        }
    
    def get_all_bots_status(self) -> Dict[str, Dict]:
        """
        Get the status of all bots.
        
        Returns:
            A dictionary mapping bot IDs to their status information
        """
        return {bot_id: self.get_bot_status(bot_id) for bot_id in self.engines}
    
    def get_strategy_status(self, strategy_inst_id: str) -> Dict:
        """
        Get the status of a strategy.
        
        Args:
            strategy_inst_id: Strategy instance identifier
            
        Returns:
            A dictionary containing the strategy's status information
        """
        if strategy_inst_id not in self.strategies:
            return {"error": f"Strategy {strategy_inst_id} does not exist"}
        
        strategy_info = self.strategies[strategy_inst_id]
        bot_id = strategy_info['bot_id']
        strategy_id = strategy_info['strategy_id']
        
        if bot_id not in self.engines:
            return {"error": f"Bot {bot_id} for strategy {strategy_inst_id} does not exist"}
        
        engine = self.engines[bot_id]
        strategy_status = engine.get_strategy_status(strategy_id)
        
        return {
            "strategy_inst_id": strategy_inst_id,
            "bot_id": bot_id,
            "strategy_id": strategy_id,
            "type": strategy_info['type'],
            "active": strategy_info['active'],
            "status": strategy_status
        }
    
    def update_daily_performance(self) -> None:
        """Update daily performance metrics for all bots."""
        current_date = datetime.datetime.now().date()
        
        for bot_id, performance_tracker in self.performance_trackers.items():
            if bot_id in self.engines:
                engine = self.engines[bot_id]
                
                # Only update if engine is running
                if engine.is_running():
                    # Get current equity (in a real implementation, this would come from the broker)
                    # For this example, we'll just use the current_capital
                    equity_value = performance_tracker.current_capital
                    
                    # Update daily equity
                    performance_tracker.update_daily_equity(current_date, equity_value)
        
        logger.info("Updated daily performance metrics")
    
    def generate_performance_reports(self) -> Dict[str, str]:
        """
        Generate performance reports for all bots.
        
        Returns:
            A dictionary mapping bot IDs to their performance reports
        """
        reports = {}
        
        for bot_id, performance_tracker in self.performance_trackers.items():
            reports[bot_id] = performance_tracker.generate_performance_report()
        
        return reports
    
    def save_config(self, config_path: str = None) -> bool:
        """
        Save the current configuration to a file.
        
        Args:
            config_path: Path to save the configuration file
            
        Returns:
            True if the configuration was saved successfully, False otherwise
        """
        if config_path is None:
            config_path = self.config_path
        
        if config_path is None:
            logger.error("No configuration path specified")
            return False
        
        try:
            # Build configuration dictionary
            config = {
                "global": {},
                "bots": []
            }
            
            # Group strategies by bot
            bot_strategies = {}
            for strategy_inst_id, strategy_info in self.strategies.items():
                bot_id = strategy_info['bot_id']
                if bot_id not in bot_strategies:
                    bot_strategies[bot_id] = []
                
                bot_strategies[bot_id].append({
                    "id": strategy_info['strategy_id'],
                    "type": strategy_info['type'],
                    "params": strategy_info['params']
                })
            
            # Build bot configurations
            for bot_id, engine in self.engines.items():
                # Extract config from engine
                config_dict = {
                    "id": bot_id,
                    "ibkr_host": engine.config.ibkr_host,
                    "ibkr_port": engine.config.ibkr_port,
                    "ibkr_client_id": engine.config.ibkr_client_id,
                    "engine_loop_interval": engine.config.engine_loop_interval,
                    "paper_trading": engine.config.paper_trading,
                    "max_positions": engine.config.max_positions,
                    "max_risk_per_trade": engine.config.max_risk_per_trade,
                    "initial_capital": engine.config.initial_capital,
                    "strategies": bot_strategies.get(bot_id, [])
                }
                
                config["bots"].append(config_dict)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            
            # Write to file
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            logger.info(f"Saved configuration to {config_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return False