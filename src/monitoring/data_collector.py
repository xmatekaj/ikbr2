"""Collects data from various components for monitoring and visualization."""
import threading
from datetime import datetime
import pandas as pd

from src.connectors.ibkr.client import IBKRClient
from src.core.bot_manager import BotManager


class DataCollector:
    """Centralized data collection for monitoring and visualization."""
    
    def __init__(self, bot_manager, ibkr_client, collection_interval=5.0):
        """
        Initialize the data collector.
        
        Args:
            bot_manager: Reference to the BotManager instance
            ibkr_client: Reference to the IBKR client
            collection_interval: Data collection interval in seconds
        """
        self.bot_manager = bot_manager
        self.ibkr_client = ibkr_client
        self.collection_interval = collection_interval
        
        # Storage for collected data
        self._data_cache = {}
        self._historical_data = {}
        
        # Thread control
        self._stop_event = threading.Event()
        self._collection_thread = threading.Thread(
            target=self._collection_loop,
            daemon=True
        )
        self._lock = threading.Lock()
    
    def start(self):
        """Start the data collection thread."""
        if not self._collection_thread.is_alive():
            self._stop_event.clear()
            self._collection_thread.start()
    
    def stop(self):
        """Stop the data collection thread."""
        self._stop_event.set()
        self._collection_thread.join(timeout=5.0)
    
    def get_latest_data(self):
        """Get the latest collected data snapshot."""
        with self._lock:
            return self._data_cache.copy()
    
    def get_historical_data(self, data_type, timeframe='1d'):
        """Get historical data of a specific type."""
        with self._lock:
            if data_type not in self._historical_data:
                return pd.DataFrame()
            
            return self._historical_data[data_type].get(timeframe, pd.DataFrame())
    
    def _collection_loop(self):
        """Main data collection loop."""
        while not self._stop_event.is_set():
            try:
                self._collect_data()
                self._update_historical_data()
                self._sleep_with_jitter(self.collection_interval)
            except Exception as e:
                print(f"Error in data collection: {e}")
    
    def _collect_data(self):
        """Collect data from various system components."""
        data = {
            'timestamp': datetime.now(),
            'system': self._collect_system_metrics(),
            'accounts': self._collect_account_data(),
            'positions': self._collect_position_data(),
            'strategies': self._collect_strategy_data(),
            'orders': self._collect_order_data(),
        }
        
        with self._lock:
            self._data_cache = data
    
    def _update_historical_data(self):
        """Update historical data storage with latest data."""
        with self._lock:
            current_data = self._data_cache
            
            # Example: Store equity history
            if 'accounts' in current_data:
                equity = current_data['accounts'].get('equity', 0)
                self._append_historical_data('equity', equity)
            
            # Example: Store strategy performance
            if 'strategies' in current_data:
                for strategy_id, strategy_data in current_data['strategies'].items():
                    if 'pnl' in strategy_data:
                        self._append_historical_data(
                            f'strategy_{strategy_id}_pnl', 
                            strategy_data['pnl']
                        )
    
    def _append_historical_data(self, data_type, value):
        """Append a value to the historical data store."""
        if data_type not in self._historical_data:
            self._historical_data[data_type] = {
                '1h': pd.DataFrame(columns=['timestamp', 'value']),
                '1d': pd.DataFrame(columns=['timestamp', 'value']),
                '1w': pd.DataFrame(columns=['timestamp', 'value']),
            }
        
        # Add new data point
        new_point = pd.DataFrame({
            'timestamp': [datetime.now()],
            'value': [value]
        })
        
        # Update each timeframe
        for timeframe in self._historical_data[data_type]:
            self._historical_data[data_type][timeframe] = pd.concat(
                [self._historical_data[data_type][timeframe], new_point]
            )
            
            # Apply retention policy based on timeframe
            max_points = {
                '1h': 60,   # 1 point per minute for 1 hour
                '1d': 288,  # 5 minute intervals for 1 day
                '1w': 168   # 1 hour intervals for 1 week
            }
            
            df = self._historical_data[data_type][timeframe]
            if len(df) > max_points.get(timeframe, 100):
                self._historical_data[data_type][timeframe] = df.iloc[-max_points.get(timeframe, 100):]
    
    def _collect_system_metrics(self):
        """Collect system-level metrics."""
        return {
            'cpu_usage': self._get_cpu_usage(),
            'memory_usage': self._get_memory_usage(),
            'thread_count': threading.active_count(),
            'uptime': self._get_uptime()
        }
    
    def _collect_account_data(self):
        """Collect account information from IBKR."""
        # This would be implemented using your existing IBKR client
        return {
            'equity': self.ibkr_client.get_account_value(),
            'buying_power': self.ibkr_client.get_buying_power(),
            'cash_balance': self.ibkr_client.get_cash_balance(),
            'margin_used': self.ibkr_client.get_margin_used(),
        }
    
    def _collect_position_data(self):
        """Collect current positions."""
        return self.ibkr_client.get_positions()
    
    def _collect_strategy_data(self):
        """Collect data from active strategies."""
        strategies = {}
        for strategy_id, strategy in self.bot_manager.get_strategies().items():
            strategies[strategy_id] = {
                'name': strategy.name,
                'status': strategy.status,
                'pnl': strategy.get_pnl(),
                'trade_count': strategy.get_trade_count(),
                'last_signal': strategy.get_last_signal(),
            }
        return strategies
    
    def _collect_order_data(self):
        """Collect current order information."""
        return self.ibkr_client.get_open_orders()
    
    def _get_cpu_usage(self):
        """Get current CPU usage."""
        # Implementation depends on your system
        return 0.0  # Placeholder
    
    def _get_memory_usage(self):
        """Get current memory usage."""
        # Implementation depends on your system
        return 0.0  # Placeholder
    
    def _get_uptime(self):
        """Get system uptime in seconds."""
        # Implementation depends on your system
        return 0.0  # Placeholder
    
    def _sleep_with_jitter(self, base_interval):
        """Sleep with a small jitter to avoid synchronization issues."""
        import random
        import time
        
        jitter = random.uniform(-0.1, 0.1) * base_interval
        time.sleep(max(0.1, base_interval + jitter))