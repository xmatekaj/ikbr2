"""Real-time performance tracking for the IKBR trading bot."""
import threading
import time
from collections import deque
from datetime import datetime

from src.utils.metrics import calculate_sharpe_ratio, calculate_drawdown
from src.utils.logging import system_logger


class PerformanceTracker:
    """Tracks real-time performance metrics for trading strategies."""
    
    def __init__(self, tracking_window=100, update_interval=1.0):
        """
        Initialize the performance tracker.
        
        Args:
            tracking_window: Number of data points to keep in memory
            update_interval: How often to update metrics (in seconds)
        """
        self.tracking_window = tracking_window
        self.update_interval = update_interval
        
        # Performance data storage (thread-safe)
        self._lock = threading.Lock()
        self._equity_history = deque(maxlen=tracking_window)
        self._trade_history = deque(maxlen=tracking_window)
        self._metrics_cache = {}
        
        # Initialize tracking thread
        self._stop_event = threading.Event()
        self._tracking_thread = threading.Thread(
            target=self._metrics_update_loop,
            daemon=True
        )
        
    def start(self):
        """Start the performance tracking thread."""
        if not self._tracking_thread.is_alive():
            self._stop_event.clear()
            self._tracking_thread.start()
            system_logger.info("Performance tracking started")
    
    def stop(self):
        """Stop the performance tracking thread."""
        self._stop_event.set()
        self._tracking_thread.join(timeout=5.0)
        system_logger.info("Performance tracking stopped")
    
    def add_equity_point(self, timestamp, equity_value, portfolio=None):
        """Add an equity point to the tracker."""
        with self._lock:
            self._equity_history.append({
                'timestamp': timestamp or datetime.now(),
                'equity': equity_value,
                'portfolio': portfolio
            })
    
    def add_trade(self, trade_data):
        """Add a completed trade to the history."""
        with self._lock:
            self._trade_history.append({
                'timestamp': trade_data.get('timestamp', datetime.now()),
                'symbol': trade_data.get('symbol'),
                'direction': trade_data.get('direction'),
                'entry_price': trade_data.get('entry_price'),
                'exit_price': trade_data.get('exit_price'),
                'size': trade_data.get('size'),
                'pnl': trade_data.get('pnl'),
                'strategy': trade_data.get('strategy')
            })
    
    def get_current_metrics(self):
        """Get the current performance metrics."""
        with self._lock:
            return self._metrics_cache.copy()
    
    def _metrics_update_loop(self):
        """Background thread that updates performance metrics."""
        while not self._stop_event.is_set():
            try:
                self._update_metrics()
                time.sleep(self.update_interval)
            except Exception as e:
                system_logger.error(f"Error in performance tracker: {e}")
    
    def _update_metrics(self):
        """Calculate and update performance metrics."""
        with self._lock:
            if not self._equity_history:
                return
            
            # Extract equity values
            equity_values = [point['equity'] for point in self._equity_history]
            
            # Calculate metrics
            current_metrics = {
                'current_equity': equity_values[-1],
                'equity_change_1d': self._calculate_change(equity_values, 1440),
                'equity_change_1h': self._calculate_change(equity_values, 60),
                'max_drawdown': calculate_drawdown(equity_values),
                'sharpe_ratio': calculate_sharpe_ratio(equity_values),
                'trade_count': len(self._trade_history),
                'win_rate': self._calculate_win_rate(),
                'avg_trade_pnl': self._calculate_avg_trade_pnl(),
                'timestamp': datetime.now()
            }
            
            # Update cache
            self._metrics_cache = current_metrics
    
    def _calculate_change(self, values, lookback):
        """Calculate change over a specific lookback period."""
        if len(values) <= 1:
            return 0.0
            
        if len(values) >= lookback:
            return (values[-1] - values[-lookback]) / values[-lookback]
        else:
            return (values[-1] - values[0]) / values[0]
    
    def _calculate_win_rate(self):
        """Calculate the win rate from trade history."""
        if not self._trade_history:
            return 0.0
            
        winning_trades = sum(1 for trade in self._trade_history if trade.get('pnl', 0) > 0)
        return winning_trades / len(self._trade_history)
    
    def _calculate_avg_trade_pnl(self):
        """Calculate average PNL per trade."""
        if not self._trade_history:
            return 0.0
            
        total_pnl = sum(trade.get('pnl', 0) for trade in self._trade_history)
        return total_pnl / len(self._trade_history)