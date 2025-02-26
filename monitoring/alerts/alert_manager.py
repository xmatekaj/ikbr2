# src/monitoring/alerts/alert_manager.py
"""Alert system for monitoring trading performance and system events."""
import threading
import time
from datetime import datetime
from collections import deque

from src.utils.logging import system_logger
from src.monitoring.alerts.notifier import Notifier


class AlertCondition:
    """Defines an alert condition with a check function and parameters."""
    
    def __init__(self, name, check_func, params, severity="info", cooldown_seconds=300):
        """
        Initialize an alert condition.
        
        Args:
            name: Name of the alert
            check_func: Function that returns True when alert should trigger
            params: Parameters for the check function
            severity: Alert severity (info, warning, error, critical)
            cooldown_seconds: Minimum seconds between repeated alerts
        """
        self.name = name
        self.check_func = check_func
        self.params = params
        self.severity = severity
        self.cooldown_seconds = cooldown_seconds
        self.last_triggered = None
    
    def check(self, data):
        """Check if this alert condition is met."""
        # Check cooldown period
        if self.last_triggered and (datetime.now() - self.last_triggered).total_seconds() < self.cooldown_seconds:
            return False
        
        # Check condition
        if self.check_func(data, **self.params):
            self.last_triggered = datetime.now()
            return True
        
        return False


class AlertManager:
    """Manages alert conditions and triggers notifications when conditions are met."""
    
    def __init__(self, data_collector, performance_tracker, check_interval=60):
        """
        Initialize the alert manager.
        
        Args:
            data_collector: Data collector instance
            performance_tracker: Performance tracker instance
            check_interval: How often to check alert conditions (seconds)
        """
        self.data_collector = data_collector
        self.performance_tracker = performance_tracker
        self.check_interval = check_interval
        
        # Alert storage
        self.conditions = []
        self.alert_history = deque(maxlen=100)
        
        # Initialize notifier
        self.notifier = Notifier()
        
        # Threading
        self._stop_event = threading.Event()
        self._alert_thread = threading.Thread(
            target=self._alert_loop,
            daemon=True
        )
    
    def start(self):
        """Start the alert manager."""
        # Register default alerts
        self._register_default_alerts()
        
        # Start alert thread
        if not self._alert_thread.is_alive():
            self._stop_event.clear()
            self._alert_thread.start()
            system_logger.info("Alert manager started")
    
    def stop(self):
        """Stop the alert manager."""
        self._stop_event.set()
        self._alert_thread.join(timeout=5.0)
        system_logger.info("Alert manager stopped")
    
    def add_condition(self, condition):
        """Add a new alert condition."""
        self.conditions.append(condition)
        system_logger.info(f"Added alert condition: {condition.name}")
    
    def get_alert_history(self):
        """Get the alert history."""
        return list(self.alert_history)
    
    def _register_default_alerts(self):
        """Register default alert conditions."""
        # Drawdown alert
        self.add_condition(AlertCondition(
            name="High Drawdown",
            check_func=self._check_drawdown,
            params={"threshold": 0.1},  # 10% drawdown
            severity="warning",
            cooldown_seconds=3600  # Once per hour
        ))
        
        # Equity drop alert
        self.add_condition(AlertCondition(
            name="Rapid Equity Decline",
            check_func=self._check_equity_drop,
            params={"threshold": 0.05, "timeframe": "1h"},  # 5% drop in 1 hour
            severity="warning",
            cooldown_seconds=1800  # Once per 30 minutes
        ))
        
        # Connection status alert
        self.add_condition(AlertCondition(
            name="IBKR Connection Lost",
            check_func=self._check_connection,
            params={},
            severity="critical",
            cooldown_seconds=300  # Once per 5 minutes
        ))
    
    def _alert_loop(self):
        """Main alert checking loop."""
        while not self._stop_event.is_set():
            try:
                self._check_alerts()
                time.sleep(self.check_interval)
            except Exception as e:
                system_logger.error(f"Error in alert manager: {e}")
                time.sleep(60)  # Longer sleep on error
    
    def _check_alerts(self):
        """Check all alert conditions."""
        # Get current data
        latest_data = self.data_collector.get_latest_data()
        metrics = self.performance_tracker.get_current_metrics()
        
        # Combine data for alert checks
        check_data = {
            'latest_data': latest_data,
            'metrics': metrics,
            'timestamp': datetime.now()
        }
        
        # Check each condition
        for condition in self.conditions:
            if condition.check(check_data):
                self._trigger_alert(condition, check_data)
    
    def _trigger_alert(self, condition, data):
        """Trigger an alert and send notifications."""
        alert_data = {
            'name': condition.name,
            'severity': condition.severity,
            'timestamp': datetime.now(),
            'data': data
        }
        
        # Add to history
        self.alert_history.append(alert_data)
        
        # Log the alert
        log_message = f"ALERT: {condition.name} ({condition.severity})"
        if condition.severity == "critical":
            system_logger.critical(log_message)
        elif condition.severity == "error":
            system_logger.error(log_message)
        elif condition.severity == "warning":
            system_logger.warning(log_message)
        else:
            system_logger.info(log_message)
        
        # Send notification
        self.notifier.send_alert(
            title=condition.name,
            message=self._format_alert_message(condition, data),
            severity=condition.severity
        )
    
    def _format_alert_message(self, condition, data):
        """Format an alert message based on the condition type."""
        if condition.name == "High Drawdown":
            drawdown = data['metrics'].get('max_drawdown', 0) * 100
            return f"High drawdown detected: {drawdown:.2f}%"
        
        elif condition.name == "Rapid Equity Decline":
            equity_change = data['metrics'].get('equity_change_1h', 0) * 100
            return f"Rapid equity decline detected: {equity_change:.2f}% in the last hour"
        
        elif condition.name == "IBKR Connection Lost":
            system_data = data['latest_data'].get('system', {})
            last_seen = system_data.get('last_connection_time', 'unknown')
            return f"Connection to IBKR has been lost. Last connection time: {last_seen}"
        
        # Generic message for other alerts
        else:
            return f"Alert condition '{condition.name}' triggered"

    def _check_drawdown(self, data, threshold=0.1):
        """Check if the drawdown exceeds the threshold."""
        drawdown = data['metrics'].get('max_drawdown', 0)
        return drawdown >= threshold

    def _check_equity_drop(self, data, threshold=0.05, timeframe="1h"):
        """Check if there has been a significant equity drop in the given timeframe."""
        if timeframe == "1h":
            equity_change = data['metrics'].get('equity_change_1h', 0)
        elif timeframe == "1d":
            equity_change = data['metrics'].get('equity_change_1d', 0)
        else:
            return False
        
        return equity_change <= -threshold

    def _check_connection(self, data):
        """Check if the IBKR connection is active."""
        latest_data = data.get('latest_data', {})
        system_data = latest_data.get('system', {})
        
        # Connection status would be stored in system data
        connected = system_data.get('ibkr_connected', True)  # Default to True to avoid false alarms
        
        return not connected