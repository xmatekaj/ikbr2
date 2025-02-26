"""Web dashboard for the IKBR trading bot performance monitoring."""
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from datetime import datetime, timedelta
import threading
import time

# Import from project
from src.monitoring.data_collector import DataCollector
from src.monitoring.performance_tracker import PerformanceTracker


class Dashboard:
    """Interactive web dashboard for real-time performance monitoring."""
    
    def __init__(self, data_collector, performance_tracker, update_interval=5):
        """
        Initialize the dashboard.
        
        Args:
            data_collector: DataCollector instance
            performance_tracker: PerformanceTracker instance
            update_interval: Data refresh interval in seconds
        """
        self.data_collector = data_collector
        self.performance_tracker = performance_tracker
        self.update_interval = update_interval
        
        # Initialize Dash app
        self.app = dash.Dash(__name__, title="IKBR Trading Bot Dashboard")
        self.setup_layout()
        self.setup_callbacks()
        
        # Data cache
        self.data_cache = {}
        self.last_update = datetime.now()
        
        # Update thread
        self._stop_event = threading.Event()
        self._update_thread = threading.Thread(
            target=self._background_update,
            daemon=True
        )
    
    def setup_layout(self):
        """Set up the dashboard layout."""
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("IKBR Trading Bot Dashboard"),
                html.Div(id="last-update-time"),
                dcc.Interval(id="interval-component", interval=self.update_interval*1000, n_intervals=0),
            ], className="header"),
            
            # Main content
            html.Div([
                # Performance overview
                html.Div([
                    html.H2("Performance Overview"),
                    html.Div([
                        html.Div([
                            html.H3("Account Equity"),
                            html.H4(id="current-equity", children="$0.00"),
                            html.Div(id="equity-change", children="0.00%")
                        ], className="metric-card"),
                        html.Div([
                            html.H3("Win Rate"),
                            html.H4(id="win-rate", children="0.00%")
                        ], className="metric-card"),
                        html.Div([
                            html.H3("Drawdown"),
                            html.H4(id="max-drawdown", children="0.00%")
                        ], className="metric-card"),
                        html.Div([
                            html.H3("Sharpe Ratio"),
                            html.H4(id="sharpe-ratio", children="0.00")
                        ], className="metric-card")
                    ], className="metric-container")
                ], className="card"),
                
                # Equity chart
                html.Div([
                    html.H2("Equity Curve"),
                    dcc.Tabs([
                        dcc.Tab(label="1 Day", children=[
                            dcc.Graph(id="equity-chart-1d")
                        ]),
                        dcc.Tab(label="1 Week", children=[
                            dcc.Graph(id="equity-chart-1w")
                        ]),
                        dcc.Tab(label="1 Month", children=[
                            dcc.Graph(id="equity-chart-1m")
                        ])
                    ])
                ], className="card"),
                
                # Strategy performance
                html.Div([
                    html.H2("Strategy Performance"),
                    dcc.Graph(id="strategy-performance-chart")
                ], className="card"),
                
                # Recent trades
                html.Div([
                    html.H2("Recent Trades"),
                    html.Div(id="recent-trades-table")
                ], className="card"),
                
                # Current positions
                html.Div([
                    html.H2("Current Positions"),
                    html.Div(id="positions-table")
                ], className="card")
            ], className="main-content"),
        ])
    
    def setup_callbacks(self):
        """Set up dashboard callbacks."""
        # Update timestamp
        @self.app.callback(
            Output("last-update-time", "children"),
            Input("interval-component", "n_intervals")
        )
        def update_time(_):
            return f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Update overview metrics
        @self.app.callback(
            [
                Output("current-equity", "children"),
                Output("equity-change", "children"),
                Output("win-rate", "children"),
                Output("max-drawdown", "children"),
                Output("sharpe-ratio", "children"),
            ],
            Input("interval-component", "n_intervals")
        )
        def update_metrics(_):
            metrics = self.performance_tracker.get_current_metrics()
            
            equity = f"${metrics.get('current_equity', 0):,.2f}"
            equity_change = f"{metrics.get('equity_change_1d', 0) * 100:.2f}%"
            win_rate = f"{metrics.get('win_rate', 0) * 100:.2f}%"
            drawdown = f"{metrics.get('max_drawdown', 0) * 100:.2f}%"
            sharpe = f"{metrics.get('sharpe_ratio', 0):.2f}"
            
            # Set color classes based on values
            equity_change_class = "positive" if metrics.get('equity_change_1d', 0) >= 0 else "negative"
            
            return equity, equity_change, win_rate, drawdown, sharpe
        
        # Update equity chart
        @self.app.callback(
            Output("equity-chart-1d", "figure"),
            Input("interval-component", "n_intervals")
        )
        def update_equity_chart_1d(_):
            equity_data = self.data_collector.get_historical_data('equity', '1d')
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=equity_data['timestamp'], 
                y=equity_data['value'],
                mode='lines',
                name='Equity',
                line=dict(color='#00b33c', width=2)
            ))
            
            fig.update_layout(
                title='Account Equity (24 Hours)',
                xaxis_title='Time',
                yaxis_title='Equity ($)',
                template='plotly_white',
                height=400,
                margin=dict(l=10, r=10, t=40, b=10)
            )
            
            return fig
    
    def start(self, host='0.0.0.0', port=8050, debug=False):
        """Start the dashboard server and update thread."""
        if not self._update_thread.is_alive():
            self._stop_event.clear()
            self._update_thread.start()
        
        self.app.run_server(host=host, port=port, debug=debug)
    
    def stop(self):
        """Stop the dashboard."""
        self._stop_event.set()
        self._update_thread.join(timeout=5.0)
    
    def _background_update(self):
        """Background thread for updating data cache."""
        while not self._stop_event.is_set():
            try:
                # Collect fresh data if needed
                current_time = datetime.now()
                if (current_time - self.last_update).total_seconds() >= self.update_interval:
                    # Update cache
                    self.data_cache = {
                        'latest_data': self.data_collector.get_latest_data(),
                        'metrics': self.performance_tracker.get_current_metrics(),
                        'timestamp': current_time
                    }
                    self.last_update = current_time
                
                # Sleep until next update
                time.sleep(min(1.0, self.update_interval / 2))
            except Exception as e:
                print(f"Error in dashboard background update: {e}")
                time.sleep(5.0)  # Longer sleep on error