# tests/integration/test_backtesting.py
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, timedelta
from src.config.settings import BacktestConfig
from src.backtesting.optimizers import ParameterOptimizer  # You'll need to implement this
from tests.utils.mock_strategy import MockStrategy

class TestBacktesting(unittest.TestCase):
    
    def setUp(self):
        # Create a test config
        self.config = BacktestConfig(
            start_date="2023-01-01",
            end_date="2023-01-31",
            data_source="csv",
            data_path="tests/data/",
            simulate_slippage=True,
            simulate_commission=True,
            initial_capital=100000.0
        )
        
        # Create test data
        self.test_data = {}
        symbols = ['AAPL', 'MSFT']
        
        for symbol in symbols:
            # Create price data
            dates = pd.date_range(start="2023-01-01", end="2023-01-31")
            data = pd.DataFrame({
                'open': [100 + i * 0.1 for i in range(len(dates))],
                'high': [101 + i * 0.1 for i in range(len(dates))],
                'low': [99 + i * 0.1 for i in range(len(dates))],
                'close': [100.5 + i * 0.1 for i in range(len(dates))],
                'volume': [1000000 for _ in range(len(dates))]
            }, index=dates)
            
            self.test_data[symbol] = data
    
    @patch('src.backtesting.engine.DataProvider')  # You'll need to implement this
    def test_backtest_strategy(self, mock_data_provider):
        """Test running a backtest on a strategy"""
        from src.backtesting.engine import BacktestEngine  # You'll need to implement this
        
        # Mock data provider
        data_provider = mock_data_provider.return_value
        data_provider.get_data.side_effect = lambda symbol, *args, **kwargs: self.test_data.get(symbol, pd.DataFrame())
        
        # Create backtest engine
        backtest_engine = BacktestEngine(self.config)
        backtest_engine.data_provider = data_provider
        
        # Create a strategy
        strategy = MockStrategy(
            data_feed=None,  # Will be provided by backtest engine
            order_manager=None,  # Will be provided by backtest engine
            config={
                'symbols': ['AAPL', 'MSFT'],
                'timeframes': ['1 day']
            }
        )
        
        # Run backtest
        results = backtest_engine.run(strategy)
        
        # Check results
        self.assertIsNotNone(results)
        self.assertIn('equity_curve', results)
        self.assertIn('trades', results)
        self.assertIn('metrics', results)
        
        # Check metrics
        metrics = results['metrics']
        self.assertIn('total_return', metrics)
        self.assertIn('sharpe_ratio', metrics)
        self.assertIn('max_drawdown', metrics)
        
        # Check trades
        trades = results['trades']
        self.assertGreater(len(trades), 0)
    
    @patch('src.backtesting.optimizers.BacktestEngine')
    def test_parameter_optimization(self, mock_backtest_engine):
        """Test parameter optimization"""
        # Mock backtest engine
        backtest_engine = mock_backtest_engine.return_value
        backtest_engine.run.return_value = {
            'metrics': {
                'total_return': 0.05,
                'sharpe_ratio': 1.2,
                'max_drawdown': 0.02
            }
        }
        
        # Create optimizer
        optimizer = ParameterOptimizer(
            strategy_class=MockStrategy,
            parameters={
                'lookback_period': [10, 20, 30],
                'momentum_threshold': [0.01, 0.02, 0.03]
            },
            config=self.config,
            optimization_target='sharpe_ratio'
        )
        
        # Run optimization
        results = optimizer.optimize()
        
        # Check results
        self.assertIsNotNone(results)
        self.assertIn('best_parameters', results)
        self.assertIn('all_results', results)
        
        # Verify backtest was called for each parameter combination
        expected_calls = 9  # 3 lookback * 3 threshold values
        self.assertEqual(backtest_engine.run.call_count, expected_calls)

if __name__ == '__main__':
    unittest.main()