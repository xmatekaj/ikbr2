# tests/unit/strategies/conventional/test_momentum.py
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.strategies.conventional.momentum import MomentumStrategy

class TestMomentumStrategy(unittest.TestCase):
    
    def setUp(self):
        # Create mocks for dependencies
        self.data_feed = MagicMock()
        self.order_manager = MagicMock()
        
        # Sample configuration
        self.config = {
            'lookback_period': 10,
            'momentum_threshold': 0.02,
            'universe_size': 3,
            'symbols': ['AAPL', 'MSFT', 'GOOGL'],
            'timeframes': ['1 day'],
            'trading_times': ['10:00', '14:00']
        }
        
        # Create strategy instance
        self.strategy = MomentumStrategy(
            data_feed=self.data_feed,
            order_manager=self.order_manager,
            config=self.config
        )
        
        # Mock market data
        self.strategy.market_data = {
            'AAPL': {'1 day': self._create_mock_bars('AAPL', 15, 150, 165)},
            'MSFT': {'1 day': self._create_mock_bars('MSFT', 15, 250, 230)},
            'GOOGL': {'1 day': self._create_mock_bars('GOOGL', 15, 1800, 2000)}
        }
        
    def _create_mock_bars(self, symbol, num_bars, start_price, end_price):
        """Helper to create mock price bars with a trend"""
        bars = []
        price_step = (end_price - start_price) / (num_bars - 1) if num_bars > 1 else 0
        
        for i in range(num_bars):
            price = start_price + (i * price_step)
            bars.append({
                'date': datetime.now(),
                'open': price - 1,
                'high': price + 2,
                'low': price - 2,
                'close': price,
                'volume': 1000
            })
            
        return bars
        
    def test_initialization(self):
        self.assertEqual(self.strategy.name, "Momentum")
        self.assertEqual(self.strategy.lookback_period, 10)
        self.assertEqual(self.strategy.momentum_threshold, 0.02)
        self.assertEqual(self.strategy.universe_size, 3)
        
    @patch('src.strategies.conventional.momentum.datetime')
    def test_is_trading_time(self, mock_datetime):
        # Mock current time to match a trading time
        mock_datetime.now.return_value.strftime.return_value = '10:00'
        self.assertTrue(self.strategy._is_trading_time('10:00'))
        
        # Mock current time to not match a trading time
        mock_datetime.now.return_value.strftime.return_value = '12:00'
        self.assertFalse(self.strategy._is_trading_time('12:00'))
        
    def test_calculate_momentum(self):
        # Test positive momentum
        momentum_aapl = self.strategy._calculate_momentum('AAPL')
        self.assertGreater(momentum_aapl, 0)
        
        # Test negative momentum
        momentum_msft = self.strategy._calculate_momentum('MSFT')
        self.assertLess(momentum_msft, 0)
        
    @patch('src.strategies.conventional.momentum.datetime')
    def test_generate_signals(self, mock_datetime):
        # Mock current time to match a trading time
        mock_datetime.now.return_value.strftime.return_value = '10:00'
        mock_datetime.now.return_value = datetime.now()  # For timestamp in signals
        
        # Mock last price for position sizing
        self.strategy.get_last_price = MagicMock(return_value=100.0)
        
        # Test signal generation
        signals = self.strategy.generate_signals()
        
        # Verify signals are generated based on momentum
        self.assertTrue(len(signals) > 0)
        
        # Check if highest momentum stock (GOOGL) is in signals
        has_googl = any(s['symbol'] == 'GOOGL' for s in signals)
        self.assertTrue(has_googl)

if __name__ == '__main__':
    unittest.main()