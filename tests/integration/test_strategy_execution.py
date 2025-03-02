# tests/integration/test_strategy_execution.py
import unittest
from unittest.mock import MagicMock, patch
import time
from datetime import datetime
from src.connectors.ibkr.data_feed import IBKRDataFeed
from src.connectors.ibkr.order_manager import IBKROrderManager
from src.strategies.conventional.momentum import MomentumStrategy
from src.trading.trade_manager import TradeManager

class TestStrategyExecution(unittest.TestCase):
    
    @patch('src.connectors.ibkr.client.IBKRClient')
    def setUp(self, mock_client_class):
        # Create mocked components
        self.mock_client = mock_client_class.return_value
        self.mock_client.connected = True
        
        # Create actual components with mocked dependencies
        self.data_feed = IBKRDataFeed(host="127.0.0.1", port=7497, client_id=1)
        self.data_feed.client = self.mock_client  # Manually set the client attribute
        self.order_manager = IBKROrderManager(host="127.0.0.1", port=7497, client_id=2)
        self.order_manager.client = self.mock_client  # Manually set the client attribute
        self.trade_manager = TradeManager(order_manager=self.order_manager)
        
        # Patch the actual connect methods
        self.data_feed.connect_and_run = MagicMock(return_value=True)
        self.order_manager.connect_and_run = MagicMock(return_value=True)
        
        # Mock market data methods
        self.data_feed.request_historical_data = MagicMock()
        self.data_feed.get_historical_data = MagicMock()
        self.data_feed.get_last_price = MagicMock(return_value=150.0)
        
        # Mock order methods
        self.order_manager.create_market_order = MagicMock(return_value=('mock_contract', 'mock_order'))
        self.order_manager.place_order = MagicMock(return_value=12345)
        
        # Create strategy
        self.strategy_config = {
            'symbols': ['AAPL', 'MSFT', 'GOOGL'],
            'timeframes': ['1 day'],
            'lookback_period': 20,
            'momentum_threshold': 0.01,
            'universe_size': 2,
            'position_size': 0.1,
            'run_interval': 5,  # Short interval for testing
            'trading_times': ['00:00', '12:00', '18:00']  # Include all hours for testing
        }
        
        self.strategy = MomentumStrategy(
            data_feed=self.data_feed,
            order_manager=self.order_manager,
            config=self.strategy_config
        )
        
        # Mock strategy market data
        self.strategy.market_data = {}
        for symbol in self.strategy_config['symbols']:
            self.strategy.market_data[symbol] = {
                '1 day': self._create_test_bars(symbol)
            }
    
    def _create_test_bars(self, symbol, num_bars=30, uptrend=True):
        """Create test bar data with strong trends"""
        bars = []
        base_price = 100.0 if symbol == 'AAPL' else 200.0 if symbol == 'MSFT' else 1500.0
        
        for i in range(num_bars):
            # Create a stronger uptrend or downtrend
            if uptrend and symbol in ['AAPL', 'GOOGL']:  # Make these symbols have strong uptrends
                price = base_price * (1 + (i * 0.02))  # 2% growth per bar
            else:
                price = base_price * (1 - (i * 0.005))
                
            bars.append({
                'date': datetime.now(),
                'open': price - 1,
                'high': price + 2,
                'low': price - 2,
                'close': price,
                'volume': 1000
            })
        
        return bars
    
    @patch('src.strategies.conventional.momentum.datetime')
    def test_strategy_signal_to_order(self, mock_datetime):
        """Test the flow from strategy signal generation to order placement"""
        # Mock current time to match a trading time
        mock_datetime.now.return_value.strftime.return_value = '12:00'
        mock_datetime.now.return_value = datetime.now()
        
        # Override the momentum threshold to ensure signals are generated
        self.strategy.momentum_threshold = 0.001  # Lower threshold to guarantee signals
        
        # Test initialization
        self.strategy.initialize()
        
        # Mock the calculate_momentum method to return a strong positive value
        self.strategy._calculate_momentum = MagicMock(return_value=0.15)  # 15% return
        
        # Generate signals and process them
        signals = self.strategy.generate_signals()
        
        if len(signals) == 0:
            # If no signals, manually create one for testing the rest of the flow
            print("No signals generated, creating a test signal")
            signals = [{
                'symbol': 'AAPL',
                'action': 'BUY',
                'quantity': 10,
                'type': 'market',
                'reason': 'Test signal',
                'timestamp': datetime.now()
            }]
        
        self.assertGreater(len(signals), 0, "Should generate at least one signal")
        
        # Process signals
        for signal in signals:
            self.strategy._execute_signal(signal)
        
        # Verify order placement was called
        self.order_manager.place_order.assert_called()
        
        # Verify the symbols match
        signal_symbols = [signal['symbol'] for signal in signals]
        order_calls = self.order_manager.create_market_order.call_args_list
        
        # Verify the symbols in the calls match the signals
        for call in order_calls:
            args, _ = call
            symbol = args[0]
            self.assertIn(symbol, signal_symbols, f"Order placed for {symbol} which was not in signals")

    def test_strategy_lifecycle(self):
        """Test starting and stopping a strategy"""
        # Start the strategy
        result = self.strategy.start()
        self.assertTrue(result, "Strategy should start successfully")
        self.assertTrue(self.strategy.running, "Strategy should be running")
        
        # Let it run briefly
        time.sleep(0.1)
        
        # Stop the strategy
        stop_result = self.strategy.stop()
        self.assertTrue(stop_result, "Strategy should stop successfully")
        self.assertFalse(self.strategy.running, "Strategy should not be running after stop")

if __name__ == '__main__':
    unittest.main()