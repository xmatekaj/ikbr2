# tests/integration/test_order_execution.py
import unittest
from unittest.mock import MagicMock, patch
import time
from datetime import datetime
from src.connectors.ibkr.order_manager import IBKROrderManager
from src.trading.trade_manager import TradeManager

class TestOrderExecution(unittest.TestCase):
    
    @patch('src.connectors.ibkr.client.IBKRClient')
    def setUp(self, mock_client_class):
         # Create mock IBKR client
        self.mock_client = mock_client_class.return_value
        self.mock_client.connected = True
        
        # Create order manager with correct constructor arguments
        self.order_manager = IBKROrderManager(host="127.0.0.1", port=7497, client_id=1)
        self.order_manager.client = self.mock_client  # Manually set the client
        
        # Mock methods
        self.order_manager.connect_and_run = MagicMock(return_value=True)
        self.order_manager.place_order = MagicMock(return_value=12345)
        self.order_manager.get_order_status = MagicMock(return_value={
            'status': 'SUBMITTED',
            'filled_quantity': 0,
            'remaining_quantity': 100,
            'avg_fill_price': 0.0,
            'is_complete': False
        })
        
        # Create trade manager
        self.trade_manager = TradeManager(self.order_manager)
    
    def test_place_and_track_trade(self):
        """Test placing a trade and tracking it through to completion"""
        # Place a trade
        trade_id = self.trade_manager.place_trade(
            strategy_id="test_strategy",
            symbol="AAPL",
            direction="BUY",
            quantity=100,
            order_type="MARKET"
        )
        
        self.assertIsNotNone(trade_id)
        self.assertEqual(len(self.trade_manager.active_trades), 1)
        
        # Verify correct order placement
        self.order_manager.place_market_order.assert_called_once()
        
        # Simulate order update
        order_id = self.trade_manager.active_trades[trade_id]['order_id']
        
        # Update with partial fill
        self.trade_manager.update_trade(
            order_id=order_id,
            status="PARTIALLY_FILLED",
            fill_price=150.0,
            filled_quantity=50,
            commission=2.5
        )
        
        # Check trade status
        self.assertEqual(self.trade_manager.active_trades[trade_id]['status'], "PARTIALLY_FILLED")
        
        # Update with full fill
        self.trade_manager.update_trade(
            order_id=order_id,
            status="FILLED",
            fill_price=150.0,
            filled_quantity=100,
            commission=5.0
        )
        
        # Trade should be moved to history
        self.assertEqual(len(self.trade_manager.active_trades), 0)
        self.assertEqual(len(self.trade_manager.trade_history), 1)
        
        # Check trade history
        trade = self.trade_manager.get_trade(trade_id)
        self.assertEqual(trade['status'], "FILLED")
        self.assertEqual(trade['fill_price'], 150.0)
        self.assertEqual(trade['commission'], 5.0)
    
    def test_cancel_trade(self):
        """Test cancelling a trade"""
        # Place a trade
        trade_id = self.trade_manager.place_trade(
            strategy_id="test_strategy",
            symbol="MSFT",
            direction="BUY",
            quantity=50,
            order_type="LIMIT",
            price=250.0
        )
        
        # Mock cancel order
        self.order_manager.cancel_order = MagicMock(return_value=True)
        
        # Cancel the trade
        result = self.trade_manager.cancel_trade(trade_id)
        self.assertTrue(result)
        
        # Verify cancel was called
        self.order_manager.cancel_order.assert_called_once()
        
        # Simulate cancellation update
        order_id = self.trade_manager.active_trades[trade_id]['order_id']
        self.trade_manager.update_trade(
            order_id=order_id,
            status="CANCELLED"
        )
        
        # Trade should be moved to history
        self.assertEqual(len(self.trade_manager.active_trades), 0)
        self.assertEqual(len(self.trade_manager.trade_history), 1)
        
        # Check trade history
        trade = self.trade_manager.get_trade(trade_id)
        self.assertEqual(trade['status'], "CANCELLED")

if __name__ == '__main__':
    unittest.main()