# tests/unit/connectors/test_ibkr_client.py
import unittest
from unittest.mock import MagicMock, patch
from src.connectors.ibkr.client import IBKRClient

class TestIBKRClient(unittest.TestCase):
    
    def setUp(self):
        # Create a mock for EClient to avoid actual connections
        with patch('src.connectors.ibkr.client.EClient'):
            self.client = IBKRClient(host="mock_host", port=7497, client_id=1)
            self.client.connect = MagicMock()
            self.client.run = MagicMock()
            self.client.disconnect = MagicMock()
            
    def test_initialization(self):
        self.assertEqual(self.client.host, "mock_host")
        self.assertEqual(self.client.port, 7497)
        self.assertEqual(self.client.client_id, 1)
        self.assertFalse(self.client.connected)
        
    def test_connect_and_run(self):
        # Mock the connection state
        self.client.connected = False
        
        # Test the connection method
        self.client.connect_and_run()
        
        # Verify it called the right methods
        self.client.connect.assert_called_once()
        
    def test_disconnect_and_stop(self):
        # Setup
        self.client.connected = True
        
        # Test
        self.client.disconnect_and_stop()
        
        # Verify
        self.client.disconnect.assert_called_once()
        
    def test_get_next_req_id(self):
        # Initial req_id should be 0
        self.assertEqual(self.client._req_id, 0)
        
        # First call should return 1
        first_id = self.client.get_next_req_id()
        self.assertEqual(first_id, 1)
        
        # Second call should return 2
        second_id = self.client.get_next_req_id()
        self.assertEqual(second_id, 2)
        
    def test_create_stock_contract(self):
        contract = self.client.create_stock_contract("AAPL")
        self.assertEqual(contract.symbol, "AAPL")
        self.assertEqual(contract.secType, "STK")
        self.assertEqual(contract.exchange, "SMART")
        self.assertEqual(contract.currency, "USD")

if __name__ == '__main__':
    unittest.main()