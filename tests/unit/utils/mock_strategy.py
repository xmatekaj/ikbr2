# tests/utils/mock_strategy.py
from src.strategies.base_strategy import BaseStrategy
from datetime import datetime

class MockStrategy(BaseStrategy):
    """A simple mock strategy for testing purposes"""
    
    def __init__(self, **kwargs):
        super().__init__(name="MockStrategy", **kwargs)
        self.signal_called = 0
        self.should_generate_signals = True
        self.last_prices = {}
    
    def generate_signals(self):
        """Generate mock trading signals"""
        self.signal_called += 1
        signals = []
        
        if not self.should_generate_signals:
            return signals
        
        # Generate a buy signal for first symbol in the list
        if self.symbols and len(self.symbols) > 0:
            symbol = self.symbols[0]
            price = self.get_last_price(symbol) or 100.0
            self.last_prices[symbol] = price
            
            signals.append({
                'symbol': symbol,
                'action': 'BUY',
                'quantity': 1,
                'type': 'market',
                'reason': 'Test signal',
                'timestamp': datetime.now()
            })
        
        return signals