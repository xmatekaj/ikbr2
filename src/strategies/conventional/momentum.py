"""
Momentum trading strategy implementation.
This strategy buys assets that have shown strength in the recent past.
"""
import logging
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

# Import the base strategy
from src.strategies.base_strategy import BaseStrategy

# Set up logger
logger = logging.getLogger(__name__)

class MomentumStrategy(BaseStrategy):
    """
    Momentum trading strategy that buys securities showing upward price movement.
    This is a simple implementation that uses relative strength over a defined lookback period.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the momentum strategy.
        
        Args:
            **kwargs: Arguments to pass to the base strategy
        """
        super().__init__(name="Momentum", **kwargs)
        
        # Strategy-specific parameters
        self.lookback_period = self.config.get('lookback_period', 20)  # Default 20 bars
        self.momentum_threshold = self.config.get('momentum_threshold', 0.02)  # 2% threshold
        self.universe_size = self.config.get('universe_size', 5)  # Top N symbols to trade
        
        # Calculate trading schedule 
        self.trading_times = self.config.get('trading_times', ['10:00', '14:00'])  # Default trading times
        
        logger.info(f"Momentum strategy initialized with lookback={self.lookback_period}, threshold={self.momentum_threshold}")
    
    def initialize(self) -> bool:
        """
        Initialize the strategy.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        # Call the parent's initialize method first
        if not super().initialize():
            return False
        
        try:
            # Additional initialization for momentum strategy
            logger.info("Performing additional initialization for momentum strategy")
            
            # Pre-calculate momentum for initial ranking
            self._calculate_momentum_for_all()
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing momentum strategy: {e}")
            return False
    
    def generate_signals(self) -> List[Dict[str, Any]]:
        """
        Generate trading signals based on momentum calculations.
        
        Returns:
            List[Dict[str, Any]]: A list of trading signals
        """
        signals = []
        
        try:
            # Check if it's a good time to trade
            current_time = datetime.now().strftime('%H:%M')
            if not self._is_trading_time(current_time):
                return signals
            
            # Calculate momentum for all symbols
            momentum_scores = self._calculate_momentum_for_all()
            
            # Rank symbols by momentum
            ranked_symbols = sorted(momentum_scores.items(), key=lambda x: x[1], reverse=True)
            
            # Get current positions
            current_positions = set(self.positions.keys())
            
            # Determine top symbols that exceed the threshold
            top_symbols = []
            for symbol, momentum in ranked_symbols:
                if momentum > self.momentum_threshold:
                    top_symbols.append(symbol)
                    if len(top_symbols) >= self.universe_size:
                        break
            
            logger.info(f"Top symbols by momentum: {top_symbols}")
            
            # Generate buy signals for top symbols not already in portfolio
            for symbol in top_symbols:
                if symbol not in current_positions and len(current_positions) < self.max_positions:
                    # Determine position size
                    quantity = self._calculate_position_size(symbol)
                    
                    if quantity > 0:
                        signals.append({
                            'symbol': symbol,
                            'action': 'BUY',
                            'quantity': quantity,
                            'type': 'market',
                            'reason': f'Momentum score: {momentum_scores[symbol]:.2%}',
                            'timestamp': datetime.now()
                        })
                        logger.info(f"Generated BUY signal for {symbol} with momentum {momentum_scores[symbol]:.2%}")
            
            # Generate sell signals for positions not in top symbols
            for symbol in current_positions:
                if symbol not in top_symbols:
                    position = self.positions[symbol]
                    signals.append({
                        'symbol': symbol,
                        'action': 'SELL',
                        'quantity': position['quantity'],
                        'type': 'market',
                        'reason': 'No longer in top momentum symbols',
                        'timestamp': datetime.now()
                    })
                    logger.info(f"Generated SELL signal for {symbol} as it's no longer in top momentum symbols")
        
        except Exception as e:
            logger.error(f"Error generating momentum signals: {e}")
        
        return signals
    
    def _calculate_momentum_for_all(self) -> Dict[str, float]:
        """
        Calculate momentum scores for all symbols in the watchlist.
        
        Returns:
            Dict[str, float]: Dictionary of symbols and their momentum scores
        """
        momentum_scores = {}
        
        for symbol in self.symbols:
            momentum = self._calculate_momentum(symbol)
            if momentum is not None:
                momentum_scores[symbol] = momentum
        
        return momentum_scores
    
    def _calculate_momentum(self, symbol: str) -> Optional[float]:
        """
        Calculate momentum for a single symbol.
        
        Args:
            symbol: The stock symbol
            
        Returns:
            Optional[float]: Momentum score, or None if data is insufficient
        """
        # Use daily data for momentum calculation
        timeframe = "1 day"
        
        if symbol not in self.market_data or timeframe not in self.market_data[symbol]:
            return None
        
        bars = self.market_data[symbol][timeframe]
        if len(bars) < self.lookback_period:
            logger.warning(f"Insufficient data for {symbol} momentum calculation")
            return None
        
        # Get the most recent bars
        recent_bars = bars[-self.lookback_period:]
        
        # Extract close prices
        closes = [bar['close'] for bar in recent_bars]
        
        # Calculate momentum as percentage change from start to end
        start_price = closes[0]
        end_price = closes[-1]
        
        if start_price <= 0:
            return None
        
        momentum = (end_price - start_price) / start_price
        
        return momentum
    
    def _calculate_position_size(self, symbol: str) -> int:
        """
        Calculate the position size for a symbol.
        
        Args:
            symbol: The stock symbol
            
        Returns:
            int: Number of shares to buy
        """
        # Get the last price
        price = self.get_last_price(symbol)
        if price is None or price <= 0:
            logger.warning(f"Unable to get valid price for {symbol}")
            return 0
        
        # Default position value (could be based on account size in a real implementation)
        position_value = 10000 * self.position_size  # Example: $10,000 * position size
        
        # Calculate number of shares
        quantity = int(position_value / price)
        
        return max(1, quantity)  # Ensure at least 1 share
    
    def _is_trading_time(self, current_time: str) -> bool:
        """
        Check if it's an appropriate time to trade.
        
        Args:
            current_time: Current time in HH:MM format
            
        Returns:
            bool: True if it's a trading time, False otherwise
        """
        return current_time in self.trading_times


# Example of strategy configuration (would normally come from a config file)
EXAMPLE_CONFIG = {
    'symbols': ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'FB', 'TSLA', 'NVDA', 'AMD', 'INTC', 'IBM'],
    'timeframes': ['1 min', '1 hour', '1 day'],
    'lookback_period': 20,
    'momentum_threshold': 0.02,
    'universe_size': 5,
    'max_positions': 3,
    'position_size': 0.1,
    'run_interval': 300,  # 5 minutes
    'trading_times': ['10:00', '11:00', '14:00', '15:00']  # Trading at these specific times
}