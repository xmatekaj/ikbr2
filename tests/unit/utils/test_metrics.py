# tests/unit/utils/test_metrics.py
import unittest
import numpy as np
import pandas as pd
from src.utils.metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_volatility,
    calculate_sortino_ratio,
    calculate_cagr,
    calculate_win_rate,
    calculate_profit_factor,
    calculate_drawdown
)

class TestMetrics(unittest.TestCase):
    
    def setUp(self):
        # Sample returns data for testing
        self.positive_returns = [0.01, 0.02, 0.015, -0.005, 0.025, 0.01]
        self.negative_returns = [-0.01, -0.02, -0.015, -0.005, -0.025, -0.01]
        self.mixed_returns = [0.03, -0.02, 0.015, -0.01, 0.02, -0.015]
        
        # Sample equity curve
        self.equity_curve = pd.Series([
            10000, 10100, 10200, 10150, 10050, 10000, 10200, 10300, 10200, 10400
        ])
        
        # Sample equity curve with drawdown
        self.drawdown_equity = pd.Series([
            10000, 10200, 10300, 10100, 9900, 9800, 9900, 10000, 10200, 10300
        ])
    
    def test_sharpe_ratio(self):
        # Test positive returns
        sharpe_positive = calculate_sharpe_ratio(self.positive_returns)
        self.assertGreater(sharpe_positive, 0)
        
        # Test negative returns
        sharpe_negative = calculate_sharpe_ratio(self.negative_returns)
        self.assertLess(sharpe_negative, 0)
        
        # Test mixed returns
        sharpe_mixed = calculate_sharpe_ratio(self.mixed_returns)
        self.assertIsInstance(sharpe_mixed, float)
        
        # Test empty returns
        sharpe_empty = calculate_sharpe_ratio([])
        self.assertEqual(sharpe_empty, 0.0)
    
    def test_max_drawdown(self):
        # Test equity curve with clear drawdown
        drawdown = calculate_max_drawdown(self.drawdown_equity)
        self.assertGreater(drawdown, 0)
        
        # The max drawdown should be around 4.85% (from 10300 to 9800)
        expected_drawdown = ((9800 - 10300) / 10300) * 100
        self.assertAlmostEqual(drawdown, abs(expected_drawdown), places=1)
        
        # Test increasing equity curve (should have minimal drawdown)
        increasing_equity = pd.Series([10000, 10100, 10200, 10300, 10400])
        min_drawdown = calculate_max_drawdown(increasing_equity)
        self.assertAlmostEqual(min_drawdown, 0.0, places=1)
    
    def test_volatility(self):
        # Test with known standard deviation
        known_returns = [0.01, 0.01, 0.01, 0.01, 0.01]  # All same return
        vol = calculate_volatility(known_returns)
        self.assertAlmostEqual(vol, 0.0, places=4)  # Should be zero volatility
        
        # Test with mixed returns
        mixed_vol = calculate_volatility(self.mixed_returns)
        self.assertGreater(mixed_vol, 0)
    
    def test_sortino_ratio(self):
        # Test with no downside risk (all positive returns)
        all_positive = [0.01, 0.02, 0.03]
        sortino_positive = calculate_sortino_ratio(all_positive)
        self.assertEqual(sortino_positive, float('inf'))  # No downside risk
        
        # Test with mixed returns
        sortino_mixed = calculate_sortino_ratio(self.mixed_returns)
        self.assertIsInstance(sortino_mixed, float)
    
    def test_cagr(self):
        # Test with known values
        cagr = calculate_cagr(initial_value=10000, final_value=12100, years=2)
        expected_cagr = ((12100 / 10000) ** (1/2) - 1) * 100  # 10% per year
        self.assertAlmostEqual(cagr, expected_cagr, places=4)
        
        # Test edge cases
        cagr_zero_initial = calculate_cagr(initial_value=0, final_value=10000, years=1)
        self.assertEqual(cagr_zero_initial, 0.0)
        
        cagr_zero_years = calculate_cagr(initial_value=10000, final_value=12000, years=0)
        self.assertEqual(cagr_zero_years, 0.0)
    
    def test_win_rate(self):
        # Test with all wins
        all_wins = calculate_win_rate(wins=10, losses=0)
        self.assertEqual(all_wins, 100.0)
        
        # Test with all losses
        all_losses = calculate_win_rate(wins=0, losses=10)
        self.assertEqual(all_losses, 0.0)
        
        # Test with mixed
        mixed = calculate_win_rate(wins=7, losses=3)
        self.assertEqual(mixed, 70.0)
        
        # Test with no trades
        no_trades = calculate_win_rate(wins=0, losses=0)
        self.assertEqual(no_trades, 0.0)
    
    def test_profit_factor(self):
        # Test with only profits
        only_profits = calculate_profit_factor(gross_profit=1000, gross_loss=0)
        self.assertEqual(only_profits, float('inf'))
        
        # Test with only losses
        only_losses = calculate_profit_factor(gross_profit=0, gross_loss=1000)
        self.assertEqual(only_losses, 0.0)
        
        # Test with mixed
        mixed = calculate_profit_factor(gross_profit=1000, gross_loss=500)
        self.assertEqual(mixed, 2.0)
    
    def test_drawdown(self):
        # Test the drawdown calculation function
        drawdowns, max_dd, max_duration = calculate_drawdown(self.drawdown_equity)
        
        self.assertIsInstance(drawdowns, np.ndarray)
        self.assertLess(max_dd, 0)  # Should be negative percentage
        self.assertGreater(max_duration, 0)  # Should be positive number of periods

if __name__ == '__main__':
    unittest.main()