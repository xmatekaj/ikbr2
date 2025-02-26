"""
Utility functions for calculating financial and trading metrics.

This module provides functions to calculate various performance metrics
like Sharpe ratio, maximum drawdown, volatility, etc.
"""
import numpy as np
import pandas as pd
from typing import List, Union


def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.0, annualization_factor: int = 252) -> float:
    """
    Calculate the Sharpe ratio for a given set of returns.
    
    The Sharpe ratio is a measure of risk-adjusted performance, calculated as:
    (Mean Return - Risk Free Rate) / Standard Deviation of Returns
    
    Args:
        returns: List of period returns (daily, weekly, etc.)
        risk_free_rate: The risk-free rate for the period (default: 0.0)
        annualization_factor: Factor to annualize returns (252 for daily, 52 for weekly, 12 for monthly)
        
    Returns:
        The Sharpe ratio value
    """
    if not returns:
        return 0.0
    
    returns_array = np.array(returns)
    
    # Calculate mean return and standard deviation
    mean_return = np.mean(returns_array)
    std_dev = np.std(returns_array, ddof=1)  # Use sample standard deviation
    
    if std_dev == 0:
        return 0.0  # Avoid division by zero
    
    # Calculate daily Sharpe ratio
    daily_sharpe = (mean_return - risk_free_rate) / std_dev
    
    # Annualize the Sharpe ratio
    sharpe_ratio = daily_sharpe * np.sqrt(annualization_factor)
    
    return sharpe_ratio


def calculate_max_drawdown(equity_curve: pd.Series) -> float:
    """
    Calculate the maximum drawdown percentage from a series of equity values.
    
    Maximum drawdown is the maximum observed loss from a peak to a trough,
    before a new peak is attained.
    
    Args:
        equity_curve: A pandas Series of equity values over time
        
    Returns:
        The maximum drawdown as a percentage (0 to 100)
    """
    if equity_curve.empty:
        return 0.0
    
    # Calculate running maximum
    running_max = equity_curve.cummax()
    
    # Calculate drawdown in percentage terms
    drawdown = ((equity_curve - running_max) / running_max) * 100
    
    # Get the maximum drawdown
    max_drawdown = drawdown.min()
    
    # Return as a positive percentage for ease of interpretation
    return abs(max_drawdown)


def calculate_volatility(returns: List[float], annualization_factor: int = 252) -> float:
    """
    Calculate the annualized volatility (standard deviation) of returns.
    
    Args:
        returns: List of period returns (daily, weekly, etc.)
        annualization_factor: Factor to annualize volatility
        
    Returns:
        The annualized volatility as a percentage
    """
    if not returns:
        return 0.0
    
    returns_array = np.array(returns)
    
    # Calculate standard deviation
    std_dev = np.std(returns_array, ddof=1)  # Use sample standard deviation
    
    # Annualize the volatility
    annualized_vol = std_dev * np.sqrt(annualization_factor)
    
    # Convert to percentage
    return annualized_vol * 100


def calculate_sortino_ratio(returns: List[float], risk_free_rate: float = 0.0, annualization_factor: int = 252) -> float:
    """
    Calculate the Sortino ratio for a given set of returns.
    
    The Sortino ratio is similar to the Sharpe ratio but uses only
    downside deviation instead of total standard deviation.
    
    Args:
        returns: List of period returns (daily, weekly, etc.)
        risk_free_rate: The risk-free rate for the period (default: 0.0)
        annualization_factor: Factor to annualize returns
        
    Returns:
        The Sortino ratio value
    """
    if not returns:
        return 0.0
    
    returns_array = np.array(returns)
    
    # Calculate mean return
    mean_return = np.mean(returns_array)
    
    # Calculate downside returns (only negative returns)
    downside_returns = returns_array[returns_array < 0]
    
    if len(downside_returns) == 0:
        return float('inf')  # No downside risk
    
    # Calculate downside deviation
    downside_deviation = np.std(downside_returns, ddof=1)
    
    if downside_deviation == 0:
        return 0.0  # Avoid division by zero
    
    # Calculate daily Sortino ratio
    daily_sortino = (mean_return - risk_free_rate) / downside_deviation
    
    # Annualize the Sortino ratio
    sortino_ratio = daily_sortino * np.sqrt(annualization_factor)
    
    return sortino_ratio


def calculate_cagr(initial_value: float, final_value: float, years: float) -> float:
    """
    Calculate the Compound Annual Growth Rate (CAGR).
    
    Args:
        initial_value: The initial investment value
        final_value: The final investment value
        years: The number of years between initial and final value
        
    Returns:
        The CAGR as a percentage
    """
    if initial_value <= 0 or years <= 0:
        return 0.0
    
    # Calculate CAGR
    cagr = (final_value / initial_value) ** (1 / years) - 1
    
    # Convert to percentage
    return cagr * 100


def calculate_win_rate(wins: int, losses: int) -> float:
    """
    Calculate the win rate of trades.
    
    Args:
        wins: Number of winning trades
        losses: Number of losing trades
        
    Returns:
        The win rate as a percentage (0 to 100)
    """
    total_trades = wins + losses
    
    if total_trades == 0:
        return 0.0
    
    win_rate = (wins / total_trades) * 100
    return win_rate


def calculate_profit_factor(gross_profit: float, gross_loss: float) -> float:
    """
    Calculate the profit factor.
    
    Profit factor is calculated as: Gross Profit / Gross Loss
    
    Args:
        gross_profit: Total profit from winning trades
        gross_loss: Total loss from losing trades (as a positive number)
        
    Returns:
        The profit factor
    """
    if gross_loss == 0:
        return float('inf') if gross_profit > 0 else 0.0
    
    return gross_profit / gross_loss


def calculate_average_trade(total_profit_loss: float, total_trades: int) -> float:
    """
    Calculate the average profit/loss per trade.
    
    Args:
        total_profit_loss: The total profit or loss amount
        total_trades: The total number of trades
        
    Returns:
        The average profit/loss per trade
    """
    if total_trades == 0:
        return 0.0
    
    return total_profit_loss / total_trades


def calculate_expectancy(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """
    Calculate the system expectancy (expected return per trade).
    
    Expectancy = (Win Rate * Average Win) - (Loss Rate * Average Loss)
    
    Args:
        win_rate: The win rate as a decimal (0 to 1)
        avg_win: The average profit on winning trades
        avg_loss: The average loss on losing trades (as a positive number)
        
    Returns:
        The system expectancy
    """
    loss_rate = 1 - win_rate
    return (win_rate * avg_win) - (loss_rate * avg_loss)


def calculate_risk_of_ruin(win_rate: float, risk_reward_ratio: float) -> float:
    """
    Calculate the risk of ruin.
    
    This is a simple approximation assuming constant risk per trade
    and fixed risk/reward ratio.
    
    Args:
        win_rate: The win rate as a decimal (0 to 1)
        risk_reward_ratio: The risk-reward ratio (reward/risk)
        
    Returns:
        The risk of ruin as a percentage (0 to 100)
    """
    if win_rate >= 1.0 or win_rate <= 0.0:
        return 0.0 if win_rate >= 1.0 else 100.0
    
    # Calculate probability adjustment factor
    a = (1 - win_rate) / (1 - win_rate + (win_rate * risk_reward_ratio))
    
    # Calculate risk of ruin
    risk_of_ruin = a ** 100  # Assuming 100 units of capital
    
    return risk_of_ruin * 100  # Return as percentage

def calculate_drawdown(equity_curve):
    """
    Calculate the drawdown and maximum drawdown of an equity curve.
    
    Args:
        equity_curve: List or array of equity values over time
        
    Returns:
        tuple: (drawdowns, max_drawdown_percent, max_drawdown_duration)
    """
    import numpy as np
    
    # Convert to numpy array if it's not already
    equity = np.array(equity_curve)
    
    # Calculate running maximum
    running_max = np.maximum.accumulate(equity)
    
    # Calculate drawdown in percentage terms
    drawdown = (equity - running_max) / running_max * 100
    
    # Find the maximum drawdown
    max_drawdown = drawdown.min()
    
    # Calculate drawdown duration
    is_drawdown = drawdown < 0
    duration_counter = 0
    max_duration = 0
    
    for is_dd in is_drawdown:
        if is_dd:
            duration_counter += 1
        else:
            max_duration = max(max_duration, duration_counter)
            duration_counter = 0
    
    # In case we're still in a drawdown at the end
    max_duration = max(max_duration, duration_counter)
    
    return drawdown, max_drawdown, max_duration