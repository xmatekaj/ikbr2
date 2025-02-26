"""
Performance Tracking System for IKBR Trader Bot.

This module provides functionality to track and analyze trading performance,
including metrics like profit and loss, win rate, Sharpe ratio, drawdown,
and other performance indicators.
"""
import datetime
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

from ..utils.metrics import calculate_sharpe_ratio, calculate_max_drawdown
from ..utils.logging import system_logger

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """Data class for storing individual trade information."""
    trade_id: str
    strategy_id: str
    symbol: str
    quantity: int
    entry_price: float
    entry_time: datetime.datetime
    exit_price: Optional[float] = None
    exit_time: Optional[datetime.datetime] = None
    profit_loss: Optional[float] = None
    profit_loss_percent: Optional[float] = None
    trade_duration: Optional[float] = None  # in seconds
    trade_type: str = "LONG"  # "LONG" or "SHORT"
    status: str = "OPEN"  # "OPEN", "CLOSED", "CANCELED"
    tags: List[str] = None


class PerformanceTracker:
    """
    Tracks and analyzes trading performance metrics.
    
    This class is responsible for recording trades, calculating performance
    metrics, and generating performance reports.
    """
    
    def __init__(self, initial_capital: float):
        """
        Initialize the performance tracker.
        
        Args:
            initial_capital: The initial trading capital
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.trades: Dict[str, TradeRecord] = {}
        self.daily_equity: Dict[datetime.date, float] = {}
        self.daily_returns: Dict[datetime.date, float] = {}
        
        # Performance metrics
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.gross_profit = 0.0
        self.gross_loss = 0.0
        self.commission_paid = 0.0
        
        # Track metrics by strategy
        self.strategy_metrics: Dict[str, Dict] = {}
        
        # Record start date
        self.start_date = datetime.datetime.now().date()
        
        logger.info(f"Performance tracker initialized with capital: ${initial_capital:.2f}")
    
    def record_trade(self, trade: TradeRecord) -> None:
        """
        Record a new trade or update an existing trade.
        
        Args:
            trade: The trade record to add or update
        """
        if trade.trade_id in self.trades:
            # Update existing trade
            existing_trade = self.trades[trade.trade_id]
            
            # Update exit information
            existing_trade.exit_price = trade.exit_price
            existing_trade.exit_time = trade.exit_time
            existing_trade.status = trade.status
            
            # Calculate P&L if the trade is closed
            if trade.status == "CLOSED" and trade.exit_price is not None:
                # Calculate profit/loss
                if trade.trade_type == "LONG":
                    profit_loss = (trade.exit_price - existing_trade.entry_price) * existing_trade.quantity
                else:  # SHORT
                    profit_loss = (existing_trade.entry_price - trade.exit_price) * existing_trade.quantity
                
                existing_trade.profit_loss = profit_loss
                existing_trade.profit_loss_percent = (profit_loss / (existing_trade.entry_price * existing_trade.quantity)) * 100
                
                # Calculate trade duration
                if existing_trade.entry_time and trade.exit_time:
                    existing_trade.trade_duration = (trade.exit_time - existing_trade.entry_time).total_seconds()
                
                # Update overall metrics
                self.total_trades += 1
                if profit_loss > 0:
                    self.winning_trades += 1
                    self.gross_profit += profit_loss
                else:
                    self.losing_trades += 1
                    self.gross_loss += abs(profit_loss)
                
                # Update current capital
                self.current_capital += profit_loss
                
                # Update strategy metrics
                strategy_id = existing_trade.strategy_id
                if strategy_id not in self.strategy_metrics:
                    self._initialize_strategy_metrics(strategy_id)
                
                self.strategy_metrics[strategy_id]["total_trades"] += 1
                if profit_loss > 0:
                    self.strategy_metrics[strategy_id]["winning_trades"] += 1
                    self.strategy_metrics[strategy_id]["gross_profit"] += profit_loss
                else:
                    self.strategy_metrics[strategy_id]["losing_trades"] += 1
                    self.strategy_metrics[strategy_id]["gross_loss"] += abs(profit_loss)
                
                logger.info(f"Trade {trade.trade_id} closed with P&L: ${profit_loss:.2f} ({existing_trade.profit_loss_percent:.2f}%)")
        else:
            # Add new trade
            if trade.tags is None:
                trade.tags = []
            self.trades[trade.trade_id] = trade
            
            # Initialize strategy metrics if needed
            if trade.strategy_id not in self.strategy_metrics:
                self._initialize_strategy_metrics(trade.strategy_id)
            
            logger.info(f"New trade recorded: {trade.trade_id}, Symbol: {trade.symbol}, Type: {trade.trade_type}")
    
    def _initialize_strategy_metrics(self, strategy_id: str) -> None:
        """
        Initialize tracking metrics for a new strategy.
        
        Args:
            strategy_id: The strategy identifier
        """
        self.strategy_metrics[strategy_id] = {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "commission_paid": 0.0,
        }
    
    def update_daily_equity(self, date: datetime.date = None, equity_value: float = None) -> None:
        """
        Update the daily equity value.
        
        Args:
            date: The date to update (defaults to today)
            equity_value: The equity value (defaults to current_capital)
        """
        if date is None:
            date = datetime.datetime.now().date()
        
        if equity_value is None:
            equity_value = self.current_capital
        
        self.daily_equity[date] = equity_value
        
        # Calculate daily return if we have a previous day
        prev_day = date - datetime.timedelta(days=1)
        if prev_day in self.daily_equity:
            daily_return = (equity_value / self.daily_equity[prev_day]) - 1
            self.daily_returns[date] = daily_return
    
    def get_performance_summary(self) -> Dict:
        """
        Get a summary of performance metrics.
        
        Returns:
            A dictionary containing performance metrics
        """
        # Calculate win rate
        win_rate = 0
        if self.total_trades > 0:
            win_rate = (self.winning_trades / self.total_trades) * 100
        
        # Calculate profit factor
        profit_factor = 0
        if self.gross_loss > 0:
            profit_factor = self.gross_profit / self.gross_loss
        
        # Calculate returns
        total_return_pct = ((self.current_capital / self.initial_capital) - 1) * 100
        
        # Calculate Sharpe ratio and max drawdown if we have daily returns
        sharpe_ratio = None
        max_drawdown = None
        
        if len(self.daily_returns) > 0:
            returns_list = list(self.daily_returns.values())
            sharpe_ratio = calculate_sharpe_ratio(returns_list)
            
            equity_series = pd.Series(self.daily_equity)
            max_drawdown = calculate_max_drawdown(equity_series)
        
        # Calculate average trade metrics
        avg_profit_per_winning_trade = 0
        if self.winning_trades > 0:
            avg_profit_per_winning_trade = self.gross_profit / self.winning_trades
        
        avg_loss_per_losing_trade = 0
        if self.losing_trades > 0:
            avg_loss_per_losing_trade = self.gross_loss / self.losing_trades
        
        # Calculate trading duration
        if self.total_trades > 0:
            trading_days = (datetime.datetime.now().date() - self.start_date).days
            if trading_days == 0:
                trading_days = 1
        else:
            trading_days = 0
        
        return {
            "initial_capital": self.initial_capital,
            "current_capital": self.current_capital,
            "total_profit_loss": self.current_capital - self.initial_capital,
            "total_profit_loss_pct": total_return_pct,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": win_rate,
            "gross_profit": self.gross_profit,
            "gross_loss": self.gross_loss,
            "profit_factor": profit_factor,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "avg_profit_per_winning_trade": avg_profit_per_winning_trade,
            "avg_loss_per_losing_trade": avg_loss_per_losing_trade,
            "commission_paid": self.commission_paid,
            "trading_period_days": trading_days,
            "trades_per_day": self.total_trades / trading_days if trading_days > 0 else 0,
        }
    
    def get_strategy_performance(self, strategy_id: str) -> Dict:
        """
        Get performance metrics for a specific strategy.
        
        Args:
            strategy_id: The strategy identifier
            
        Returns:
            A dictionary containing strategy-specific performance metrics
        """
        if strategy_id not in self.strategy_metrics:
            return {"error": f"Strategy {strategy_id} not found in performance tracker"}
        
        metrics = self.strategy_metrics[strategy_id]
        
        # Calculate win rate
        win_rate = 0
        if metrics["total_trades"] > 0:
            win_rate = (metrics["winning_trades"] / metrics["total_trades"]) * 100
        
        # Calculate profit factor
        profit_factor = 0
        if metrics["gross_loss"] > 0:
            profit_factor = metrics["gross_profit"] / metrics["gross_loss"]
        
        return {
            "strategy_id": strategy_id,
            "total_trades": metrics["total_trades"],
            "winning_trades": metrics["winning_trades"],
            "losing_trades": metrics["losing_trades"],
            "win_rate": win_rate,
            "gross_profit": metrics["gross_profit"],
            "gross_loss": metrics["gross_loss"],
            "net_profit": metrics["gross_profit"] - metrics["gross_loss"],
            "profit_factor": profit_factor,
            "commission_paid": metrics["commission_paid"],
        }
    
    def get_trade_history(self, 
                          strategy_id: str = None, 
                          symbol: str = None, 
                          status: str = None,
                          from_date: datetime.datetime = None,
                          to_date: datetime.datetime = None) -> List[TradeRecord]:
        """
        Get filtered trade history.
        
        Args:
            strategy_id: Filter by strategy ID
            symbol: Filter by symbol
            status: Filter by trade status
            from_date: Filter by trades after this date
            to_date: Filter by trades before this date
            
        Returns:
            A list of filtered trade records
        """
        filtered_trades = []
        
        for trade in self.trades.values():
            # Apply filters
            if strategy_id and trade.strategy_id != strategy_id:
                continue
            if symbol and trade.symbol != symbol:
                continue
            if status and trade.status != status:
                continue
            if from_date and trade.entry_time < from_date:
                continue
            if to_date and trade.entry_time > to_date:
                continue
            
            filtered_trades.append(trade)
        
        # Sort by entry time
        filtered_trades.sort(key=lambda t: t.entry_time)
        
        return filtered_trades
    
    def get_equity_curve(self) -> Dict[datetime.date, float]:
        """
        Get the equity curve data.
        
        Returns:
            A dictionary mapping dates to equity values
        """
        return dict(sorted(self.daily_equity.items()))
    
    def generate_performance_report(self) -> str:
        """
        Generate a comprehensive performance report.
        
        Returns:
            A string containing the formatted performance report
        """
        summary = self.get_performance_summary()
        
        report = "====== PERFORMANCE REPORT ======\n\n"
        
        # Overall performance
        report += "OVERALL PERFORMANCE\n"
        report += "-------------------\n"
        report += f"Initial Capital: ${summary['initial_capital']:.2f}\n"
        report += f"Current Capital: ${summary['current_capital']:.2f}\n"
        report += f"Total P&L: ${summary['total_profit_loss']:.2f} ({summary['total_profit_loss_pct']:.2f}%)\n"
        report += f"Trading Period: {summary['trading_period_days']} days\n\n"
        
        # Trade statistics
        report += "TRADE STATISTICS\n"
        report += "----------------\n"
        report += f"Total Trades: {summary['total_trades']}\n"
        report += f"Winning Trades: {summary['winning_trades']} ({summary['win_rate']:.2f}%)\n"
        report += f"Losing Trades: {summary['losing_trades']}\n"
        report += f"Profit Factor: {summary['profit_factor']:.2f}\n"
        
        if summary['sharpe_ratio'] is not None:
            report += f"Sharpe Ratio: {summary['sharpe_ratio']:.2f}\n"
        
        if summary['max_drawdown'] is not None:
            report += f"Max Drawdown: {summary['max_drawdown']:.2f}%\n"
        
        report += f"Avg Profit (Winners): ${summary['avg_profit_per_winning_trade']:.2f}\n"
        report += f"Avg Loss (Losers): ${summary['avg_loss_per_losing_trade']:.2f}\n"
        report += f"Commission Paid: ${summary['commission_paid']:.2f}\n\n"
        
        # Strategy performance
        report += "STRATEGY PERFORMANCE\n"
        report += "--------------------\n"
        
        for strategy_id in self.strategy_metrics.keys():
            strategy_perf = self.get_strategy_performance(strategy_id)
            report += f"\nStrategy: {strategy_id}\n"
            report += f"  Total Trades: {strategy_perf['total_trades']}\n"
            report += f"  Win Rate: {strategy_perf['win_rate']:.2f}%\n"
            report += f"  Net Profit: ${strategy_perf['net_profit']:.2f}\n"
            report += f"  Profit Factor: {strategy_perf['profit_factor']:.2f}\n"
        
        return report