# tests/utils/test_utils.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_test_price_bars(symbol, days=30, interval='1d', trend='up', volatility=0.02, 
                          start_price=100.0, start_date=None):
    """
    Create test price bars for backtesting and unit tests.
    
    Args:
        symbol: Stock symbol
        days: Number of days to generate
        interval: Time interval ('1d', '1h', etc.)
        trend: Price trend ('up', 'down', 'sideways', 'volatile')
        volatility: Daily volatility as decimal
        start_price: Starting price
        start_date: Starting date (defaults to days ago from today)
        
    Returns:
        DataFrame with OHLCV bars
    """
    if start_date is None:
        start_date = datetime.now() - timedelta(days=days)
    
    # Determine datetime frequency
    if interval == '1d':
        freq = 'D'
    elif interval == '1h':
        freq = 'H'
        days = days * 24  # Convert to hours
    elif interval == '5m':
        freq = '5min'
        days = days * 24 * 12  # Convert to 5-minute intervals
    else:
        freq = 'D'  # Default
    
    # Generate date range
    dates = pd.date_range(start=start_date, periods=days, freq=freq)
    
    # Generate prices based on trend
    prices = []
    current_price = start_price
    
    for i in range(days):
        if trend == 'up':
            drift = 0.001  # Small daily upward drift
        elif trend == 'down':
            drift = -0.001  # Small daily downward drift
        elif trend == 'sideways':
            drift = 0  # No drift
        elif trend == 'volatile':
            drift = 0.002 if np.random.random() > 0.5 else -0.002  # Random drift
        
        # Add random noise
        daily_return = drift + np.random.normal(0, volatility)
        current_price *= (1 + daily_return)
        
        # Calculate OHLC values with some intraday variation
        daily_volatility = current_price * volatility * 0.5
        open_price = current_price - daily_volatility * (np.random.random() - 0.5)
        high_price = max(open_price, current_price) + daily_volatility * np.random.random()
        low_price = min(open_price, current_price) - daily_volatility * np.random.random()
        close_price = current_price
        volume = int(1000000 * (1 + np.random.random()))
        
        prices.append([open_price, high_price, low_price, close_price, volume])
    
    # Create DataFrame
    df = pd.DataFrame(prices, index=dates, columns=['open', 'high', 'low', 'close', 'volume'])
    df['symbol'] = symbol
    
    return df

def create_mock_ibkr_client():
    """Create a mock IBKR client for testing"""
    from unittest.mock import MagicMock
    
    mock_client = MagicMock()
    mock_client.connected = True
    mock_client.get_next_req_id.return_value = 1
    
    # Mock account data
    mock_client.request_account_summary.return_value = 1
    mock_client.get_account_summary_result.return_value = [
        {'account': 'DU123456', 'tag': 'NetLiquidation', 'value': '100000', 'currency': 'USD'},
        {'account': 'DU123456', 'tag': 'AvailableFunds', 'value': '90000', 'currency': 'USD'},
        {'account': 'DU123456', 'tag': 'TotalCashValue', 'value': '80000', 'currency': 'USD'}
    ]
    
    # Mock order placement
    mock_client.place_market_order.return_value = 1001
    
    return mock_client

def create_test_database(db_path='sqlite:///test_data.db'):
    """Create a test database with initial data for testing"""
    import sqlalchemy as sa
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean
    
    engine = create_engine(db_path)
    metadata = MetaData()
    
    # Define tables
    price_data = Table(
        'price_data', metadata,
        Column('id', Integer, primary_key=True),
        Column('symbol', String(20), nullable=False),
        Column('timeframe', String(10), nullable=False),
        Column('timestamp', DateTime, nullable=False),
        Column('open', Float, nullable=False),
        Column('high', Float, nullable=False),
        Column('low', Float, nullable=False),
        Column('close', Float, nullable=False),
        Column('volume', Integer),
        Column('data_type', String(20), nullable=False),
        Column('source', String(20), default='TEST'))
        # tests/utils/test_utils.py (continued)
    # Create all tables
    metadata.create_all(engine)
    
    # Insert sample data
    with engine.connect() as conn:
        # Add some sample symbols
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
        timeframes = ['1 day', '1 hour', '5 mins']
        
        # Generate sample data for each symbol and timeframe
        for symbol in symbols:
            for timeframe in timeframes:
                # Generate appropriate test data based on timeframe
                if timeframe == '1 day':
                    days = 60
                elif timeframe == '1 hour':
                    days = 7
                else:  # 5 mins
                    days = 2
                
                # Create test data
                df = create_test_price_bars(
                    symbol=symbol,
                    days=days,
                    interval=timeframe.replace(' ', ''),
                    trend='up' if symbol in ['AAPL', 'GOOGL'] else 'down'
                )
                
                # Insert data
                for idx, row in df.iterrows():
                    conn.execute(
                        price_data.insert().values(
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=idx,
                            open=row['open'],
                            high=row['high'],
                            low=row['low'],
                            close=row['close'],
                            volume=row['volume'],
                            data_type='TRADES',
                            source='TEST'
                        )
                    )
    
    return engine