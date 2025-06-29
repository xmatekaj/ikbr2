"""
Database storage module for harvesting and storing historical market data.
"""
import logging
import os
import time
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional


import sqlalchemy as sa
import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, NUMERIC
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, Boolean, Text, create_engine, text



logger = logging.getLogger(__name__)

class DataHarvester:
    """Harvests and stores historical market data from IBKR."""
    
    def __init__(self, data_feed, db_path="postgresql://username:password@localhost:5432/market_data", pool_size=5):
        """
        Initialize the data harvester with connection pooling.
        
        Args:
            db_path: Database connection string
            pool_size: Size of the connection pool
        """
        # Validate connection string to ensure it's TimescaleDB (PostgreSQL)
        if not db_path.startswith('postgresql://'):
            raise ValueError("TimescaleDB requires a PostgreSQL connection string (postgresql://)")
        
        self.engine = create_engine(
            db_path,
            pool_size=pool_size,          # Maximum number of connections
            pool_timeout=30,              # Seconds to wait before timeout
            pool_recycle=3600,            # Recycle connections after 1 hour
            max_overflow=10               # Allow extra connections
        )
        
        self.metadata = MetaData()
        
        # Create tables if they don't exist
        self._create_tables()
        
        # Initialize lock for thread safety
        self._lock = threading.RLock()
        
        logger.info(f"Initialized database connection to {db_path} with pool size {pool_size}")
    
    def _create_tables(self):
        """Create database tables with optimized structure for market data."""
        
        # Price data table - for OHLCV data
        self.price_data = Table(
            'price_data', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('symbol', String(20), nullable=False),
            Column('timeframe', String(10), nullable=False),
            Column('timestamp', TIMESTAMP(timezone=True), nullable=False),
            Column('open', NUMERIC(precision=19, scale=6), nullable=False),
            Column('high', NUMERIC(precision=19, scale=6), nullable=False),
            Column('low', NUMERIC(precision=19, scale=6), nullable=False),
            Column('close', NUMERIC(precision=19, scale=6), nullable=False),
            Column('volume', NUMERIC(precision=25, scale=6)),
            Column('data_type', String(20), nullable=False),
            Column('source', String(20), default='IBKR'),
            Column('created_at', TIMESTAMP(timezone=True), default=datetime.now),
            Column('updated_at', TIMESTAMP(timezone=True), onupdate=datetime.now),
            Column('is_adjusted', Boolean, default=False),
            Column('data_quality', Float, default=1.0),
            
            # Unique constraint to prevent duplicates
            sa.UniqueConstraint('symbol', 'timeframe', 'timestamp', 'data_type', name='uix_price_data'),
            
            # Compound index for fast lookups
            sa.Index('idx_price_data_lookup', 'symbol', 'timeframe', 'timestamp'),
            # Date range index for time-based queries
            sa.Index('idx_price_data_time_range', 'symbol', 'timeframe', 'timestamp')
        )
        
        # Symbols metadata table
        self.symbols_meta = Table(
            'symbols_meta', self.metadata,
            Column('symbol', String(20), primary_key=True),
            Column('name', String(100)),
            Column('type', String(20)),  # stock, forex, crypto, etc.
            Column('exchange', String(20)),
            Column('currency', String(10)),
            Column('first_date', TIMESTAMP(timezone=True)),  # First data point available
            Column('last_update', TIMESTAMP(timezone=True)),  # Last updated
            Column('update_count', Integer, default=0),  # Count of updates
            Column('metadata', JSONB)  # Additional metadata as JSON
        )
        
        # Data collection log
        self.harvest_log = Table(
            'harvest_log', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('symbol', String(20), nullable=False),
            Column('timeframe', String(10), nullable=False),
            Column('data_type', String(20), nullable=False),
            Column('start_time', TIMESTAMP(timezone=True)),
            Column('end_time', TIMESTAMP(timezone=True)),
            Column('records_processed', Integer),
            Column('records_added', Integer),
            Column('records_updated', Integer),
            Column('status', String(20)),  # success, partial, failed
            Column('error', Text),
            Column('created_at', TIMESTAMP(timezone=True), default=datetime.now)
        )
        
        # Create all tables
        self.metadata.create_all(self.engine)

        with self.engine.connect() as conn:
            try:
                # Create TimescaleDB extension
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                
                # Convert price_data to hypertable 
                conn.execute(text(
                    "SELECT create_hypertable('price_data', 'timestamp', if_not_exists => TRUE);"
                ))
                
                # Add compression policy
                conn.execute(text(
                    "SELECT add_compression_policy('price_data', INTERVAL '7 days', if_not_exists => TRUE);"
                ))
                
                # Add retention policy (adjust as needed for data retention requirements)
                conn.execute(text(
                    "SELECT add_retention_policy('price_data', INTERVAL '5 years', if_not_exists => TRUE);"
                ))
                
                logger.info("TimescaleDB extension configured successfully")
            except Exception as e:
                logger.error(f"Error setting up TimescaleDB: {e}")
                raise ValueError("TimescaleDB setup failed. Make sure TimescaleDB is properly installed")

        logger.info("Database tables created or verified")

    def harvest_data(self, 
                symbol: str, 
                duration: str, 
                bar_size: str,
                what_to_show: str = "TRADES"):
        """
        Harvest and store historical data for a symbol.
        
        Args:
            symbol: The stock symbol
            duration: Time period (e.g., "1 D", "1 Y")
            bar_size: Bar size (e.g., "1 min", "1 day")
            what_to_show: Type of data (e.g., "TRADES", "MIDPOINT")
            
        Returns:
            Dict with statistics about the operation
        """
        logger.info(f"Harvesting {bar_size} data for {symbol} ({duration}, {what_to_show})")
        
        try:
            # Request data from IBKR
            req_id = self.data_feed.request_historical_data(
                symbol=symbol,
                duration=duration,
                bar_size=bar_size,
                what_to_show=what_to_show
            )
            
            # Wait for the data to be received
            bars = self.data_feed.get_historical_data(req_id)
            
            if not bars:
                logger.warning(f"No data received for {symbol}")
                return {"status": "no_data", "message": "No data received"}
            
            # Store the data
            stats = self._store_bars(symbol, bar_size, what_to_show, bars)
            logger.info(f"Processed {stats['processed']} bars for {symbol}: "
                        f"{stats['added']} added, {stats['updated']} updated")
            return stats
            
        except Exception as e:
            logger.error(f"Error harvesting data for {symbol}: {e}", exc_info=True)
            return {"status": "failed", "error": str(e)}

    def _store_bars(self, symbol, bar_size, data_type, bars):
        """
        Store market data bars with robust error handling and duplicate prevention.
        
        Args:
            symbol: The market symbol
            bar_size: Timeframe of the data
            data_type: Type of data (TRADES, MIDPOINT, etc.)
            bars: List of bar data from IBKR
            
        Returns:
            Dict with statistics about the operation
        """
        # Acquire lock for thread safety
        with self._lock:
            if not bars:
                return {"processed": 0, "added": 0, "updated": 0, "errors": 0, "status": "empty"}
            
            # Statistics to track
            stats = {
                "processed": 0,
                "added": 0, 
                "updated": 0,
                "errors": 0,
                "status": "success"
            }
            
            # Start harvest log entry
            log_entry = {
                "symbol": symbol,
                "timeframe": bar_size,
                "data_type": data_type,
                "start_time": datetime.now(),
                "records_processed": 0,
                "records_added": 0,
                "records_updated": 0,
                "status": "in_progress"
            }
            
            # Connect and start transaction
            conn = self.engine.connect()
            transaction = conn.begin()
            
            try:
                # Insert harvest log
                log_result = conn.execute(self.harvest_log.insert().values(**log_entry))
                log_id = log_result.inserted_primary_key[0]
                
                # Update symbol metadata - First attempt to insert
                try:
                    conn.execute(
                        self.symbols_meta.insert().values(
                            symbol=symbol,
                            type=self._determine_symbol_type(symbol),
                            last_update=datetime.now(),
                            update_count=1
                        )
                    )
                except:
                    # Symbol already exists, update it
                    conn.execute(
                        self.symbols_meta.update()
                        .where(self.symbols_meta.c.symbol == symbol)
                        .values(
                            last_update=datetime.now(),
                            update_count=self.symbols_meta.c.update_count + 1
                        )
                    )
                
                # Process each bar
                for bar in bars:
                    stats["processed"] += 1
                    
                    try:
                        # Parse timestamp
                        timestamp = self._parse_bar_timestamp(bar)
                        
                        # Prepare values for the bar
                        bar_values = {
                            "symbol": symbol,
                            "timeframe": bar_size,
                            "timestamp": timestamp,
                            "open": float(bar['open']),
                            "high": float(bar['high']),
                            "low": float(bar['low']),
                            "close": float(bar['close']),
                            "volume": float(bar.get('volume', 0)),
                            "data_type": data_type,
                            "source": "IBKR",
                            "created_at": datetime.now(),
                            "data_quality": self._calculate_data_quality(bar)
                        }
                        
                        # Try to insert first (optimistic approach)
                        try:
                            conn.execute(self.price_data.insert().values(**bar_values))
                            stats["added"] += 1
                        except sa.exc.IntegrityError:
                            # Record already exists, update it instead
                            # Exclude fields that shouldn't change
                            update_values = {k: v for k, v in bar_values.items() 
                                            if k not in ('symbol', 'timeframe', 'timestamp', 'data_type', 'source', 'created_at')}
                            update_values["updated_at"] = datetime.now()
                            
                            conn.execute(
                                self.price_data.update()
                                .where(
                                    sa.and_(
                                        self.price_data.c.symbol == symbol,
                                        self.price_data.c.timeframe == bar_size,
                                        self.price_data.c.timestamp == timestamp,
                                        self.price_data.c.data_type == data_type
                                    )
                                )
                                .values(**update_values)
                            )
                            stats["updated"] += 1
                            
                    except Exception as e:
                        # Log error but continue processing other bars
                        logger.error(f"Error processing bar for {symbol}: {e}")
                        stats["errors"] += 1
                
                # Update first_date in symbols_meta if needed
                try:
                    # Get the oldest timestamp in the batch
                    oldest_timestamp = min([self._parse_bar_timestamp(bar) for bar in bars])
                    
                    # Update if this is older than what's stored
                    conn.execute(
                        text("""
                            UPDATE symbols_meta
                            SET first_date = :new_date
                            WHERE symbol = :symbol
                            AND (first_date IS NULL OR first_date > :new_date)
                        """),
                        {"symbol": symbol, "new_date": oldest_timestamp}
                    )
                except Exception as e:
                    logger.warning(f"Couldn't update first_date for {symbol}: {e}")
                
                # Update harvest log
                conn.execute(
                    self.harvest_log.update()
                    .where(self.harvest_log.c.id == log_id)
                    .values(
                        end_time=datetime.now(),
                        records_processed=stats["processed"],
                        records_added=stats["added"],
                        records_updated=stats["updated"],
                        status="success" if stats["errors"] == 0 else "partial",
                        error=f"{stats['errors']} errors encountered" if stats["errors"] > 0 else None
                    )
                )
                
                # Commit transaction
                transaction.commit()
                return stats
                
            except Exception as e:
                # Transaction failed, roll back
                transaction.rollback()
                
                error_msg = f"Database error storing bars for {symbol}: {str(e)}"
                logger.error(error_msg)
                
                # Update harvest log with failure
                try:
                    conn.execute(
                        self.harvest_log.update()
                        .where(self.harvest_log.c.id == log_id)
                        .values(
                            end_time=datetime.now(),
                            status="failed",
                            error=error_msg
                        )
                    )
                    # Need a new transaction since we rolled back
                    conn.execute(text("COMMIT"))
                except:
                    logger.error("Failed to update harvest log with error")
                
                stats["status"] = "failed"
                return stats
                
            finally:
                # Always close the connection
                conn.close()
    
    def _parse_bar_timestamp(self, bar):
        """Parse timestamp from bar data with robust error handling."""
        try:
            if isinstance(bar['date'], datetime):
                return bar['date']
            
            # Handle different date formats
            if isinstance(bar['date'], str):
                # Strip timezone information if present for consistent handling
                date_str = bar['date']
                timezone_parts = ["US/Eastern", "America/New_York", "US/Central", "US/Pacific"]
                
                for tz in timezone_parts:
                    if tz in date_str:
                        date_str = date_str.split(tz)[0].strip()
                        break
                
                # YYYYMMDD format
                if len(date_str) == 8 and date_str.isdigit():
                    return datetime.strptime(date_str, '%Y%m%d')
                
                # YYYYMMDD HH:MM:SS format
                if ' ' in date_str and len(date_str) >= 17:
                    return datetime.strptime(date_str, '%Y%m%d %H:%M:%S')
                
                # ISO format
                if 'T' in date_str or '-' in date_str:
                    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            
            # Unix timestamp (either seconds or milliseconds)
            if isinstance(bar['date'], (int, float)):
                if bar['date'] > 1e10:  # Milliseconds
                    return datetime.fromtimestamp(bar['date'] / 1000)
                else:  # Seconds
                    return datetime.fromtimestamp(bar['date'])
                    
            raise ValueError(f"Unknown date format: {bar['date']}")
            
        except Exception as e:
            logger.error(f"Error parsing timestamp: {e}, using current time instead")
            return datetime.now()

    def _calculate_data_quality(self, bar):
        """Calculate data quality score (0-1) based on various factors."""
        quality = 1.0
        
        # Check for missing values
        if None in (bar.get('open'), bar.get('high'), bar.get('low'), bar.get('close')):
            quality -= 0.3
        
        # Check for zero values where inappropriate
        if bar.get('high', 0) <= 0 or bar.get('low', 0) <= 0:
            quality -= 0.3
        
        # Check for logical inconsistencies
        if bar.get('high', 0) < bar.get('low', 0):
            quality -= 0.5
        
        # Check if high is the highest value
        if bar.get('high', 0) < max(bar.get('open', 0), bar.get('close', 0)):
            quality -= 0.2
        
        # Check if low is the lowest value
        if bar.get('low', 0) > min(bar.get('open', 0), bar.get('close', 0)):
            quality -= 0.2
        
        return max(0.0, quality)

    def _determine_symbol_type(self, symbol):
        """Determine the likely type of the symbol based on its format."""
        if '.' in symbol:  # Likely stock with exchange suffix
            return 'stock'
        if len(symbol) == 6 and symbol[3] == '/':  # Format like 'EUR/USD'
            return 'forex'
        if symbol.startswith('BTC') or symbol.startswith('ETH'):
            return 'crypto'
        return 'stock'  # Default

    
    def get_data(self, 
           symbol: str, 
           timeframe: str, 
           start_date: datetime = None,
           end_date: datetime = None,
           as_dataframe: bool = True) -> Any:
        """
        Retrieve stored data from the database.
        
        Args:
            symbol: The stock symbol
            timeframe: Bar size (e.g., "1 min", "1 day")
            start_date: Start date for the data
            end_date: End date for the data
            as_dataframe: Return as pandas DataFrame if True, else as list of dicts
            
        Returns:
            Retrieved data
        """
        query = sa.select([self.price_data]).where(
            sa.and_(
                self.price_data.c.symbol == symbol,
                self.price_data.c.timeframe == timeframe
            )
        )
        
        if start_date:
            query = query.where(self.price_data.c.timestamp >= start_date)
        if end_date:
            query = query.where(self.price_data.c.timestamp <= end_date)
        
        # Order by timestamp
        query = query.order_by(self.price_data.c.timestamp)
        
        # Execute query
        with self.engine.connect() as conn:
            result = conn.execute(query)
            data = [dict(row) for row in result]
        
        if as_dataframe and data:
            return pd.DataFrame(data)
        
        return data

    def get_symbols(self):
        """Get all symbols in the database."""
        query = sa.select([self.symbols_meta])
        
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row) for row in result]

    def get_timeframes(self, symbol=None):
        """Get all timeframes in the database, optionally filtered by symbol."""
        query = sa.select([
            self.price_data.c.timeframe,
            sa.func.count().label('count')
        ])
        
        if symbol:
            query = query.where(self.price_data.c.symbol == symbol)
        
        query = query.group_by(self.price_data.c.timeframe)
        
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row) for row in result]
    
    def harvest_multiple_symbols(self, 
                          symbols: List[str], 
                          timeframes: List[Dict],
                          max_parallel: int = 1):
        """
        Harvest data for multiple symbols.
        
        Args:
            symbols: List of symbols to harvest
            timeframes: List of dictionaries with timeframe configurations
                        e.g., [{"duration": "1 Y", "bar_size": "1 day"}]
            max_parallel: Maximum number of parallel harvesting operations
            
        Returns:
            Dict with harvesting results
        """
        # For parallel execution
        if max_parallel > 1:
            import concurrent.futures
            
            results = {
                "total_symbols": len(symbols),
                "successful": 0,
                "failed": 0,
                "details": {}
            }
            
            def harvest_single_symbol(symbol):
                """Harvest all timeframes for a single symbol."""
                symbol_results = {}
                success = True
                
                for tf in timeframes:
                    try:
                        tf_result = self.harvest_data(
                            symbol=symbol,
                            duration=tf["duration"],
                            bar_size=tf["bar_size"],
                            what_to_show=tf.get("what_to_show", "TRADES")
                        )
                        
                        symbol_results[f"{tf['bar_size']}_{tf.get('what_to_show', 'TRADES')}"] = tf_result
                        
                        if tf_result.get("status") == "failed":
                            success = False
                            
                        # Avoid rate limits
                        time.sleep(1)
                        
                    except Exception as e:
                        symbol_results[f"{tf['bar_size']}_{tf.get('what_to_show', 'TRADES')}"] = {
                            "status": "failed",
                            "error": str(e)
                        }
                        success = False
                
                return symbol, symbol_results, success
            
            # Use ThreadPoolExecutor for parallel execution
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
                futures = {executor.submit(harvest_single_symbol, symbol): symbol for symbol in symbols}
                
                for future in concurrent.futures.as_completed(futures):
                    symbol, symbol_results, success = future.result()
                    
                    results["details"][symbol] = symbol_results
                    if success:
                        results["successful"] += 1
                    else:
                        results["failed"] += 1
            
            return results
        
        # For sequential execution
        else:
            results = {
                "total_symbols": len(symbols),
                "successful": 0,
                "failed": 0,
                "details": {}
            }
            
            for symbol in symbols:
                symbol_results = {}
                success = True
                
                for tf in timeframes:
                    try:
                        tf_result = self.harvest_data(
                            symbol=symbol,
                            duration=tf["duration"],
                            bar_size=tf["bar_size"],
                            what_to_show=tf.get("what_to_show", "TRADES")
                        )
                        
                        symbol_results[f"{tf['bar_size']}_{tf.get('what_to_show', 'TRADES')}"] = tf_result
                        
                        if tf_result.get("status") == "failed":
                            success = False
                            
                        # Avoid rate limits
                        time.sleep(1)
                        
                    except Exception as e:
                        symbol_results[f"{tf['bar_size']}_{tf.get('what_to_show', 'TRADES')}"] = {
                            "status": "failed",
                            "error": str(e)
                        }
                        success = False
                
                results["details"][symbol] = symbol_results
                if success:
                    results["successful"] += 1
                else:
                    results["failed"] += 1
            
            return results

    def verify_data_integrity(self, symbol, timeframe, start_date=None, end_date=None):
        """
        Verify data integrity by checking for gaps and anomalies.
        
        Args:
            symbol: The symbol to check
            timeframe: Timeframe to check
            start_date: Start date for verification
            end_date: End date for verification
            
        Returns:
            Dict with verification results
        """
        logger.info(f"Verifying data integrity for {symbol} ({timeframe})")
        
        if start_date is None:
            # Get earliest data for this symbol/timeframe
            query = sa.select([sa.func.min(self.price_data.c.timestamp)])\
                    .where(
                        sa.and_(
                            self.price_data.c.symbol == symbol,
                            self.price_data.c.timeframe == timeframe
                        )
                    )
            with self.engine.connect() as conn:
                start_date = conn.execute(query).scalar() or datetime.now()
        
        if end_date is None:
            end_date = datetime.now()
        
        # Get data for verification
        query = sa.select([
                    self.price_data.c.timestamp, 
                    self.price_data.c.open,
                    self.price_data.c.high,
                    self.price_data.c.low,
                    self.price_data.c.close,
                    self.price_data.c.volume
                ])\
                .where(
                    sa.and_(
                        self.price_data.c.symbol == symbol,
                        self.price_data.c.timeframe == timeframe,
                        self.price_data.c.timestamp >= start_date,
                        self.price_data.c.timestamp <= end_date
                    )
                )\
                .order_by(self.price_data.c.timestamp)
        
        with self.engine.connect() as conn:
            result = conn.execute(query)
            data = [dict(row) for row in result]
        
        if not data:
            return {
                "status": "no_data",
                "message": f"No data found for {symbol} ({timeframe})"
            }
        
        # Initialize verification results
        verification = {
            "total_bars": len(data),
            "timeframe_seconds": self._timeframe_to_seconds(timeframe),
            "gaps": [],
            "anomalies": [],
            "first_date": data[0]['timestamp'],
            "last_date": data[-1]['timestamp'],
            "status": "verified"
        }
        
        # Check for gaps in data
        expected_interval = verification["timeframe_seconds"]
        
        for i in range(1, len(data)):
            curr_time = data[i]['timestamp']
            prev_time = data[i-1]['timestamp']
            
            # Calculate difference in seconds
            diff = (curr_time - prev_time).total_seconds()
            
            # Allow a small tolerance for differences
            tolerance = expected_interval * 0.05  # 5% tolerance
            
            # If difference is significantly more than expected, it's a gap
            if diff > (expected_interval + tolerance) * 1.5:
                expected_bars = int(diff / expected_interval) - 1
                gap = {
                    "start": prev_time,
                    "end": curr_time,
                    "missing_bars": expected_bars,
                    "duration_seconds": diff
                }
                verification["gaps"].append(gap)
        
        # Check for price anomalies (simplified version)
        for i in range(len(data)):
            bar = data[i]
            
            # Check for logically inconsistent prices
            if bar['high'] < bar['low']:
                verification["anomalies"].append({
                    "timestamp": bar['timestamp'],
                    "type": "high_below_low",
                    "details": f"High ({bar['high']}) is below low ({bar['low']})"
                })
        
        # Update verification status
        if verification["gaps"]:
            verification["status"] = "gaps_found"
        
        if verification["anomalies"]:
            verification["status"] = "anomalies_found"
        
        if verification["gaps"] and verification["anomalies"]:
            verification["status"] = "issues_found"
        
        return verification

    def _timeframe_to_seconds(self, timeframe):
        """Convert timeframe string to seconds."""
        # Common timeframes: 1 min, 5 mins, 1 hour, 1 day, etc.
        timeframe = timeframe.lower()
        
        if 'sec' in timeframe:
            return int(timeframe.split()[0])
        
        if 'min' in timeframe:
            return int(timeframe.split()[0]) * 60
        
        if 'hour' in timeframe:
            return int(timeframe.split()[0]) * 3600
        
        if 'day' in timeframe:
            return int(timeframe.split()[0]) * 86400
        
        if 'week' in timeframe:
            return int(timeframe.split()[0]) * 604800
        
        # Default to daily
        return 86400
    
    def get_database_stats(self):
        """
        Get statistics about the database.
        
        Returns:
            Dict with database statistics
        """
        stats = {
            "total_symbols": 0,
            "total_records": 0,
            "timeframes": {},
            "oldest_record": None,
            "newest_record": None,
            "top_symbols": [],
            "harvest_log_stats": {
                "total_harvests": 0,
                "successful": 0,
                "partial": 0,
                "failed": 0,
                "last_harvest": None
            }
        }
        
        with self.engine.connect() as conn:
            # Count symbols
            stats["total_symbols"] = conn.execute(
                sa.select([sa.func.count()]).select_from(self.symbols_meta)
            ).scalar() or 0
            
            # Count records
            stats["total_records"] = conn.execute(
                sa.select([sa.func.count()]).select_from(self.price_data)
            ).scalar() or 0
            
            # Get timeframe distribution
            timeframe_query = sa.select([
                self.price_data.c.timeframe,
                sa.func.count().label('count')
            ]).group_by(self.price_data.c.timeframe)
            
            for row in conn.execute(timeframe_query):
                stats["timeframes"][row['timeframe']] = row['count']
            
            # Get date range (if there's data)
            if stats["total_records"] > 0:
                date_range_query = sa.select([
                    sa.func.min(self.price_data.c.timestamp).label('oldest'),
                    sa.func.max(self.price_data.c.timestamp).label('newest')
                ])
                
                date_range = conn.execute(date_range_query).fetchone()
                stats["oldest_record"] = date_range['oldest']
                stats["newest_record"] = date_range['newest']
            
            # Get harvest log statistics
            stats["harvest_log_stats"]["total_harvests"] = conn.execute(
                sa.select([sa.func.count()]).select_from(self.harvest_log)
            ).scalar() or 0
            
            # Get statistics by status
            for status in ['success', 'partial', 'failed']:
                stats["harvest_log_stats"][status] = conn.execute(
                    sa.select([sa.func.count()])
                    .select_from(self.harvest_log)
                    .where(self.harvest_log.c.status == status)
                ).scalar() or 0
        
        return stats

    def optimize_database(self):
        """
        Perform TimescaleDB optimization operations.
        
        Returns:
            Dict with optimization results
        """
        results = {
            "optimizations": [],
            "status": "success"
        }
        
        with self.engine.connect() as conn:
            try:
                # TimescaleDB optimizations
                # Analyze tables
                conn.execute(text("ANALYZE"))
                results["optimizations"].append({"type": "analyze", "status": "success"})
                
                # Vacuum tables (reclaim space)
                conn.execute(text("VACUUM FULL"))
                results["optimizations"].append({"type": "vacuum", "status": "success"})
                
                # Reindex
                conn.execute(text("REINDEX DATABASE current_database()"))
                results["optimizations"].append({"type": "reindex", "status": "success"})
                
                # Clean up old harvest logs (keep only last 1000)
                try:
                    # Get the ID threshold for deletion
                    threshold_query = sa.text("""
                        SELECT id FROM harvest_log
                        ORDER BY created_at DESC
                        LIMIT 1 OFFSET 1000
                    """)
                    threshold_result = conn.execute(threshold_query).fetchone()
                    
                    if threshold_result:
                        threshold_id = threshold_result[0]
                        
                        # Delete old logs
                        delete_query = self.harvest_log.delete().where(
                            self.harvest_log.c.id < threshold_id
                        )
                        delete_result = conn.execute(delete_query)
                        
                        results["optimizations"].append({
                            "type": "cleanup_logs",
                            "deleted_logs": delete_result.rowcount,
                            "status": "success"
                        })
                except Exception as e:
                    logger.warning(f"Failed to clean up old harvest logs: {e}")
                    results["optimizations"].append({
                        "type": "cleanup_logs",
                        "status": "failed",
                        "error": str(e)
                    })
                
            except Exception as e:
                logger.error(f"Database optimization error: {e}")
                results["status"] = "failed"
                results["error"] = str(e)
        
        return results

    def backup_database(self, backup_path=None):
        """
        Create a backup of the TimescaleDB database.
        
        Args:
            backup_path: Path for the backup file (optional)
            
        Returns:
            Dict with backup results
        """
        import subprocess
        from urllib.parse import urlparse
        import os
        
        # Generate backup path if not provided
        if backup_path is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            backup_dir = "backups"
            os.makedirs(backup_dir, exist_ok=True)
            backup_path = os.path.join(backup_dir, f"db_backup_{timestamp}.sql")
        
        try:
            # For PostgreSQL/TimescaleDB, use pg_dump
            parsed_url = urlparse(str(self.engine.url))
            db_name = parsed_url.path.lstrip('/')
            username = parsed_url.username
            password = parsed_url.password
            host = parsed_url.hostname
            port = parsed_url.port or '5432'
            
            # Set environment for password
            env = os.environ.copy()
            if password:
                env['PGPASSWORD'] = password
            
            # Run pg_dump
            cmd = [
                'pg_dump',
                '-h', host,
                '-p', str(port),
                '-U', username,
                '-F', 'c',  # Custom format (compressed)
                '-f', backup_path,
                db_name
            ]
            
            process = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if process.returncode != 0:
                return {
                    "status": "failed",
                    "error": process.stderr.strip()
                }
            
            return {
                "status": "success",
                "backup_path": backup_path,
                "size_bytes": os.path.getsize(backup_path),
                "timestamp": datetime.now()
            }
        except Exception as e:
            logger.error(f"Database backup error: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }
        
    def export_to_csv(self, symbol, timeframe, filepath=None, start_date=None, end_date=None):
        """
        Export market data to CSV file for backtesting.
        
        Args:
            symbol: Symbol to export
            timeframe: Timeframe to export
            filepath: Output file path (optional)
            start_date: Start date (optional)
            end_date: End date (optional)
            
        Returns:
            Dict with export results
        """
        import pandas as pd
        import os
        
        # Default filepath if not provided
        if filepath is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{symbol}_{timeframe}_{timestamp}.csv"
            filepath = os.path.join("exports", filename)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        try:
            # Get data 
            df = self.get_data(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
                as_dataframe=True
            )
            
            if df is None or len(df) == 0:
                return {
                    "status": "error",
                    "error": "No data found"
                }
            
            # Reorder columns for backtesting compatibility
            cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df_export = df[cols].copy()
            
            # Export to CSV
            df_export.to_csv(filepath, index=False)
            
            return {
                "status": "success",
                "filepath": filepath,
                "rows": len(df_export),
                "symbol": symbol,
                "timeframe": timeframe,
                "start_date": df_export['timestamp'].min(),
                "end_date": df_export['timestamp'].max()
            }
            
        except Exception as e:
            logger.error(f"Export error: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }

    def import_from_csv(self, filepath, symbol=None, timeframe=None):
        """
        Import data from CSV file into the database.
        
        Args:
            filepath: Path to CSV file
            symbol: Symbol override (if not in filename)
            timeframe: Timeframe override (if not in filename)
            
        Returns:
            Dict with import results
        """
        import pandas as pd
        import os
        
        try:
            # Extract symbol and timeframe from filename if not provided
            if symbol is None or timeframe is None:
                filename = os.path.basename(filepath)
                name_parts = os.path.splitext(filename)[0].split('_')
                
                if len(name_parts) >= 2 and symbol is None:
                    symbol = name_parts[0]
                
                if len(name_parts) >= 2 and timeframe is None:
                    timeframe = name_parts[1]
            
            if symbol is None or timeframe is None:
                return {
                    "status": "failed",
                    "error": "Symbol and timeframe must be provided"
                }
            
            # Read CSV
            df = pd.read_csv(filepath)
            
            # Validate required columns
            required_cols = ['timestamp', 'open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                return {
                    "status": "failed",
                    "error": f"Missing required columns: {missing_cols}"
                }
            
            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Convert to list of bars
            bars = []
            for _, row in df.iterrows():
                bar = {
                    'date': row['timestamp'], 
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row.get('volume', 0)
                }
                bars.append(bar)
            
            # Store in database
            result = self._store_bars(symbol, timeframe, "IMPORTED", bars)
            
            return {
                "status": "success" if result["status"] == "success" else "partial",
                "processed": result["processed"],
                "added": result["added"],
                "updated": result["updated"],
                "errors": result["errors"],
                "symbol": symbol,
                "timeframe": timeframe
            }
            
        except Exception as e:
            logger.error(f"Import error: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }
            
    def __repr__(self):
        """String representation of the DataHarvester."""
        stats = self.get_database_stats()
        return f"DataHarvester(symbols={stats['total_symbols']}, records={stats['total_records']})"