"""
System logger configuration for the IKBR Trader Bot.

This module provides functions to set up and configure the system-wide logging.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
import datetime
from pathlib import Path


def setup_logger(name='ikbr_trader',
                log_level=logging.INFO,
                log_file=None,
                console=True,
                log_format=None,
                max_file_size_mb=10,
                backup_count=5):
    """
    Set up a logger with file and/or console handlers.
    
    Args:
        name: Logger name
        log_level: Logging level (e.g., logging.INFO, logging.DEBUG)
        log_file: Path to log file (if None, file logging is disabled)
        console: Whether to log to console
        log_format: Custom log format (if None, default format is used)
        max_file_size_mb: Maximum size of log file in MB before rotation
        backup_count: Number of backup files to keep
        
    Returns:
        A configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers = []
    
    # Define default log format if not provided
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    formatter = logging.Formatter(log_format)
    
    # Add file handler if log_file is specified
    if log_file:
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        
        # Set up rotating file handler
        max_bytes = max_file_size_mb * 1024 * 1024  # Convert MB to bytes
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Add console handler if console is True
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger


def setup_system_logger(log_dir='logs/system', log_level=logging.INFO):
    """
    Set up the system-wide logger.
    
    Args:
        log_dir: Directory for system log files
        log_level: Logging level
        
    Returns:
        A configured logger instance for system logs
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log file name with timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'system_{timestamp}.log')
    
    # Set up logger
    logger = setup_logger(
        name='ikbr_trader.system',
        log_level=log_level,
        log_file=log_file,
        console=True,
        log_format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
    )
    
    logger.info("System logger initialized")
    return logger


def setup_trade_logger(log_dir='logs/trades', log_level=logging.INFO):
    """
    Set up a logger specifically for trade-related logs.
    
    Args:
        log_dir: Directory for trade log files
        log_level: Logging level
        
    Returns:
        A configured logger instance for trade logs
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log file name with timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'trades_{timestamp}.log')
    
    # Set up logger
    logger = setup_logger(
        name='ikbr_trader.trades',
        log_level=log_level,
        log_file=log_file,
        console=True,
        log_format='%(asctime)s - %(levelname)s - [%(strategy)s] - %(message)s'
    )
    
    logger.info("Trade logger initialized")
    return logger


def get_logger(name):
    """
    Get a configured logger by name.
    
    Args:
        name: Logger name
        
    Returns:
        A logger instance
    """
    return logging.getLogger(name)


# Set up system logger on module import
system_logger = setup_system_logger()