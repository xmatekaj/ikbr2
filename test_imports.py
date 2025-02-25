"""
Test script for imports in main.py
"""
import os
import sys
import time
import signal
import argparse
import logging
from logging.handlers import RotatingFileHandler
import threading

print("Starting import test")

# This is the problematic import pattern from main.py
try:
    print("Testing sys.path modification...")
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    print(f"Modified sys.path: {sys.path}")
except Exception as e:
    print(f"Error modifying sys.path: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\nTrying original imports...")
    print("Importing config.settings...")
    from src.config.settings import default_settings
    print("Successfully imported default_settings")
    
    print("Importing IBKRClient...")
    from src.connectors.ibkr.client import IBKRClient
    print("Successfully imported IBKRClient")
    
    print("Importing IBKRDataFeed...")
    from src.connectors.ibkr.data_feed import IBKRDataFeed
    print("Successfully imported IBKRDataFeed")
    
    print("Importing IBKROrderManager...")
    from src.connectors.ibkr.order_manager import IBKROrderManager
    print("Successfully imported IBKROrderManager")
    
    print("Importing BotManager...")
    # Comment out this line if BotManager is not yet fully implemented
    # from src.core.bot_manager import BotManager
    print("BotManager import commented out for test")
    
    print("\nAll imports successful!")
    
except Exception as e:
    print(f"Error importing modules: {e}")
    import traceback
    traceback.print_exc()

print("Test script completed")