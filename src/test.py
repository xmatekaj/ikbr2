"""
Simple debug script to check project imports
"""
print("Debug script starting...")

import os
import sys

print(f"Current directory: {os.getcwd()}")
print(f"Python executable: {sys.executable}")
print(f"Python version: {sys.version}")
print(f"Python path: {sys.path}")

try:
    print("\nTrying to import from src...")
    # Add the project root to path
    sys.path.append(os.path.dirname(os.getcwd()))
    
    print("Importing config...")
    from config.settings import default_settings
    print("Successfully imported settings!")
    
    print("\nTrying to import IBKR client...")
    from connectors.ibkr.client import IBKRClient
    print("Successfully imported IBKRClient!")
    
    # Create a small test client
    client = IBKRClient(host="127.0.0.1", port=7497, client_id=999)
    print(f"Created client: {client}")
    
except Exception as e:
    print(f"Import error: {e}")
    import traceback
    traceback.print_exc()

print("\nDebug script completed")