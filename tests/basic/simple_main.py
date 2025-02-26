"""
Simplified main script for IBKR Trading Bot
"""
import argparse
import logging
import os
import time
from src.connectors.ibkr.client import IBKRClient

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("SimpleMain")

def parse_arguments():
    """Parse command-line arguments."""
    print("Parsing arguments...")
    parser = argparse.ArgumentParser(description='Simple IBKR Trading Bot')
    
    parser.add_argument('-t', '--test-connection', 
                        action='store_true',
                        help='Test the connection to IBKR and exit')
                        
    parser.add_argument('-v', '--verbose', 
                        action='store_true',
                        help='Enable verbose logging')
    
    args = parser.parse_args()
    print(f"Arguments parsed: {args}")
    return args

def test_connection():
    """Test connection to IBKR."""
    print("Testing connection to IBKR...")
    
    # Create a client
    client = IBKRClient(host="127.0.0.1", port=7497, client_id=1)
    
    try:
        print("Connecting to IBKR...")
        client.connect_and_run()
        
        time.sleep(3)  # Wait for connection to establish
        
        if client.connected:
            print("Successfully connected to IBKR!")
            
            # Get account information
            print("Requesting account information...")
            req_id = client.request_account_summary()
            
            time.sleep(3)  # Wait for data
            
            account_summary = client.get_account_summary_result(req_id)
            print("\nAccount Summary:")
            for item in account_summary:
                print(f"{item['tag']}: {item['value']} {item['currency']}")
        else:
            print("Failed to connect to IBKR")
        
        # Keep the program running briefly to receive any async responses
        print("\nWaiting for any additional data...")
        time.sleep(5)
        
    except Exception as e:
        print(f"Error during connection test: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Always disconnect when done
        print("Disconnecting from IBKR...")
        client.disconnect_and_stop()
        print("Test complete")

def main():
    """Main entry point."""
    print("Starting simple main function...")
    args = parse_arguments()
    
    # Set log level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        print("Verbose logging enabled")
    
    if args.test_connection:
        test_connection()
    else:
        print("No specific operation requested. Use --test-connection to test IBKR connectivity.")
    
    print("Simple main function completed")

if __name__ == "__main__":
    print("Script starting execution")
    try:
        main()
        print("Script completed successfully")
    except Exception as e:
        print(f"Unhandled exception in main: {e}")
        import traceback
        traceback.print_exc()
    print("Script execution ended")