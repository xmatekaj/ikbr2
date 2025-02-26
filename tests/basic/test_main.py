"""
Simplified test script for IBKR Trading Bot connection
"""
import sys
import time
import logging
from src.connectors.ibkr.client import IBKRClient

# Configure basic logging to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("TestMain")

def test_connection():
    """Test connection to IBKR."""
    print("Testing connection to IBKR...")
    logger.info("Creating IBKR client")
    
    # Create a client
    client = IBKRClient(
        host="127.0.0.1", 
        port=7497,  # Paper trading port
        client_id=999
    )
    
    try:
        print("Connecting to IBKR...")
        client.connect_and_run()
        
        time.sleep(2)  # Wait for connection to establish
        
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


if __name__ == "__main__":
    print("Starting test script")
    test_connection()
    print("Test script completed")