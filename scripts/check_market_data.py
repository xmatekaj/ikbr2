# scripts/check_market_data.py

"""
Script to check IBKR market data subscriptions.
"""
import argparse
import json
import logging
import sys
import os

# Add the project root directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Go up one level to reach project root
sys.path.insert(0, project_root)  # Add project root to Python path

# Now we can import from src
from src.utils.market_data_info import get_market_data_subscription_info

def main():
    """Main entry point for checking market data subscriptions."""
    # Parse arguments
    parser = argparse.ArgumentParser(description='Check IBKR Market Data Subscriptions')
    parser.add_argument('--host', default='127.0.0.1', help='IBKR host')
    parser.add_argument('--port', type=int, default=7497, help='IBKR port (7496 for TWS live, 7497 for paper)')
    parser.add_argument('--output', help='Output file for JSON results')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get market data info
    print(f"Connecting to IBKR at {args.host}:{args.port}...")
    result = get_market_data_subscription_info(host=args.host, port=args.port)
    
    # Display summary
    if "error" in result:
        print(f"Error: {result['error']}")
        return
    
    print("\n=== Market Data Subscription Summary ===")
    summary = result["summary"]
    print(f"Total categories checked: {summary['total_categories']}")
    print(f"Active subscriptions: {summary['active_subscriptions']}")
    print(f"Delayed subscriptions: {summary['delayed_subscriptions']}")
    print(f"Inactive subscriptions: {summary['inactive_subscriptions']}")
    print(f"Available tick types: {summary['total_tick_types']}")
    
    # Print subscription details
    print("\n=== Subscription Details ===")
    subscriptions = result["details"]["subscriptions"]
    
    for category, info in subscriptions.items():
        status = info["status"]
        data_type = info["data_type"]
        
        # Add status icon
        status_icon = "✓" if status == "active" else "⚠" if data_type == "delayed" else "✗"
        
        # Print with color if terminal supports it
        try:
            status_color = "\033[92m" if status == "active" else "\033[93m" if data_type == "delayed" else "\033[91m"
            reset_color = "\033[0m"
            print(f"{status_icon} {status_color}{category}{reset_color}: {info['description']} ({data_type})")
        except:
            # Fallback for terminals without color support
            print(f"{status_icon} {category}: {info['description']} ({data_type})")
        
        # Print tested symbols
        if info["tested_symbols"]:
            print(f"   Tested symbols: {', '.join(info['tested_symbols'])}")
    
    # Print error messages if any
    error_messages = result["details"].get("error_messages", [])
    if error_messages:
        print("\n=== Error Messages ===")
        for i, error in enumerate(error_messages[:5], 1):  # Show first 5 errors
            print(f"{i}. {error}")
        
        if len(error_messages) > 5:
            print(f"...and {len(error_messages) - 5} more errors")
    
    # Save to file if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nResults saved to {args.output}")

if __name__ == "__main__":
    main()