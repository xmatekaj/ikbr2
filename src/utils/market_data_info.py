# src/utils/market_data_info.py

"""
Utility module for retrieving market data information from IBKR.
This module checks available market data subscriptions based on symbol access.
"""

import logging
import threading
import time
from typing import Dict, List, Optional, Set, Tuple

from src.connectors.ibkr.client import IBKRClient
from src.connectors.ibkr.data_feed import IBKRDataFeed

logger = logging.getLogger(__name__)

class MarketDataInfoCollector:
    """
    Collects information about available market data from IBKR,
    including subscriptions, supported exchanges, and available symbols.
    """
    
    def __init__(self, host="127.0.0.1", port=7497, client_id=999):
        """
        Initialize the market data info collector.
        
        Args:
            host: IBKR host
            port: IBKR port
            client_id: Client ID for IBKR connection
        """
        self.client = IBKRClient(host=host, port=port, client_id=client_id)
        self.data_feed = None
        
        # Storage for market data info
        self.subscription_categories = self._define_subscription_categories()
        self.exchange_info = {}
        self.tick_types = {}
        self.error_messages = []
        
        # For tracking responses
        self._error_callbacks = {}
        
    def connect(self) -> bool:
        """
        Connect to IBKR.
        
        Returns:
            bool: True if connection successful
        """
        try:
            logger.info("Connecting to IBKR to collect market data information")
            self.client.connect_and_run()
            
            if not self.client.connected:
                logger.error("Failed to connect to IBKR")
                return False
                
            # Initialize data feed
            self.data_feed = IBKRDataFeed(
                host=self.client.host,
                port=self.client.port,
                client_id=self.client.client_id + 100
            )
            
            # Connect the data feed
            self.data_feed.connect_and_run()
            
            if not self.data_feed.connected:
                logger.error("Failed to connect data feed to IBKR")
                self.disconnect()
                return False
                
            logger.info("Successfully connected to IBKR for market data info collection")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to IBKR: {e}")
            self.disconnect()
            return False
    
    def disconnect(self):
        """Disconnect from IBKR."""
        logger.info("Disconnecting from IBKR")
        
        if self.data_feed:
            self.data_feed.disconnect_and_stop()
            
        if self.client:
            self.client.disconnect_and_stop()
    
    def _define_subscription_categories(self) -> Dict[str, Dict]:
        """
        Define categories of subscriptions to check.
        
        Returns:
            Dict: Categories with test symbols
        """
        return {
            "US Equities": {
                "description": "US stocks (NASDAQ, NYSE)",
                "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "SPY", "QQQ"],
                "exchanges": ["NASDAQ", "NYSE", "ISLAND", "ARCA"],
                "active": False
            },
            "US Real-Time Non Consolidated": {
                "description": "US Real-Time Non Consolidated Streaming Quotes (IBKR-PRO)",
                "symbols": ["AAPL", "MSFT", "AMZN"],
                "exchanges": ["BATS", "BYX", "EDGX", "EDGA", "IEX"],
                "active": False
            },
            "ICE Futures": {
                "description": "ICE Futures (Gold, Silver, Digital Assets)",
                "symbols": ["GC", "SI", "BTC"],
                "exchanges": ["ICE"],
                "active": False
            },
            "Cryptocurrency": {
                "description": "PAXOS Cryptocurrency",
                "symbols": ["BTC", "ETH", "LTC"],
                "exchanges": ["PAXOS"],
                "active": False
            },
            "Small Exchange": {
                "description": "The Small Exchange Securities",
                "symbols": ["S10Y", "S500", "S420"],
                "exchanges": ["SMALLS"],
                "active": False
            },
            "US Bonds": {
                "description": "US and EU Bond Quotes",
                "symbols": ["US10Y", "US30Y", "GOVT"],
                "exchanges": ["BOND"],
                "active": False
            },
            "Mutual Funds": {
                "description": "US Mutual Funds",
                "symbols": ["VFINX", "FXAIX", "VTSAX"],
                "exchanges": ["FUNDSERV"],
                "active": False
            },
            "European Equities": {
                "description": "European BATS/Chi-X Equities",
                "symbols": ["VOD.L", "SAP.DE", "BARC.L"],
                "exchanges": ["CHIX", "BATS.L"],
                "active": False
            },
            "Forex": {
                "description": "IDEALPRO FX",
                "symbols": ["EUR.USD", "GBP.USD", "USD.JPY"],
                "exchanges": ["IDEALPRO"],
                "active": False
            },
            "Index CFDs": {
                "description": "Index CFDs",
                "symbols": ["US500", "NAS100", "GER30"],
                "exchanges": ["IBCFD"],
                "active": False
            },
            "Commodities": {
                "description": "Physical Metals and Commodities",
                "symbols": ["XAUUSD", "XAGUSD", "CL"],
                "exchanges": ["COMMODITY"],
                "active": False
            }
        }
    
    def check_symbol_access(self, symbol: str, exchange: str = "SMART") -> Dict:
        """
        Check if a specific symbol is accessible with current subscriptions.
        
        Args:
            symbol: Symbol to check
            exchange: Exchange to use
            
        Returns:
            Dict: Result of the check
        """
        logger.info(f"Checking access to {symbol} on {exchange}")
        
        # Create contract
        from ibapi.contract import Contract
        contract = Contract()
        contract.symbol = symbol
        
        # Determine security type based on symbol characteristics
        if "." in symbol:
            if symbol.endswith(".L") or symbol.endswith(".DE"):
                contract.secType = "STK"
                contract.currency = "EUR" if symbol.endswith(".DE") else "GBP"
            else:
                parts = symbol.split(".")
                if len(parts) == 2 and len(parts[0]) == 3 and len(parts[1]) == 3:
                    # Forex pair like EUR.USD
                    contract.secType = "CASH"
                    contract.currency = parts[1]
                    symbol = parts[0]
                    contract.symbol = symbol
                else:
                    contract.secType = "STK"
                    contract.currency = "USD"
        elif symbol in ["GC", "SI", "CL"]:
            contract.secType = "FUT"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = "202406"  # Use a future date
        elif symbol in ["BTC", "ETH", "LTC"]:
            contract.secType = "CRYPTO"
            contract.currency = "USD"
        elif symbol.startswith("US") and symbol.endswith("Y"):
            contract.secType = "BOND"
            contract.currency = "USD"
        elif symbol in ["S10Y", "S500", "S420"]:
            contract.secType = "FUT"
            contract.currency = "USD"
        elif symbol in ["US500", "NAS100", "GER30"]:
            contract.secType = "CFD"
            contract.currency = "USD"
        elif symbol in ["XAUUSD", "XAGUSD"]:
            contract.secType = "CMDTY"
            contract.currency = "USD"
        else:
            contract.secType = "STK"
            contract.currency = "USD"
            
        contract.exchange = exchange
        
        # Request market data
        req_id = self.client.get_next_req_id()
        self._error_callbacks[req_id] = []
        
        # Store error handler
        original_error_handler = self.client.error
        
        # Override error handler to capture errors
        def capture_error_handler(reqId, errorCode, errorString):
            if reqId == req_id:
                self._error_callbacks[req_id].append({
                    "code": errorCode,
                    "message": errorString
                })
                self.error_messages.append(f"Symbol {symbol}: {errorString} (code: {errorCode})")
            # Call original handler
            original_error_handler(reqId, errorCode, errorString)
            
        # Set our capture handler
        self.client.error = capture_error_handler
        
        # Make the request
        try:
            self.client.reqMktData(req_id, contract, "", True, False, [])
            
            # Wait for response
            time.sleep(1.5)
            
            # Cancel request
            self.client.cancelMktData(req_id)
            
            # Check if we got any data
            has_data = False
            is_delayed = False
            errors = self._error_callbacks.get(req_id, [])
            
            # Check for subscription status in error messages
            for error in errors:
                msg = error.get("message", "")
                code = error.get("code")
                
                # Check for delayed data message
                if "Delayed market data is available" in msg:
                    is_delayed = True
                
                # Specific errors that indicate no subscription
                if code in [10, 200, 354, 10090]:
                    return {
                        "symbol": symbol,
                        "exchange": exchange,
                        "has_access": False,
                        "is_delayed": is_delayed,
                        "error": msg
                    }
            
            # If no blocking errors, check if we have price data
            if req_id in getattr(self.data_feed, 'market_data', {}):
                data = self.data_feed.market_data[req_id]
                price = data.get('last_price')
                
                if price is not None:
                    has_data = True
            
            return {
                "symbol": symbol,
                "exchange": exchange,
                "has_access": has_data,
                "is_delayed": is_delayed,
                "error": None if has_data else "No data received"
            }
            
        except Exception as e:
            logger.error(f"Error checking {symbol}: {e}")
            return {
                "symbol": symbol,
                "exchange": exchange,
                "has_access": False,
                "is_delayed": False,
                "error": str(e)
            }
        finally:
            # Restore original error handler
            self.client.error = original_error_handler
    
    def check_subscription_categories(self):
        """
        Check all defined subscription categories.
        """
        for category, info in self.subscription_categories.items():
            logger.info(f"Checking subscription category: {category}")
            
            success_count = 0
            delayed_count = 0
            
            # Check a subset of symbols for this category
            test_symbols = info["symbols"][:2]  # Test first 2 symbols only to save time
            
            for symbol in test_symbols:
                # Get exchange for this symbol
                exchange = info["exchanges"][0] if info["exchanges"] else "SMART"
                
                # Check symbol
                result = self.check_symbol_access(symbol, exchange)
                
                if result["has_access"]:
                    success_count += 1
                    
                if result["is_delayed"]:
                    delayed_count += 1
            
            # Mark category as active if at least one symbol was accessible
            self.subscription_categories[category]["active"] = success_count > 0
            self.subscription_categories[category]["delayed"] = delayed_count > 0
            
            # Store test results
            self.subscription_categories[category]["tested_symbols"] = test_symbols
            
    def get_available_tick_types(self):
        """
        Get a list of available tick types from IBKR.
        
        Returns:
            Dict: Mapping of tick types to descriptions
        """
        return {
            0: "BID_SIZE",
            1: "BID",
            2: "ASK",
            3: "ASK_SIZE",
            4: "LAST",
            5: "LAST_SIZE",
            6: "HIGH",
            7: "LOW",
            8: "VOLUME",
            9: "CLOSE",
            10: "BID_OPTION_COMPUTATION",
            11: "ASK_OPTION_COMPUTATION",
            12: "LAST_OPTION_COMPUTATION",
            13: "MODEL_OPTION",
            14: "OPEN",
            # More tick types
            15: "LAST_TIMESTAMP",
            16: "SHORTABLE",
            17: "FUNDAMENTAL_RATIOS",
            18: "REALTIME_VOLUME",
            19: "HALTED",
            20: "BID_YIELD",
            21: "ASK_YIELD",
            22: "LAST_YIELD",
            23: "REGULATORY_IMBALANCE",
            24: "NEWS_TICK",
            25: "TRADE_COUNT",
            26: "TRADE_RATE",
            27: "VOLUME_RATE",
            28: "LAST_RTH_TRADE",
            29: "RT_HISTORICAL_VOL",
            30: "IB_DIVIDENDS",
            31: "BOND_FACTOR_MULTIPLIER",
            32: "REGULATORY_SNAPSHOT",
            33: "DELAYED_BID",
            34: "DELAYED_ASK",
            35: "DELAYED_LAST",
            36: "DELAYED_BID_SIZE",
            37: "DELAYED_ASK_SIZE",
            38: "DELAYED_LAST_SIZE",
            39: "DELAYED_HIGH",
            40: "DELAYED_LOW",
            41: "DELAYED_VOLUME",
            42: "DELAYED_CLOSE",
            43: "DELAYED_OPEN",
            44: "RT_TRD_VOLUME",
            45: "CREDITMAN_MARK_PRICE",
            46: "CREDITMAN_SLOW_MARK_PRICE",
            47: "DELAYED_BID_OPTION",
            48: "DELAYED_ASK_OPTION",
            49: "DELAYED_LAST_OPTION",
            50: "DELAYED_MODEL_OPTION",
            51: "LAST_EXCH",
            52: "LAST_REG_TIME",
            53: "FUTURES_OPEN_INTEREST",
            54: "AVG_OPT_VOLUME",
            55: "DELAYED_LAST_TIMESTAMP",
            56: "SHORTABLE_SHARES",
            57: "DELAYED_HALTED",
            58: "REUTERS_2_MUTUAL_FUNDS",
            59: "ETF_NAV_CLOSE",
            60: "ETF_NAV_PRIOR_CLOSE",
            61: "ETF_NAV_BID",
            62: "ETF_NAV_ASK",
            63: "ETF_NAV_LAST",
            64: "ETF_FROZEN_NAV_LAST",
            65: "ETF_NAV_HIGH",
            66: "ETF_NAV_LOW",
            67: "SOCIAL_MARKET_ANALYTICS",
            68: "ESTIMATED_IPO_MIDPOINT",
            69: "FINAL_IPO_LAST",
            70: "DELAYED_YIELD_BID",
            71: "DELAYED_YIELD_ASK"
        }
    
    def collect_market_data_info(self):
        """
        Collect comprehensive market data information.
        
        Returns:
            Dict: Market data information
        """
        if not self.client.connected:
            if not self.connect():
                return {"error": "Failed to connect to IBKR"}
        
        try:
            # Clear error messages
            self.error_messages = []
            
            # Get available tick types
            self.tick_types = self.get_available_tick_types()
            
            # Check subscription categories
            self.check_subscription_categories()
            
            # Compile the results
            results = {
                "subscription_categories": self.subscription_categories,
                "tick_types": self.tick_types,
                "error_messages": self.error_messages,
            }
            
            return results
            
        except Exception as e:
            logger.error(f"Error collecting market data info: {e}")
            return {"error": str(e)}
        finally:
            # Clean up
            self.disconnect()


def get_market_data_subscription_info(host="127.0.0.1", port=7497):
    """
    Get information about the current market data subscription.
    
    Args:
        host: IBKR host
        port: IBKR port
        
    Returns:
        Dict: Market data subscription information
    """
    collector = MarketDataInfoCollector(host=host, port=port)
    
    try:
        info = collector.collect_market_data_info()
        
        if "error" in info:
            return info
        
        # Calculate subscription summary
        categories = info.get("subscription_categories", {})
        
        # Format the results for display
        result = {
            "summary": {
                "total_categories": len(categories),
                "active_subscriptions": sum(1 for c in categories.values() if c.get("active", False)),
                "delayed_subscriptions": sum(1 for c in categories.values() if c.get("delayed", False)),
                "inactive_subscriptions": sum(1 for c in categories.values() if not c.get("active", False)),
                "total_tick_types": len(info.get("tick_types", {})),
            },
            "details": {
                "subscriptions": {},
                "tick_types": info.get("tick_types", {}),
                "error_messages": info.get("error_messages", [])
            }
        }
        
        # Format subscription details
        for category, details in categories.items():
            status = "active" if details.get("active", False) else "inactive"
            data_type = "delayed" if details.get("delayed", False) else "real-time" if details.get("active", False) else "unavailable"
            
            result["details"]["subscriptions"][category] = {
                "description": details.get("description", ""),
                "status": status,
                "data_type": data_type,
                "tested_symbols": details.get("tested_symbols", []),
                "exchanges": details.get("exchanges", [])
            }
        
        return result
    except Exception as e:
        logger.error(f"Error getting market data subscription info: {e}")
        return {"error": str(e)}