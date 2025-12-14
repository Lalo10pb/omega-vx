import sys
import os
import logging
from omega_vx.clients import get_trading_client, get_data_client
from omega_vx.market_scanner import MarketScanner

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_scanner():
    print("🚀 Initializing Manual Scanner Test...")
    
    try:
        trading_client = get_trading_client()
        data_client = get_data_client()
        
        scanner = MarketScanner(trading_client, data_client)
        
        print("🔍 Scanning Market (Hunter Strategy)...")
        candidates = scanner._scan_market()
        
        if candidates:
            print(f"✅ Found {len(candidates)} candidates:")
            for c in candidates:
                print(f"   - {c}")
        else:
            print("⚠️ No candidates found (Market might be closed or low volatility).")
            
    except Exception as e:
        print(f"❌ Test Failed: {e}")

if __name__ == "__main__":
    test_scanner()
