import os
import logging
from typing import List, Tuple
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta, timezone
from omega_vx.scanner import get_watchlist_from_google_sheet
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus

logger = logging.getLogger("omega_vx.scanner")

class MarketScanner:
    """
    Hybrid Scanner:
    1. Hunter: Scans Alpaca for Top Gainers/Volatile stocks.
    2. Sniper: Reads 'User Priority' from Google Sheets.
    """

    def __init__(self, trading_client: TradingClient, data_client: StockHistoricalDataClient):
        self.trading_client = trading_client
        self.data_client = data_client

    def get_candidates(self) -> List[str]:
        """
        Returns a unique list of symbols from both sources.
        """
        hunter_list = self._scan_market()
        sniper_list = self._get_sheet_watchlist()
        
        # Merge and dedupe (Priority list first)
        combined = list(set(sniper_list + hunter_list))
        logger.info(f"Market Scanner found {len(combined)} candidates (Hunter: {len(hunter_list)}, Sniper: {len(sniper_list)})")
        return combined

    def _scan_market(self) -> List[str]:
        """
        The Hunter: Finds top gainers and high volume stocks via Alpaca API.
        Dynamically fetches all active US Equity assets and scans for volatility.
        """
        try:
            # 1. Dynamic Universe Fetching
            logger.info("Fetching all active US Equity assets from Alpaca...")
            search_params = GetAssetsRequest(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY
            )
            assets = self.trading_client.get_all_assets(search_params)
            
            # Filter for tradable, marginable (optional: shortable)
            # We want liquid stocks, so usually marginable is a good proxy for better quality.
            tradable_assets = [
                a.symbol for a in assets 
                if a.tradable and a.marginable and "." not in a.symbol # Exclude warrants/rights often having decimals
            ]
            
            logger.info(f"Found {len(tradable_assets)} tradable assets. Fetching bars...")
            
            # 2. Chunked Bar Requests
            # Using get_stock_bars is more robust than snapshots for bulk data 
            # as it avoids SDK crashes on missing keys for some symbols.
            chunk_size = 500 # Reduced chunk size to prevent large response timeouts
            # The SDK handles pagination. 
            chunk_size = 500
            
            candidates = []
            start_time = datetime.now(timezone.utc) - timedelta(days=5) # Ensure we get at least 2 days of data
            
            for i in range(0, len(tradable_assets), chunk_size):
                chunk = tradable_assets[i:i + chunk_size]
                if not chunk: 
                    continue
                    
                try:
                    request_params = StockBarsRequest(
                        symbol_or_symbols=chunk,
                        timeframe=TimeFrame.Day,
                        start=start_time
                    )
                    bars = self.data_client.get_stock_bars(request_params)
                    
                    # 3. Analyze Bars
                    for symbol, bar_data in bars.data.items():
                        try:
                            if len(bar_data) < 2:
                                continue
                                
                            # Get last two bars
                            prev_bar = bar_data[-2]
                            curr_bar = bar_data[-1]
                            
                            current_price = curr_bar.close
                            prev_close = prev_bar.close
                            volume = curr_bar.volume
                            
                            if prev_close == 0: continue
                            
                            pct_change = ((current_price - prev_close) / prev_close) * 100
                            
                            # Filter Logic:
                            # 1. Minimum Volume (>100k)
                            # 2. Positive Gain > 3.0%
                            # 3. Price > $2.0
                            
                            if volume > 100000 and pct_change >= 3.0 and current_price > 2.0:
                                candidates.append({
                                    "symbol": symbol,
                                    "pct_change": pct_change,
                                    "volume": volume
                                })
                        except Exception:
                            continue

                except Exception as e:
                    logger.error(f"Bar chunk {i//chunk_size + 1} failed: {e}")
                    continue

            logger.info(f"Analyzed bars. Found {len(candidates)} potential candidates.")

            # 4. Sort by Top Gainers
            candidates.sort(key=lambda x: x["pct_change"], reverse=True)
            
            # Return top 20 for the Hunter to pick from
            top_picks = [c["symbol"] for c in candidates[:20]]
            
            if top_picks:
                logger.info(f"Hunter Strategy found top movers: {top_picks}")
            else:
                logger.warning("Hunter Strategy found no candidates meeting criteria.")
            
            return top_picks

        except Exception as e:
            logger.error(f"Hunter Scan failed: {e}")
            return []

    def _get_sheet_watchlist(self) -> List[str]:
        """
        The Sniper: Reads from Google Sheet.
        Re-implements the logic from scanner.py to be self-contained.
        """
        # ... logic from scanner.py ...
        # For brevity in this artifact, reusing the pattern but cleaner.
        try:
            # (Simplified for the plan execution - we will connect this to the real scanner.py logic later or copy it)
            # Checking if we can import the existing function
            from omega_vx.scanner import get_watchlist_from_google_sheet
            return get_watchlist_from_google_sheet()
        except ImportError:
            # Fallback if import fails (unlikely in same pkg)
            return []
        except Exception as e:
            logger.error(f"Sniper Sheet read failed: {e}")
            return []
