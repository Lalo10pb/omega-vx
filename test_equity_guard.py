from omega_vx_bot import should_block_trading_due_to_equity

if should_block_trading_due_to_equity():
    print("✅ Guard Triggered: Equity dropped > 5%")
else:
    print("❌ Guard Failed: Equity drop not detected")
