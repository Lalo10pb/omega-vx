"""Manual helper to test protection order placement."""

from omega_vx_bot import place_split_protection


if __name__ == "__main__":
    # Example values – adjust to match the live position you want to protect.
    place_split_protection("MSFT", tp_price=330.0, sl_price=300.0)
