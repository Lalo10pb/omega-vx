from __future__ import annotations

from omega_vx.notifications import flush_alert_queue


def main() -> None:
    sent = flush_alert_queue()
    print(f"Flushed {sent} queued alert(s).")


if __name__ == "__main__":
    main()
