from __future__ import annotations

import os
import time

import health_watchdog as hw


def test_collect_statuses_detects_missing(tmp_path):
    target = ("foo.csv", tmp_path / "foo.csv", 60)
    statuses = hw.collect_statuses([target])
    assert len(statuses) == 1
    assert statuses[0].label == "foo.csv"
    assert statuses[0].ok is False
    assert "missing" in statuses[0].detail


def test_collect_statuses_detects_stale(tmp_path):
    path = tmp_path / "bar.csv"
    path.write_text("hello")
    old = time.time() - 10_000
    os.utime(path, (old, old))
    target = ("bar.csv", path, 60)
    status = hw.collect_statuses([target])[0]
    assert status.ok is False
    assert "old" in status.detail


def test_collect_statuses_detects_fresh(tmp_path):
    path = tmp_path / "fresh.csv"
    path.write_text("1")
    target = ("fresh.csv", path, 3600)
    status = hw.collect_statuses([target])[0]
    assert status.ok is True
    assert "old" in status.detail
