"""
Omega-VX core package.

This package exposes lighter-weight helpers (configuration, logging, clients,
notifications) so that utility scripts no longer need to import the full
`omega_vx_bot` module with all of its boot-time side effects.
"""

from __future__ import annotations

from . import config, logging_utils, clients, notifications  # noqa: F401

__all__ = [
    "config",
    "logging_utils",
    "clients",
    "notifications",
]
