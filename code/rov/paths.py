"""Locations of repository data, owned runtime state, and existing software."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "runtime/config"
BIN = ROOT / "software/bin"
