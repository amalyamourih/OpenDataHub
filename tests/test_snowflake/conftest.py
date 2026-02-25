# tests/conftest.py
import sys
from unittest.mock import MagicMock

# ── Modules optionnels non installés localement ────────────────────────────
_sf = MagicMock()
sys.modules["snowflake"]                  = _sf
sys.modules["snowflake.connector"]        = _sf.connector
sys.modules["snowflake.connector.errors"] = _sf.connector.errors