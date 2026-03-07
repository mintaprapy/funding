# Exchanges Directory

All exchange-specific collectors live here. Some exchanges also include a standalone single-exchange dashboard.

Typical folder contents:
- `<exchange>_funding_baseinfo.py`: fetch current symbol/base information
- `<exchange>_funding_history.py`: fetch funding history
- `funding_dashboard.py`: optional single-exchange dashboard

Current folders:
- `bincance_funding/`: Binance collectors and dashboard
- `bybit_funding/`: Bybit collectors and dashboard
- `aster_funding/`: Aster collectors and dashboard
- `hyperliquid_funding/`: Hyperliquid collectors and dashboard
- `backpack_funding/`: Backpack collectors and dashboard
- `ethereal_funding/`: Ethereal collectors
- `grvt_funding/`: GRVT collectors
- `standx_funding/`: StandX collectors
- `lighter_funding/`: Lighter collectors

Manual run examples:

```bash
python3 exchanges/bybit_funding/bybit_funding_baseinfo.py
python3 exchanges/bybit_funding/bybit_funding_history.py
python3 exchanges/bincance_funding/funding_dashboard.py
```

Notes:
- Shared DB path is controlled by `FUNDING_DB_PATH`.
- Shared helpers are in `core/common_funding.py`.
- The root scheduler reads exchange script locations from `core/funding_exchanges.py`, so moving folders again should be coordinated there.
