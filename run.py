# ============================================================
#  run.py  —  IBKR Dashboard
#
#  Data parity with the old console framework
#
#   • Portfolio tab  : positions with Exchange + P&L%,
#                      all 13 account metrics incl. Cushion
#   • Execution tab  : full execution log (execId, fill price,
#                      commission, side, timestamp …)
#   • Order Book tab : live open orders  +  fill history from DB
#   • Contracts tab  : full details incl. trading/liquid hours,
#                      valid exchanges, order types
#   • Prices tab     : OHLCV chart + bar table + live snapshot
#   • Scanners tab   : live scanner results
#
#  Database writes (ibkr_database — account_book / order_book):
#   • account_book  : written ONLY when most-recent row is
#                     ≥ 1 calendar day old (or table is empty)
#   • order_book    : new execution fills, deduplicated by exec_id
#   Everything else is shown in the GUI but never persisted.
#
#  Threading model:
#   IBKRWorker (QThread/asyncio) ──sig──► GUI (main thread)
#                                              │
#                                         DbTask (QRunnable)
#                                         QThreadPool (max 1 DB thread)
#                                              │
#                                         DatabaseManager
# ============================================================
from __future__ import annotations

import sys
from ibkr_gui import *

# ════════════════════════════════════════════════════════════
#  DEBUG HELPER
# ════════════════════════════════════════════════════════════

def _p(section: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}][{section}] {msg}", flush=True)
# ════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    cfg = ConnectionConfig()

    _p("Main", "=" * 55)
    _p("Main", "IBKR Dashboard starting")
    _p("Main", f"Host={cfg.host}  Port={cfg.port}  ClientID={cfg.client_id}")
    _p("Main", f"IBC path={cfg.ibc_path!r}  Gateway wait={GATEWAY_WAIT}s")
    _p("Main", "=" * 55)

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(STYLE)

    win = MainWindow(cfg)
    win.show()

    _p("Main", "Qt event loop starting")
    sys.exit(app.exec())