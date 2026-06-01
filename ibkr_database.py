# ============================================================
#  ibkr_database.py  —  SQLite persistence layer
# ============================================================
#
#  Tables
#  ──────
#  account_book  – account summary snapshots
#                  stored ONLY when the most-recent entry is
#                  more than one calendar day old (or absent)
#
#  order_book    – execution log entries (one row per fill)
#                  deduplicated by exec_id (INSERT OR IGNORE)
#
#  All other data (portfolio positions, historical bars,
#  snapshots, contracts, scanner results, fundamentals) is
#  displayed in the GUI but never persisted.
# ============================================================

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

_DB_PATH = Path("ibkr_data.db")


# ════════════════════════════════════════════════════════════
#  LOW-LEVEL CONNECTION
# ════════════════════════════════════════════════════════════

def get_connection(path: Path = _DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


# ════════════════════════════════════════════════════════════
#  SCHEMA
# ════════════════════════════════════════════════════════════

SQL_DIR = Path(__file__).parent / "sql"

def load_sql(*files: str) -> str:
    return "\n".join(
        (SQL_DIR / f).read_text(encoding="utf-8")
        for f in files
    )

DDL = load_sql(
    "schema.sql",
    "indexes.sql",
)


def init_db(path: Path = _DB_PATH) -> sqlite3.Connection:
    conn = get_connection(path)
    conn.executescript(DDL)
    conn.commit()
    print(f"[DB] Initialized → {path.resolve()}")
    return conn


# ════════════════════════════════════════════════════════════
#  DATABASE MANAGER
# ════════════════════════════════════════════════════════════

class DatabaseManager:
    """
    Thread-safe facade over the two-table SQLite database.

    One shared instance is created at startup (get_db()).
    All writes commit immediately so the GUI thread sees
    fresh data on the next query.
    """

    def __init__(self, path: Path = _DB_PATH) -> None:
        self.conn = init_db(path)

    def close(self) -> None:
        self.conn.close()

    # ── helpers ──────────────────────────────────────────────

    def _exec(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    def _now(self) -> str:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _safe_float(self, val: Any) -> float | None:
        try:
            f = float(val)
            return None if f != f else f      # NaN → None
        except (TypeError, ValueError):
            return None

    # ════════════════════════════════════════════════════════
    #  ACCOUNT BOOK
    # ════════════════════════════════════════════════════════

    def should_store_account(self) -> bool:
        """
        Return True iff there is no stored row, or the most recent
        row is older than one full calendar day (≥ 86 400 seconds).
        """
        row = self.get_latest_account()
        if not row:
            return True
        try:
            dt = datetime.strptime(row["recorded_at"], "%Y-%m-%d %H:%M:%S")
            return (datetime.utcnow() - dt).total_seconds() >= 86_400
        except (ValueError, TypeError):
            return True

    def insert_account_book(self, data: dict) -> None:
        """
        Persist an account summary dict.
        Keys must use the IBKR CamelCase format (e.g. 'NetLiquidation').
        Call should_store_account() first to avoid needless rows.
        """
        def g(k): return self._safe_float(data.get(k))
        self._exec(
            """
            INSERT INTO account_book
              (recorded_at,
               net_liquidation, total_cash, available_funds,
               buying_power, gross_position,
               unrealized_pnl, realized_pnl,
               init_margin_req, maint_margin_req, excess_liquidity,
               cushion, leverage, day_trades_remaining)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                self._now(),
                g("NetLiquidation"), g("TotalCashValue"), g("AvailableFunds"),
                g("BuyingPower"),    g("GrossPositionValue"),
                g("UnrealizedPnL"), g("RealizedPnL"),
                g("InitMarginReq"), g("MaintMarginReq"),
                g("ExcessLiquidity"), g("Cushion"),
                g("Leverage"),       g("DayTradesRemaining"),
            ),
        )

    def get_latest_account(self) -> sqlite3.Row | None:
        cur = self.conn.execute(
            "SELECT * FROM account_book ORDER BY recorded_at DESC LIMIT 1"
        )
        return cur.fetchone()

    def get_account_history(self, limit: int = 200) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            "SELECT * FROM account_book ORDER BY recorded_at DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()

    # ════════════════════════════════════════════════════════
    #  ORDER BOOK  (execution log)
    # ════════════════════════════════════════════════════════

    def insert_executions(self, records: list[dict]) -> int:
        """
        Bulk-insert execution records.
        Uses INSERT OR IGNORE so duplicate exec_ids are silently skipped.
        Returns the number of rows attempted (not the actual inserted count).
        """
        if not records:
            return 0
        rows = []
        for r in records:
            rows.append((
                r.get("order_id"),
                str(r.get("exec_id", "") or ""),
                (r.get("symbol", "") or "").upper(),
                str(r.get("timestamp", self._now()))[:19],
                r.get("side"),
                self._safe_float(r.get("qty")),
                self._safe_float(r.get("price")),
                self._safe_float(r.get("commission")),
                r.get("currency"),
                r.get("order_type"),
            ))
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO order_book
              (order_id, exec_id, symbol, recorded_at,
               side, qty, price, commission, currency, order_type)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def get_execution_log(self, limit: int = 500) -> list[sqlite3.Row]:
        """Return execution log rows, newest-first."""
        cur = self.conn.execute(
            "SELECT * FROM order_book ORDER BY recorded_at DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()

    def get_executions_for_symbol(self, symbol: str) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            "SELECT * FROM order_book WHERE symbol = ? ORDER BY recorded_at DESC",
            (symbol.upper(),),
        )
        return cur.fetchall()

    # ════════════════════════════════════════════════════════
    #  UTILITY
    # ════════════════════════════════════════════════════════

    def get_db_stats(self) -> dict:
        stats = {}
        for t in ("account_book", "order_book"):
            cur = self.conn.execute(f"SELECT COUNT(*) FROM {t}")
            stats[t] = cur.fetchone()[0]
        return stats

    def vacuum(self) -> None:
        self.conn.execute("VACUUM;")
        print("[DB] VACUUM complete.")


# ════════════════════════════════════════════════════════════
#  MODULE-LEVEL SINGLETON
# ════════════════════════════════════════════════════════════

_db: DatabaseManager | None = None


def get_db(path: Path = _DB_PATH) -> DatabaseManager:
    global _db
    if _db is None:
        _db = DatabaseManager(path)
    return _db


# ════════════════════════════════════════════════════════════
#  QUICK SELF-TEST
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    db = DatabaseManager(_DB_PATH)

    print("should_store_account (fresh DB):", db.should_store_account())   # True

    db.insert_account_book({
        "NetLiquidation": 100_000.0, "TotalCashValue": 50_000.0,
        "AvailableFunds": 45_000.0,  "BuyingPower":    90_000.0,
        "GrossPositionValue": 55_000.0,
        "UnrealizedPnL": 1_234.56,   "RealizedPnL":   -200.0,
        "InitMarginReq": 10_000.0,   "MaintMarginReq": 8_000.0,
        "ExcessLiquidity": 37_000.0, "Cushion":         0.37,
        "Leverage": 1.1,             "DayTradesRemaining": 3.0,
    })
    print("should_store_account (just stored):", db.should_store_account())  # False
    print("latest account:", dict(db.get_latest_account()))

    n = db.insert_executions([
        {"order_id": 1, "exec_id": "EXEC001", "symbol": "AAPL",
         "timestamp": "2025-01-15 10:30:00", "side": "BUY",
         "qty": 100, "price": 185.50, "commission": 1.0,
         "currency": "USD", "order_type": "LMT"},
        {"order_id": 1, "exec_id": "EXEC001",   # duplicate — ignored
         "symbol": "AAPL", "timestamp": "2025-01-15 10:30:00",
         "side": "BUY", "qty": 100, "price": 185.50,
         "commission": 1.0, "currency": "USD", "order_type": "LMT"},
        {"order_id": 2, "exec_id": "EXEC002", "symbol": "MSFT",
         "timestamp": "2025-01-15 11:00:00", "side": "SELL",
         "qty": 50, "price": 420.0, "commission": 0.75,
         "currency": "USD", "order_type": "MKT"},
    ])
    print(f"insert_executions attempted {n} rows")
    execs = db.get_execution_log()
    print(f"execution log rows: {len(execs)}")   # 2 (EXEC001 deduped)
    for r in execs:
        print(" ", dict(r))

    print("stats:", db.get_db_stats())
    db.close()
    print("\n[OK] Self-test passed.")