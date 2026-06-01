CREATE TABLE IF NOT EXISTS account_book (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
    net_liquidation REAL,
    total_cash REAL,
    available_funds REAL,
    buying_power REAL,
    gross_position REAL,
    unrealized_pnl REAL,
    realized_pnl REAL,
    init_margin_req REAL,
    maint_margin_req REAL,
    excess_liquidity REAL,
    cushion REAL,
    leverage REAL,
    day_trades_remaining REAL
);

CREATE TABLE IF NOT EXISTS order_book (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    exec_id TEXT UNIQUE NOT NULL,
    symbol TEXT NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
    side TEXT,
    qty REAL,
    price REAL,
    commission REAL,
    currency TEXT,
    order_type TEXT
);