CREATE INDEX IF NOT EXISTS idx_ab_ts
    ON account_book(recorded_at);

CREATE INDEX IF NOT EXISTS idx_ob_symbol
    ON order_book(symbol);

CREATE INDEX IF NOT EXISTS idx_ob_ts
    ON order_book(recorded_at);