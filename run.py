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

import asyncio
import sys
import threading
from datetime import datetime
from typing import Any, Callable

import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTableWidget, QTableWidgetItem, QLabel, QPushButton,
    QLineEdit, QComboBox, QDoubleSpinBox, QSpinBox, QHeaderView,
    QSizePolicy, QGroupBox, QSplitter, QFrame, QTextEdit
)
from PyQt6.QtCore import Qt, QTimer, QRunnable, QThreadPool, QObject, pyqtSignal
from PyQt6.QtGui import QColor, QCloseEvent, QFont

import matplotlib
matplotlib.use("QtAgg") # QtAgg works for both 5 and 6
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from ib_insync import Contract, TagValue
from ibkr_database import DatabaseManager, get_db
from ibkr_client import IBKRWorker, ConnectionConfig, format_contract_info


# ── Palette ──────────────────────────────────────────────────
BG  = "#0d0d0d"
SU  = "#1a1a1a"
BO  = "#2a2a2a"
TX  = "#e8e8e8"
DM  = "#888888"
AC  = "#cc785c"
G   = "#4caf82"
R   = "#e05555"
INF = "#5a9fd4"

STYLE = f"""
QMainWindow, QWidget  {{ background:{BG}; color:{TX};
                         font-family:'Inter','Segoe UI',Arial; font-size:12px }}
QTabWidget::pane      {{ border:1px solid {BO}; background:{SU}; border-radius:6px }}
QTabBar::tab          {{ background:transparent; color:{DM}; padding:9px 24px;
                         border:none; margin-right:2px; font-weight:500 }}
QTabBar::tab:selected {{ color:{AC}; border-bottom:2px solid {AC} }}
QTabBar::tab:hover    {{ color:{TX} }}
QTableWidget          {{ background:{SU}; gridline-color:{BO}; border:none; color:{TX};
                         selection-background-color:#242424; border-radius:4px }}
QHeaderView::section  {{ background:{BG}; color:{DM}; border:none;
                         border-bottom:1px solid {BO}; padding:5px 10px;
                         font-weight:500; font-size:11px }}
QLineEdit,QComboBox,QDoubleSpinBox,QSpinBox {{
    background:{SU}; color:{TX}; border:1px solid {BO};
    border-radius:5px; padding:5px 10px }}
QLineEdit:focus,QComboBox:focus,
QDoubleSpinBox:focus,QSpinBox:focus {{ border-color:{AC} }}
QComboBox::drop-down  {{ border:none }}
QPushButton           {{ background:{SU}; color:{TX}; border:1px solid {BO};
                         border-radius:5px; padding:6px 16px; font-weight:500 }}
QPushButton:hover     {{ background:#252525; border-color:{DM} }}
QPushButton:pressed   {{ background:{BO} }}
QGroupBox             {{ color:{DM}; border:1px solid {BO}; border-radius:6px;
                         margin-top:12px; padding:10px 8px 8px 8px }}
QGroupBox::title      {{ subcontrol-origin:margin; padding:0 8px; color:{AC};
                         font-weight:600; font-size:11px }}
QLabel                {{ color:{TX} }}
QTextEdit             {{ background:{SU}; color:{TX}; border:1px solid {BO}; border-radius:5px }}
QScrollBar:vertical   {{ background:{BG}; width:6px; border:none }}
QScrollBar::handle:vertical {{ background:{BO}; border-radius:3px; min-height:20px }}
QSplitter::handle     {{ background:{BO} }}
"""


# ════════════════════════════════════════════════════════════
#  ASYNC DB BRIDGE
# ════════════════════════════════════════════════════════════

class _DbSignals(QObject):
    done = pyqtSignal(object, object)


class _DbTask(QRunnable):
    def __init__(self, fn, args, kwargs, callback, signals) -> None:
        super().__init__()
        self._fn = fn; self._args = args; self._kwargs = kwargs
        self._callback = callback; self._signals = signals
        self.setAutoDelete(True)

    def run(self) -> None:
        try:
            result = self._fn(*self._args, **self._kwargs)
        except Exception as exc:
            result = exc
        self._signals.done.emit(result, self._callback)


class DbBridge:
    _pool = QThreadPool()
    _pool.setMaxThreadCount(1)

    def __init__(self, manager: DatabaseManager) -> None:
        self.api   = manager
        self._sigs = _DbSignals()
        self._sigs.done.connect(self._dispatch, Qt.ConnectionType.QueuedConnection)

    def run(self, fn: Callable, *args,
            callback: Callable | None = None, **kwargs) -> None:
        task = _DbTask(fn, args, kwargs, callback, self._sigs)
        self._pool.start(task)

    @staticmethod
    def _dispatch(result: Any, callback: Any) -> None:
        if callback is None:
            return
        if isinstance(result, Exception):
            _p("DB", f"Error: {result}")
            return
        callback(result)

    def close(self) -> None:
        self._pool.waitForDone(3000)
        self.api.close()


# ════════════════════════════════════════════════════════════
#  DEBUG HELPER
# ════════════════════════════════════════════════════════════

def _p(section: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}][{section}] {msg}", flush=True)


# ════════════════════════════════════════════════════════════
#  SHARED UI HELPERS
# ════════════════════════════════════════════════════════════

def _tbl(cols: list[str]) -> QTableWidget:
    t = QTableWidget(0, len(cols))
    t.setHorizontalHeaderLabels(cols)
    t.verticalHeader().setVisible(False)
    t.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
    t.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
    t.setAlternatingRowColors(True)
    t.setStyleSheet("alternate-background-color:#181818;")
    h = t.horizontalHeader()
    if h:
        h.setStretchLastSection(True)
    return t


def _kv(data: dict) -> QTableWidget:
    t = QTableWidget(len(data), 2)
    t.setHorizontalHeaderLabels(["Field", "Value"])
    t.verticalHeader().setVisible(False)
    t.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
    h = t.horizontalHeader()
    if h:
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
    for i, (k, v) in enumerate(data.items()):
        t.setItem(i, 0, QTableWidgetItem(str(k)))
        t.setItem(i, 1, QTableWidgetItem(str(v) if v is not None else "N/A"))
    return t


def _btn(label: str, color: str = "") -> QPushButton:
    b = QPushButton(label)
    if color:
        b.setStyleSheet(
            f"QPushButton{{background:{color};color:#fff;border:none;"
            f"border-radius:5px;padding:6px 18px;font-weight:600}}"
            f"QPushButton:hover{{background:{color};border:2px solid #ffffff33}}"
            f"QPushButton:pressed{{background:{color};border:2px solid #00000044}}"
        )
    return b


def _row(label: str, widget: QWidget, lw: int = 110) -> QHBoxLayout:
    h = QHBoxLayout()
    lbl = QLabel(label)
    lbl.setFixedWidth(lw)
    lbl.setStyleSheet(f"color:{DM}; font-size:11px;")
    h.addWidget(lbl)
    h.addWidget(widget)
    return h


def _div() -> QFrame:
    f = QFrame()
    f.setFrameShape(QFrame.Shape.HLine)
    f.setStyleSheet(f"background:{BO}; max-height:1px; border:none;")
    return f


def _dim_lbl(msg: str) -> QLabel:
    lbl = QLabel(msg)
    lbl.setStyleSheet(f"color:{DM}; padding:20px;")
    lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
    return lbl


def _clear(layout) -> None:
    while layout.count():
        w = layout.takeAt(0).widget()
        if w:
            w.deleteLater()


def _replace(layout, widget: QWidget) -> None:
    _clear(layout)
    layout.addWidget(widget)


# ════════════════════════════════════════════════════════════
#  CHART
# ════════════════════════════════════════════════════════════

class PriceChart(FigureCanvas):
    def __init__(self) -> None:
        self.fig = Figure(facecolor=SU)
        super().__init__(self.fig)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.ax  = self.fig.add_subplot(211)
        self.axv = self.fig.add_subplot(212, sharex=self.ax)
        self.fig.subplots_adjust(left=0.09, right=0.97, top=0.92, bottom=0.12, hspace=0.06)

    def _style(self) -> None:
        for ax in (self.ax, self.axv):
            ax.set_facecolor(SU)
            ax.tick_params(colors=DM, labelsize=8)
            for sp in ax.spines.values():
                sp.set_color(BO)
            ax.grid(True, color=BO, linewidth=0.4, linestyle="--", alpha=0.5)

    def plot(self, df: pd.DataFrame, symbol: str = "", bar_size: str = "") -> None:
        if df is None or df.empty:
            return
        self.ax.clear(); self.axv.clear()
        x     = pd.to_datetime(df["date"] if "date" in df.columns else df.index)
        close = df["close"].to_numpy(dtype=float)
        open_ = df["open"].to_numpy(dtype=float)
        self.ax.plot(x, close, color=AC, linewidth=1.5)
        self.ax.fill_between(x, close, float(close.min()), color=AC, alpha=0.08)
        self.ax.set_ylabel("Price", fontsize=9)
        self.ax.set_title(f"{symbol}  —  {bar_size}", color=TX, fontsize=10, pad=6, loc="left")
        self.ax.tick_params(axis="x", labelbottom=False)
        if "volume" in df.columns:
            vol    = df["volume"].to_numpy(dtype=float)
            colors = [G if c >= o else R for c, o in zip(close, open_)]
            self.axv.bar(x, vol, color=colors, width=1.5, alpha=0.7)
            self.axv.set_ylabel("Volume", fontsize=9)
        self._style()
        self.fig.autofmt_xdate(rotation=30, ha="right")
        self.draw()

    def plot_equity(self, dates: list, values: list, title: str) -> None:
        self.ax.clear(); self.axv.clear()
        if not dates:
            return
        x = pd.to_datetime(dates)
        self.ax.plot(x, values, color=G, linewidth=1.5, marker="o", markersize=3)
        self.ax.fill_between(x, values, 0, color=G, alpha=0.07)
        self.ax.set_title(title, color=TX, fontsize=10, pad=6, loc="left")
        self.ax.set_ylabel("Value", fontsize=9)
        self.axv.set_visible(False)
        self._style()
        self.fig.autofmt_xdate(rotation=30, ha="right")
        self.draw()


# ════════════════════════════════════════════════════════════
#  TAB 1 — PORTFOLIO
#
#  Shows all 13 account metrics (matching old get_account_summary)
#  including Cushion.  Positions include Exchange and P&L%.
#
#  DB writes:  account_book ONLY when most-recent row is ≥ 1 day old.
#  Portfolio positions are NOT stored.
# ════════════════════════════════════════════════════════════

# Two rows of account metric cards — matches old AccountManager output
_ACCT_R1 = [
    ("NetLiquidation",     "Net Liq."),
    ("TotalCashValue",     "Cash"),
    ("AvailableFunds",     "Avail. Funds"),
    ("BuyingPower",        "Buying Power"),
    ("GrossPositionValue", "Gross Pos."),
    ("UnrealizedPnL",      "Unreal. P&L"),
    ("RealizedPnL",        "Real. P&L"),
]
_ACCT_R2 = [
    ("InitMarginReq",      "Init Margin"),
    ("MaintMarginReq",     "Maint Margin"),
    ("ExcessLiquidity",    "Excess Liq."),
    ("Cushion",            "Cushion"),
    ("Leverage",           "Leverage"),
    ("DayTradesRemaining", "Day Trades"),
]


class PortfolioTab(QWidget):
    def __init__(self, db: DbBridge, worker: IBKRWorker) -> None:
        super().__init__()
        self.db     = db
        self.worker = worker
        self._vals: dict[str, QLabel] = {}
        self._build()
        self._load_from_db()

    def _build(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        # ── Account summary (2 rows of cards) ───────────────
        acc = QGroupBox("Account")
        acv = QVBoxLayout(acc)
        acv.setSpacing(4)
        for row_keys in [_ACCT_R1, _ACCT_R2]:
            rw = QWidget()
            rl = QHBoxLayout(rw)
            rl.setSpacing(4)
            rl.setContentsMargins(0, 0, 0, 0)
            for key, label in row_keys:
                card = QWidget()
                card.setStyleSheet(
                    f"background:{BG}; border:1px solid {BO}; border-radius:5px;"
                )
                cl = QVBoxLayout(card)
                cl.setContentsMargins(8, 6, 8, 6)
                cl.setSpacing(2)
                lbl = QLabel(label)
                lbl.setStyleSheet(
                    f"color:{DM}; font-size:10px; font-weight:500;"
                )
                lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
                val = QLabel("—")
                val.setStyleSheet(
                    f"color:{TX}; font-weight:600; font-size:13px;"
                )
                val.setAlignment(Qt.AlignmentFlag.AlignCenter)
                self._vals[key] = val
                cl.addWidget(lbl)
                cl.addWidget(val)
                rl.addWidget(card)
            acv.addWidget(rw)
        rb = _btn("↺  Refresh", AC)
        rb.clicked.connect(self._refresh)
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_row.addWidget(rb)
        acv.addLayout(btn_row)
        root.addWidget(acc)

        # ── Positions table + equity chart ───────────────────
        split  = QSplitter(Qt.Orientation.Vertical)
        pos_g  = QGroupBox("Positions")
        pl     = QVBoxLayout(pos_g)
        self._pos = _tbl([
            "Symbol", "Type", "Exchange", "Position", "Avg Cost",
            "Mkt Price", "Mkt Value", "Unreal. P&L", "Real. P&L", "P&L%", "CCY",
        ])
        pl.addWidget(self._pos)
        split.addWidget(pos_g)

        chart_g = QGroupBox("Net Liquidation History")
        cgl = QVBoxLayout(chart_g)
        self._chart = PriceChart()
        cgl.addWidget(self._chart)
        split.addWidget(chart_g)
        split.setStretchFactor(0, 3)
        split.setStretchFactor(1, 2)
        root.addWidget(split, 1)

    # ── Actions ──────────────────────────────────────────────
    def _refresh(self) -> None:
        _p("Portfolio", "Refresh → requesting portfolio + account")
        self.worker.request("portfolio")
        self.worker.request("account")

    def _load_from_db(self) -> None:
        """Load last stored account summary and history chart from DB."""
        _p("Portfolio", "Loading account data from DB (async)")
        self.db.run(self.db.api.get_latest_account,
                    callback=self._on_acc_loaded)
        self.db.run(self.db.api.get_account_history, 120,
                    callback=self._on_hist_loaded)

    # ── DB callbacks ─────────────────────────────────────────
    def _on_acc_loaded(self, row) -> None:
        if row:
            _p("Portfolio", "Filling account cards from DB")
            self._fill_acc(_db_row_to_acct(row))

    def _on_hist_loaded(self, rows) -> None:
        if not rows:
            return
        s = list(reversed([r["recorded_at"]           for r in rows]))
        v = list(reversed([r["net_liquidation"] or 0   for r in rows]))
        _p("Portfolio", f"Plotting equity curve: {len(s)} points")
        self._chart.plot_equity(s, v, "Net Liquidation")

    # ── Fill helpers ─────────────────────────────────────────
    def _fill_pos(self, items: list) -> None:
        """Fill positions table from live ib.portfolio() PortfolioItem list."""
        self._pos.setRowCount(len(items))
        for i, p in enumerate(items):
            c    = p.contract
            exch = c.primaryExchange or c.exchange or ""
            pnl_pct = ""
            if p.averageCost and p.averageCost != 0 and p.position != 0:
                pnl_pct = (
                    f"{(p.unrealizedPNL / (abs(p.position) * p.averageCost)) * 100:.2f}%"
                )
            data = [
                c.symbol, c.secType, exch,
                f"{p.position:.0f}",
                f"{p.averageCost:.2f}",
                f"{p.marketPrice:.2f}",
                f"{p.marketValue:.2f}",
                f"{p.unrealizedPNL:.2f}",
                f"{p.realizedPNL:.2f}",
                pnl_pct,
                c.currency,
            ]
            for j, v in enumerate(data):
                it = QTableWidgetItem(str(v))
                if j == 7:   # Unreal. P&L
                    it.setForeground(QColor(G if (p.unrealizedPNL or 0) >= 0 else R))
                if j == 9:   # P&L%
                    it.setForeground(QColor(G if (p.unrealizedPNL or 0) >= 0 else R))
                self._pos.setItem(i, j, it)
        self._pos.resizeColumnsToContents()

    def _fill_acc(self, d: dict) -> None:
        """Update account metric cards. d must use IBKR CamelCase keys."""
        _money = {
            "NetLiquidation", "TotalCashValue", "AvailableFunds",
            "BuyingPower", "GrossPositionValue", "UnrealizedPnL",
            "RealizedPnL", "InitMarginReq", "MaintMarginReq", "ExcessLiquidity",
        }
        for key, lbl in self._vals.items():
            val = d.get(key)
            if val is None:
                lbl.setText("—")
                lbl.setStyleSheet(f"color:{TX}; font-weight:600; font-size:13px;")
                continue
            if key == "Leverage":
                text  = f"{val:.2f}×"
                color = TX
            elif key == "Cushion":
                text  = f"{val:.4f}"
                color = G if val > 0.05 else R
            elif key == "DayTradesRemaining":
                text  = f"{int(val)}"
                color = TX
            elif key in _money:
                text  = f"{val:,.0f}"
                color = G if val >= 0 else TX
                if key in ("UnrealizedPnL", "RealizedPnL"):
                    color = G if val >= 0 else R
            else:
                text  = str(val)
                color = TX
            lbl.setText(text)
            lbl.setStyleSheet(f"color:{color}; font-weight:600; font-size:13px;")

    # ── Signal handlers ───────────────────────────────────────
    def on_portfolio(self, items: list) -> None:
        _p("Portfolio", f"[SIGNAL] sig_portfolio: {len(items)} items")
        self._fill_pos(items)   # live data — not stored in DB

    def on_account(self, d: dict) -> None:
        _p("Portfolio", f"[SIGNAL] sig_account: {list(d.keys())}")
        self._fill_acc(d)       # always update GUI

        # Write to DB only when data is ≥ 1 day old (or table is empty)
        def _write_if_needed(_=None):
            if self.db.api.should_store_account():
                _p("Portfolio", "  → storing account_book row (≥1 day since last)")
                self.db.api.insert_account_book(d)
            return self.db.api.get_account_history(120)

        self.db.run(_write_if_needed, callback=self._on_hist_loaded)


# ── Helper: convert account_book DB row → IBKR CamelCase dict ─────────────

def _db_row_to_acct(row) -> dict:
    r = dict(row)
    return {
        "NetLiquidation":     r.get("net_liquidation"),
        "TotalCashValue":     r.get("total_cash"),
        "AvailableFunds":     r.get("available_funds"),
        "BuyingPower":        r.get("buying_power"),
        "GrossPositionValue": r.get("gross_position"),
        "UnrealizedPnL":      r.get("unrealized_pnl"),
        "RealizedPnL":        r.get("realized_pnl"),
        "InitMarginReq":      r.get("init_margin_req"),
        "MaintMarginReq":     r.get("maint_margin_req"),
        "ExcessLiquidity":    r.get("excess_liquidity"),
        "Cushion":            r.get("cushion"),
        "Leverage":           r.get("leverage"),
        "DayTradesRemaining": r.get("day_trades_remaining"),
    }


# ════════════════════════════════════════════════════════════
#  TAB 2 — EXECUTION
#
#  Matches old OrderManager.execution_log format:
#  orderId, execId, symbol, timestamp, side, qty, price,
#  commission, currency, orderType
#
#  DB write: order_book — INSERT OR IGNORE per exec_id.
#  Loaded from DB at startup and refreshed on every new fill.
# ════════════════════════════════════════════════════════════

_EXEC_COLS = ["Time", "Symbol", "Side", "Qty", "Price",
              "Comm.", "CCY", "Type", "Order ID", "Exec ID"]


class ExecutionTab(QWidget):
    def __init__(self, db: DbBridge, worker: IBKRWorker) -> None:
        super().__init__()
        self.db     = db
        self.worker = worker
        self._build()
        # Load existing execution log from DB at startup
        self.db.run(self.db.api.get_execution_log,
                    callback=self._fill_exec_table)

    def _build(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(12)

        # ── Left: Order Entry form ───────────────────────────
        form = QGroupBox("Order Entry")
        form.setMaximumWidth(360)
        fl = QVBoxLayout(form)
        fl.setSpacing(6)

        self._sym_e = QLineEdit()
        self._sym_e.setPlaceholderText("e.g. AIR")
        self._stype = QComboBox()
        self._stype.addItems(["STK", "FUT", "CASH", "CMDTY"])
        self._exch  = QLineEdit("SMART")
        self._cur   = QLineEdit("EUR")
        self._otype = QComboBox()
        self._otype.addItems(["MKT", "LMT", "TRAIL", "STP"])
        self._otype.currentTextChanged.connect(self._on_type)
        self._qty   = QDoubleSpinBox()
        self._qty.setRange(0.01, 1_000_000)
        self._qty.setValue(10)
        self._lmt   = QDoubleSpinBox()
        self._lmt.setRange(0, 1_000_000)
        self._lmt.setDecimals(4)
        self._stp   = QDoubleSpinBox()
        self._stp.setRange(0, 1_000_000)
        self._stp.setDecimals(4)
        self._trl   = QDoubleSpinBox()
        self._trl.setRange(0.01, 50)
        self._trl.setValue(5)
        self._trl.setSuffix(" %")

        fl.addLayout(_row("Symbol",     self._sym_e))
        fl.addLayout(_row("Sec Type",   self._stype))
        fl.addLayout(_row("Exchange",   self._exch))
        fl.addLayout(_row("Currency",   self._cur))
        fl.addWidget(_div())
        fl.addLayout(_row("Order Type", self._otype))
        fl.addLayout(_row("Quantity",   self._qty))
        self._rl = _row("Limit Price",  self._lmt)
        self._rs = _row("Stop Price",   self._stp)
        self._rt = _row("Trail %",      self._trl)
        fl.addLayout(self._rl)
        fl.addLayout(self._rs)
        fl.addLayout(self._rt)
        fl.addStretch()

        br = QHBoxLayout()
        buy  = _btn("▲  BUY",  G)
        sell = _btn("▼  SELL", R)
        buy.clicked.connect(lambda: self._send("BUY"))
        sell.clicked.connect(lambda: self._send("SELL"))
        br.addWidget(buy)
        br.addWidget(sell)
        fl.addLayout(br)
        root.addWidget(form)

        # ── Right: status log + execution log table ──────────
        right = QVBoxLayout()

        lg  = QGroupBox("Order Status")
        ll  = QVBoxLayout(lg)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setFont(QFont("Consolas", 10))
        self._log.setMaximumHeight(120)
        ll.addWidget(self._log)

        eg  = QGroupBox("Execution Log  (stored)")
        el  = QVBoxLayout(eg)
        self._exec_tbl = _tbl(_EXEC_COLS)
        el.addWidget(self._exec_tbl)

        right.addWidget(lg)
        right.addWidget(eg, 1)
        rw = QWidget()
        rw.setLayout(right)
        root.addWidget(rw, 1)

        self._on_type("MKT")

    def _on_type(self, t: str) -> None:
        for layout, show in [(self._rl, t == "LMT"),
                             (self._rs, t == "STP"),
                             (self._rt, t == "TRAIL")]:
            for i in range(layout.count()):
                w = layout.itemAt(i).widget()
                if w:
                    w.setVisible(show)

    def _contract(self) -> Contract:
        c = Contract()
        c.symbol   = self._sym_e.text().strip().upper()
        c.secType  = self._stype.currentText()
        c.exchange = self._exch.text().strip()
        c.currency = self._cur.text().strip()
        return c

    def _send(self, action: str) -> None:
        sym = self._sym_e.text().strip().upper()
        if not sym:
            self._log.append(f'<span style="color:{R}">[Error] Symbol required.</span>')
            return
        t = self._otype.currentText()
        _p("Execution", f"{action} → {self._qty.value():.0f} × {sym} ({t})")
        self.worker.request(
            "place_order",
            contract     = self._contract(),
            action       = action,
            quantity     = self._qty.value(),
            order_type   = t,
            lmt_price    = self._lmt.value(),
            stop_price   = self._stp.value(),
            trailing_pct = self._trl.value(),
        )
        ts    = datetime.now().strftime("%H:%M:%S")
        color = G if action == "BUY" else R
        self._log.append(
            f'<span style="color:{DM}">[{ts}]</span> '
            f'<span style="color:{color}">{action}</span> '
            f'{self._qty.value():.0f} × {sym} ({t}) — sent'
        )

    def _fill_exec_table(self, rows) -> None:
        """Fill execution log table from order_book DB rows (newest-first)."""
        self._exec_tbl.setRowCount(len(rows))
        for i, r in enumerate(rows):
            side = r["side"] or ""
            vals = [
                (r["recorded_at"] or "")[:16],
                r["symbol"] or "",
                side,
                f"{r['qty']:.0f}"        if r["qty"]        is not None else "—",
                f"{r['price']:.4f}"      if r["price"]      is not None else "—",
                f"{r['commission']:.4f}" if r["commission"]  is not None else "—",
                r["currency"]   or "—",
                r["order_type"] or "—",
                str(r["order_id"] or "—"),
                (r["exec_id"] or "")[:16],
            ]
            for j, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                if j == 2:   # Side
                    it.setForeground(QColor(G if side == "BUY" else R))
                self._exec_tbl.setItem(i, j, it)
        self._exec_tbl.resizeColumnsToContents()

    # ── Signal handlers ───────────────────────────────────────
    def on_order_placed(self, d: dict) -> None:
        _p("Execution", f"[SIGNAL] sig_order_placed: id={d.get('order_id')} status={d.get('status')}")
        ts    = datetime.now().strftime("%H:%M:%S")
        color = G if d["status"] == "Filled" else DM
        self._log.append(
            f'<span style="color:{DM}">[{ts}]</span> '
            f'Order <b>{d["order_id"]}</b> → '
            f'<span style="color:{color}">{d["status"]}</span>'
        )
        # Fills arrive via sig_executions — no DB write here

    def on_executions(self, records: list) -> None:
        """
        Called by sig_executions on every confirmed fill.
        Stores new records in order_book (INSERT OR IGNORE),
        then reloads the table from DB.
        """
        if not records:
            return
        _p("Execution",
           f"[SIGNAL] sig_executions: {len(records)} record(s) → order_book")
        # Log fills to status area
        for rec in records:
            ts    = datetime.now().strftime("%H:%M:%S")
            side  = rec.get("side", "")
            color = G if side == "BUY" else R
            self._log.append(
                f'<span style="color:{DM}">[{ts}]</span> '
                f'FILL <span style="color:{color}">{side}</span> '
                f'{rec.get("qty","")} {rec.get("symbol","")} '
                f'@ {rec.get("price","")}'
            )

        def _write_and_reload(_=None):
            self.db.api.insert_executions(records)
            return self.db.api.get_execution_log()

        self.db.run(_write_and_reload, callback=self._fill_exec_table)


# ════════════════════════════════════════════════════════════
#  TAB 3 — ORDER BOOK
#
#  Open orders from live sig_orders.
#  Fill history from order_book DB (same execution log).
#  No DB writes in this tab.
# ════════════════════════════════════════════════════════════

class OrderBookTab(QWidget):
    def __init__(self, db: DbBridge, worker: IBKRWorker) -> None:
        super().__init__()
        self.db     = db
        self.worker = worker
        self._build()
        self._load_history()

    def _build(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)

        bar = QHBoxLayout()
        rb  = _btn("↺  Refresh", AC)
        cb  = _btn("✕  Cancel Selected", R)
        rb.clicked.connect(self._refresh)
        cb.clicked.connect(self._cancel)
        bar.addWidget(rb)
        bar.addWidget(cb)
        bar.addStretch()
        root.addLayout(bar)

        split = QSplitter(Qt.Orientation.Vertical)

        og  = QGroupBox("Open Orders")
        ol  = QVBoxLayout(og)
        self._open = _tbl(["ID", "Symbol", "Action", "Qty",
                            "Type", "Limit", "Stop", "Trail%", "Status"])
        ol.addWidget(self._open)
        split.addWidget(og)

        hg  = QGroupBox("Fill History  (from DB)")
        hl  = QVBoxLayout(hg)
        fr  = QHBoxLayout()
        fr.addWidget(QLabel("Filter:"))
        self._flt = QLineEdit()
        self._flt.setPlaceholderText("symbol")
        self._flt.setMaximumWidth(140)
        fb = _btn("Go")
        fb.clicked.connect(self._filter)
        fr.addWidget(self._flt)
        fr.addWidget(fb)
        fr.addStretch()
        hl.addLayout(fr)
        self._hist = _tbl(["Time", "Symbol", "Side", "Qty",
                            "Price", "Comm.", "CCY", "Type"])
        hl.addWidget(self._hist)
        split.addWidget(hg)

        split.setStretchFactor(0, 1)
        split.setStretchFactor(1, 2)
        root.addWidget(split, 1)

    def _refresh(self) -> None:
        _p("OrderBook", "Refresh → requesting orders")
        self.worker.request("orders")

    def _cancel(self) -> None:
        sel = self._open.selectedItems()
        if not sel:
            return
        oid = self._open.item(self._open.row(sel[0]), 0)
        if oid:
            _p("OrderBook", f"Cancel order_id={oid.text()}")
            self.worker.request("cancel_order", order_id=int(oid.text()))

    def _filter(self) -> None:
        sym = self._flt.text().strip().upper()
        if sym:
            self.db.run(self.db.api.get_executions_for_symbol, sym,
                        callback=self._fill_hist)
        else:
            self._load_history()

    def _load_history(self) -> None:
        self.db.run(self.db.api.get_execution_log, 200,
                    callback=self._fill_hist)

    # ── Fill helpers ─────────────────────────────────────────
    def _fill_open(self, trades: list) -> None:
        self._open.setRowCount(len(trades))
        for i, t in enumerate(trades):
            o = t.order
            s = t.orderStatus
            vals = [
                str(o.orderId),
                t.contract.symbol,
                o.action,
                f"{o.totalQuantity:.0f}",
                o.orderType,
                f"{o.lmtPrice:.4f}"       if getattr(o, "lmtPrice",       0) else "—",
                f"{o.auxPrice:.4f}"       if getattr(o, "auxPrice",        0) else "—",
                f"{o.trailingPercent:.1f}" if getattr(o, "trailingPercent", 0) else "—",
                s.status,
            ]
            for j, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                if j == 2:
                    it.setForeground(QColor(G if v == "BUY" else R))
                self._open.setItem(i, j, it)
        self._open.resizeColumnsToContents()

    def _fill_hist(self, rows) -> None:
        self._hist.setRowCount(len(rows))
        for i, r in enumerate(rows):
            side = r["side"] or ""
            vals = [
                (r["recorded_at"] or "")[:16],
                r["symbol"] or "",
                side,
                f"{r['qty']:.0f}"        if r["qty"]       is not None else "—",
                f"{r['price']:.4f}"      if r["price"]     is not None else "—",
                f"{r['commission']:.4f}" if r["commission"] is not None else "—",
                r["currency"]   or "—",
                r["order_type"] or "—",
            ]
            for j, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                if j == 2:
                    it.setForeground(QColor(G if side == "BUY" else R))
                self._hist.setItem(i, j, it)
        self._hist.resizeColumnsToContents()

    # ── Signal handlers ───────────────────────────────────────
    def on_orders(self, trades: list) -> None:
        _p("OrderBook", f"[SIGNAL] sig_orders: {len(trades)} trades")
        self._fill_open(trades)   # no DB write

    def on_order_placed(self, _: dict) -> None:
        _p("OrderBook", "[SIGNAL] sig_order_placed → refreshing open orders")
        self.worker.request("orders")

    def on_executions(self, _: list) -> None:
        """Refresh fill history whenever new executions arrive."""
        self._load_history()


# ════════════════════════════════════════════════════════════
#  TAB 4 — CONTRACTS
#
#  Full details matching old format_contract_info /
#  print_formatted_contract_info output including:
#  trading/liquid hours, valid exchanges, order types.
#  No DB writes.
# ════════════════════════════════════════════════════════════

class ContractsTab(QWidget):
    def __init__(self, db: DbBridge, worker: IBKRWorker) -> None:
        super().__init__()
        self.db     = db
        self.worker = worker
        self._build()

    def _build(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)

        # ── Left: lookup form ────────────────────────────────
        left = QVBoxLayout()
        lu   = QGroupBox("Lookup")
        lu.setMaximumWidth(270)
        ll   = QVBoxLayout(lu)
        ll.setSpacing(6)
        self._sym   = QLineEdit()
        self._sym.setPlaceholderText("e.g. AIR")
        self._stype = QComboBox()
        self._stype.addItems(["STK", "FUT", "CASH", "CMDTY", "BOND"])
        self._exch  = QLineEdit("SMART")
        self._cur   = QLineEdit("EUR")
        ll.addLayout(_row("Symbol",   self._sym,   70))
        ll.addLayout(_row("Sec Type", self._stype, 70))
        ll.addLayout(_row("Exchange", self._exch,  70))
        ll.addLayout(_row("Currency", self._cur,   70))
        ib = _btn("Request Info",  AC)
        fb = _btn("Fundamentals", INF)
        ib.clicked.connect(self._fetch)
        fb.clicked.connect(self._fetch_fund)
        ll.addWidget(ib)
        ll.addWidget(fb)
        ll.addStretch()
        left.addWidget(lu, 1)
        root.addLayout(left)

        # ── Right: details + fundamentals panels ─────────────
        right = QSplitter(Qt.Orientation.Vertical)
        dg = QGroupBox("Details")
        self._det = QVBoxLayout(dg)
        self._det.addWidget(_dim_lbl("Look up a contract to see details."))
        right.addWidget(dg)
        fg = QGroupBox("Fundamentals")
        self._fun = QVBoxLayout(fg)
        self._fun.addWidget(_dim_lbl("Click 'Fundamentals' to fetch data."))
        right.addWidget(fg)
        right.setStretchFactor(0, 1)
        right.setStretchFactor(1, 2)
        root.addWidget(right, 1)

    def _contract(self) -> Contract:
        c = Contract()
        c.symbol   = self._sym.text().strip().upper()
        c.secType  = self._stype.currentText()
        c.exchange = self._exch.text().strip()
        c.currency = self._cur.text().strip()
        return c

    def _fetch(self) -> None:
        if self._sym.text().strip():
            _p("Contracts", f"Request Info: {self._sym.text().strip()}")
            self.worker.request("contract", contract=self._contract())

    def _fetch_fund(self) -> None:
        if self._sym.text().strip():
            _p("Contracts", f"Fundamentals: {self._sym.text().strip()}")
            self.worker.request("fundamentals", contract=self._contract())

    # ── Signal handlers ───────────────────────────────────────
    def on_contract(self, info: dict) -> None:
        _p("Contracts", f"[SIGNAL] sig_contract: {info.get('symbol')}")
        fmt = format_contract_info(info)

        # Format complex fields as readable strings
        def _fmt_hrs(sessions) -> str:
            if not sessions:
                return "N/A"
            return " | ".join(
                f"{s['date'][:10]}: {s['start']}–{s['end']}"
                for s in sessions[:5]
            )

        exchanges = fmt.get("Valid Exchanges", [])
        exc_str   = ", ".join(exchanges[:6])
        if len(exchanges) > 6:
            exc_str += f"  (+{len(exchanges)-6} more)"

        order_types = fmt.get("Order Types", [])
        common_ot   = [o for o in
                       ["LMT", "MKT", "STP", "STPLMT", "TRAIL", "MOC", "LOC"]
                       if o in order_types]
        ot_str = ", ".join(common_ot) + f"  (total: {len(order_types)})"

        display = {
            "Contract ID":       fmt.get("Contract ID"),
            "Symbol":            fmt.get("Symbol"),
            "Company":           fmt.get("Company"),
            "Primary Exchange":  fmt.get("Primary Exchange"),
            "Currency":          fmt.get("Currency"),
            "Category":          fmt.get("Category"),
            "Timezone":          fmt.get("Timezone"),
            "Min Tick":          fmt.get("Min Tick"),
            "Trading Hours":     _fmt_hrs(fmt.get("Trading Hours", [])),
            "Liquid Hours":      _fmt_hrs(fmt.get("Liquid Hours", [])),
            "Valid Exchanges":   exc_str,
            "Order Types":       ot_str,
            "Hist. Data Since":  fmt.get("Historical Data Since"),
        }
        _replace(self._det, _kv(display))
        # No DB write

    def on_fundamentals(self, f: dict) -> None:
        _p("Contracts", f"[SIGNAL] sig_fundamentals: {f.get('symbol')}")
        _replace(self._fun, _kv({
            k: (f"{v:.4f}" if isinstance(v, float) else str(v))
            for k, v in f.items()
            if k not in {"business_summary", "financial_summary"}
        }))
        # No DB write


# ════════════════════════════════════════════════════════════
#  TAB 5 — PRICES
#
#  Historical bars + live snapshot.
#  Matches old DataManager output.
#  No DB writes.
# ════════════════════════════════════════════════════════════

class PricesTab(QWidget):
    def __init__(self, db: DbBridge, worker: IBKRWorker) -> None:
        super().__init__()
        self.db     = db
        self.worker = worker
        self._build()

    def _build(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        bar = QHBoxLayout()
        self._sym   = QLineEdit()
        self._sym.setPlaceholderText("Symbol")
        self._sym.setMaximumWidth(90)
        self._stype = QComboBox()
        self._stype.addItems(["STK", "FUT", "CASH", "CMDTY"])
        self._stype.setMaximumWidth(70)
        self._exch  = QLineEdit("SMART")
        self._exch.setMaximumWidth(80)
        self._cur   = QLineEdit("EUR")
        self._cur.setMaximumWidth(60)
        self._dur   = QComboBox()
        self._dur.addItems(["5 D", "1 M", "3 M", "6 M", "1 Y", "2 Y", "5 Y"])
        self._dur.setCurrentIndex(2)
        self._dur.setMaximumWidth(70)
        self._bsz   = QComboBox()
        self._bsz.addItems(["1 min","5 mins","15 mins","30 mins",
                             "1 hour","4 hours","1 day","1 week","1 month"])
        self._bsz.setCurrentIndex(6)
        self._bsz.setMaximumWidth(90)
        for lbl, w in [("Symbol", self._sym), ("Type", self._stype),
                       ("Exch",   self._exch), ("CCY",  self._cur),
                       ("Dur",    self._dur),  ("Bar",  self._bsz)]:
            bar.addWidget(QLabel(lbl))
            bar.addWidget(w)
        hb = _btn("Historical", AC)
        sb = _btn("Snapshot",   INF)
        hb.clicked.connect(self._hist)
        sb.clicked.connect(self._snap)
        bar.addWidget(hb)
        bar.addWidget(sb)
        bar.addStretch()
        root.addLayout(bar)

        split = QSplitter(Qt.Orientation.Vertical)
        cg  = QGroupBox("Price Chart")
        cgl = QVBoxLayout(cg)
        self._chart = PriceChart()
        cgl.addWidget(self._chart)
        split.addWidget(cg)

        bot = QSplitter(Qt.Orientation.Horizontal)
        sg  = QGroupBox("Live Snapshot")
        self._snp = QVBoxLayout(sg)
        self._snp.addWidget(_dim_lbl("Request a snapshot to see live data."))
        bot.addWidget(sg)
        bg  = QGroupBox("Historical Bars  (last 50)")
        bgl = QVBoxLayout(bg)
        self._bars = _tbl(["Date", "Open", "High", "Low", "Close", "Volume", "WAP"])
        bgl.addWidget(self._bars)
        bot.addWidget(bg)
        split.addWidget(bot)

        split.setStretchFactor(0, 3)
        split.setStretchFactor(1, 2)
        root.addWidget(split, 1)

    def _contract(self) -> Contract:
        c = Contract()
        c.symbol   = self._sym.text().strip().upper()
        c.secType  = self._stype.currentText()
        c.exchange = self._exch.text().strip()
        c.currency = self._cur.text().strip()
        return c

    def _hist(self) -> None:
        sym = self._sym.text().strip()
        if sym:
            _p("Prices", f"Historical: {sym} {self._dur.currentText()} {self._bsz.currentText()}")
            self.worker.request("hist_data", contract=self._contract(),
                                duration=self._dur.currentText(),
                                bar_size=self._bsz.currentText())

    def _snap(self) -> None:
        sym = self._sym.text().strip()
        if sym:
            _p("Prices", f"Snapshot: {sym}")
            self.worker.request("snapshot", contract=self._contract())

    def _fill_bars(self, df: pd.DataFrame) -> None:
        tail = df.tail(50)
        self._bars.setRowCount(len(tail))
        for i, (_, r) in enumerate(tail.iterrows()):
            for j, col in enumerate(["date","open","high","low","close","volume","wap"]):
                val  = r.get(col, "")
                if col == "volume" and val:
                    text = f"{int(val):,}"
                elif isinstance(val, float):
                    text = f"{val:.4f}"
                else:
                    text = str(val)[:16]
                self._bars.setItem(i, j, QTableWidgetItem(text))
        self._bars.resizeColumnsToContents()

    # ── Signal handlers ───────────────────────────────────────
    def on_hist_data(self, df, symbol: str, bar_size: str) -> None:
        _p("Prices", f"[SIGNAL] sig_hist_data: {symbol}, {len(df)} bars @ {bar_size}")
        self._chart.plot(df, symbol, bar_size)
        self._fill_bars(df)
        # No DB write

    def on_snapshot(self, data: dict, symbol: str) -> None:
        _p("Prices", f"[SIGNAL] sig_snapshot: {symbol} bid={data.get('bid')} ask={data.get('ask')}")
        bid = data.get("bid")
        ask = data.get("ask")
        mid = f"{(bid + ask) / 2:.4f}" if bid and ask else "—"
        _replace(self._snp, _kv({
            "Symbol":  symbol,
            "Bid":     str(bid  or "—"),
            "Ask":     str(ask  or "—"),
            "Last":    str(data.get("last",   "—")),
            "Open":    str(data.get("open",   "—")),
            "High":    str(data.get("high",   "—")),
            "Low":     str(data.get("low",    "—")),
            "Close":   str(data.get("close",  "—")),
            "Volume":  str(data.get("volume", "—")),
            "Mid":     mid,
            "Time":    datetime.now().strftime("%H:%M:%S"),
        }))
        # No DB write


# ════════════════════════════════════════════════════════════
#  TAB 6 — SCANNERS
#
#  Live scanner results.  No DB writes.
# ════════════════════════════════════════════════════════════

_CODES = ["TOP_PERC_GAIN", "TOP_PERC_LOSE", "MOST_ACTIVE", "HOT_BY_VOLUME",
          "HOT_BY_OPT_VOLUME", "HIGH_VS_13W_HL", "LOW_VS_13W_HL",
          "HIGH_OPT_IMPL_VOLATILITY"]
_LOCS  = ["STK.EU.MAJOR", "STK.EU", "STK.US.MAJOR", "STK.US",
          "STK.US.NYSE", "STK.US.NASDAQ", "STK.HK"]


class ScannersTab(QWidget):
    def __init__(self, db: DbBridge, worker: IBKRWorker) -> None:
        super().__init__()
        self.db     = db
        self.worker = worker
        self._build()

    def _build(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)

        cfg = QGroupBox("Config")
        cfg.setMaximumWidth(300)
        cl  = QVBoxLayout(cfg)
        cl.setSpacing(6)

        self._code = QComboBox()
        self._code.setEditable(True)
        self._code.addItems(_CODES)
        self._loc  = QComboBox()
        self._loc.setEditable(True)
        self._loc.addItems(_LOCS)
        self._inst = QComboBox()
        self._inst.addItems(["STK", "ETF", "FUT"])
        self._nrow = QSpinBox()
        self._nrow.setRange(5, 50)
        self._nrow.setValue(15)
        self._pa   = QDoubleSpinBox()
        self._pa.setRange(0, 10000)
        self._pa.setSpecialValueText("—")
        self._pb   = QDoubleSpinBox()
        self._pb.setRange(0, 10000)
        self._pb.setValue(10000)
        self._pb.setSpecialValueText("—")
        self._va   = QDoubleSpinBox()
        self._va.setRange(0, 1e9)
        self._va.setValue(1_000_000)
        self._va.setDecimals(0)
        self._va.setSuffix(" vol")

        cl.addLayout(_row("Scan Code",  self._code, 85))
        cl.addLayout(_row("Location",   self._loc,  85))
        cl.addLayout(_row("Instrument", self._inst, 85))
        cl.addLayout(_row("Rows",       self._nrow, 85))
        cl.addWidget(_div())
        cl.addLayout(_row("Price Above", self._pa, 85))
        cl.addLayout(_row("Price Below", self._pb, 85))
        cl.addLayout(_row("Vol Above",   self._va, 85))
        rb = _btn("▶  Run Scanner", G)
        rb.clicked.connect(self._run)
        cl.addWidget(rb)
        cl.addStretch()
        root.addWidget(cfg)

        rg  = QGroupBox("Results")
        rl  = QVBoxLayout(rg)
        self._res = _tbl(["#", "Symbol", "Type", "Exchange",
                           "CCY", "Distance", "Benchmark", "Projection", "Time"])
        rl.addWidget(self._res)
        root.addWidget(rg, 1)

    def _run(self) -> None:
        filters = []
        if self._pa.value() > 0:
            filters.append(TagValue("priceAbove",  str(self._pa.value())))
        if self._pb.value() < 10000:
            filters.append(TagValue("priceBelow",  str(self._pb.value())))
        if self._va.value() > 0:
            filters.append(TagValue("volumeAbove", str(int(self._va.value()))))
        code = self._code.currentText()
        loc  = self._loc.currentText()
        _p("Scanners", f"Run: {code} @ {loc}, rows={self._nrow.value()}")
        self.worker.request("scanner",
                            scan_code=code, location=loc,
                            instrument=self._inst.currentText(),
                            num_rows=self._nrow.value(),
                            filters=filters)

    def _fill(self, rows) -> None:
        self._res.setRowCount(len(rows))
        for i, r in enumerate(rows):
            vals = [
                str(r.get("rank") or i + 1),
                r.get("symbol",     ""),
                r.get("sec_type",   ""),
                r.get("exchange",   ""),
                r.get("currency",   ""),
                r.get("distance",   ""),
                r.get("benchmark",  ""),
                r.get("projection", ""),
                r.get("run_ts",     ""),
            ]
            for j, v in enumerate(vals):
                it = QTableWidgetItem(str(v))
                if j == 1:
                    it.setForeground(QColor(AC))
                self._res.setItem(i, j, it)
        self._res.resizeColumnsToContents()

    # ── Signal handler ────────────────────────────────────────
    def on_scanner(self, results: list, scan_code: str, location: str) -> None:
        _p("Scanners", f"[SIGNAL] sig_scanner: {scan_code} @ {location}, {len(results)} results")
        ts   = datetime.now().strftime("%H:%M:%S")
        rows = []
        for item in results:
            cd = getattr(item, "contractDetails", None)
            c  = cd.contract if cd else None
            rows.append({
                "rank":       getattr(item, "rank",       0),
                "symbol":     c.symbol   if c else "",
                "sec_type":   c.secType  if c else "",
                "exchange":   c.exchange if c else "",
                "currency":   c.currency if c else "",
                "distance":   str(getattr(item, "distance",   "")),
                "benchmark":  str(getattr(item, "benchmark",  "")),
                "projection": str(getattr(item, "projection", "")),
                "run_ts":     ts,
            })
        self._fill(rows)
        # No DB write


# ════════════════════════════════════════════════════════════
#  MAIN WINDOW
# ════════════════════════════════════════════════════════════

GATEWAY_WAIT = 30


class MainWindow(QMainWindow):
    def __init__(self, cfg: ConnectionConfig) -> None:
        super().__init__()
        self.cfg = cfg
        self.setWindowTitle("IBKR Dashboard")
        self.resize(1300, 820)

        self.db     = DbBridge(get_db())
        self.worker = IBKRWorker(cfg)

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self._banner = QLabel("  Initialising…")
        self._banner.setFixedHeight(26)
        self._banner.setStyleSheet(
            f"background:{BG}; color:{DM}; font-size:11px; padding-left:10px;"
        )
        root.addWidget(self._banner)

        tabs = QTabWidget()
        root.addWidget(tabs, 1)

        self.t_port = PortfolioTab(self.db, self.worker)
        self.t_exec = ExecutionTab(self.db, self.worker)
        self.t_book = OrderBookTab(self.db, self.worker)
        self.t_cont = ContractsTab(self.db, self.worker)
        self.t_pric = PricesTab(self.db, self.worker)
        self.t_scan = ScannersTab(self.db, self.worker)

        tabs.addTab(self.t_port, "Portfolio")
        tabs.addTab(self.t_exec, "Execution")
        tabs.addTab(self.t_book, "Order Book")
        tabs.addTab(self.t_cont, "Contracts")
        tabs.addTab(self.t_pric, "Prices")
        tabs.addTab(self.t_scan, "Scanners")

        # ── Signal wiring ─────────────────────────────────────
        self.worker.sig_portfolio.connect(self.t_port.on_portfolio)
        self.worker.sig_account.connect(self.t_port.on_account)
        self.worker.sig_orders.connect(self.t_book.on_orders)
        self.worker.sig_order_placed.connect(self.t_exec.on_order_placed)
        self.worker.sig_order_placed.connect(self.t_book.on_order_placed)
        self.worker.sig_executions.connect(self.t_exec.on_executions)   # fills → DB
        self.worker.sig_executions.connect(self.t_book.on_executions)   # fills → history
        self.worker.sig_contract.connect(self.t_cont.on_contract)
        self.worker.sig_fundamentals.connect(self.t_cont.on_fundamentals)
        self.worker.sig_hist_data.connect(self.t_pric.on_hist_data)
        self.worker.sig_snapshot.connect(self.t_pric.on_snapshot)
        self.worker.sig_scanner.connect(self.t_scan.on_scanner)
        self.worker.sig_status.connect(self._on_status)
        self.worker.sig_error.connect(self._on_error)
        self.worker.sig_connected.connect(self._on_connected)

        self._gw_done   = False
        self._countdown = 0
        self._gw_timer  = QTimer(self)
        self._gw_timer.timeout.connect(self._gw_tick)
        self._start_gateway()

    # ── Banner ────────────────────────────────────────────────
    def _set_banner(self, text: str, color: str = DM) -> None:
        self._banner.setText(f"  {text}")
        self._banner.setStyleSheet(
            f"background:{BG}; color:{color}; font-size:11px; padding-left:10px;"
        )

    def _on_status(self, msg: str) -> None:
        _p("Worker", f"Status → {msg}")
        self._set_banner(msg)

    def _on_error(self, msg: str) -> None:
        _p("Worker", f"Error → {msg}")
        self._set_banner(f"⚠  {msg}", R)

    def _on_connected(self, state: bool) -> None:
        _p("Worker", f"Connected = {state}")
        self._set_banner("●  Connected to IBKR" if state
                         else "○  Disconnected from IBKR",
                         G if state else R)

    # ── Gateway startup ───────────────────────────────────────
    def _start_gateway(self) -> None:
        if self.cfg.ibc_path and GATEWAY_WAIT > 0:
            _p("Main", f"Starting gateway via IBC ({GATEWAY_WAIT}s wait)")
            self._countdown = GATEWAY_WAIT
            self._gw_done   = False
            threading.Thread(
                target=self._gw_thread,
                args=(GATEWAY_WAIT,),
                daemon=True,
            ).start()
            self._gw_timer.start(1000)
            self._set_banner(f"Starting IB Gateway… {self._countdown}s")
        else:
            _p("Main", "GATEWAY_WAIT=0 or no IBC path — starting worker immediately")
            self._set_banner("Connecting to IBKR…", DM)
            self.worker.start()

    def _gw_thread(self, wait: int) -> None:
        self.cfg.start_gateway(wait)
        self._gw_done = True

    def _gw_tick(self) -> None:
        self._countdown -= 1
        if self._gw_done or self._countdown <= 0:
            self._gw_timer.stop()
            self._set_banner("Connecting to IBKR…", DM)
            self.worker.start()
        else:
            self._set_banner(f"Starting IB Gateway… {self._countdown}s")

    # ── Close ─────────────────────────────────────────────────
    def closeEvent(self, a0: QCloseEvent | None) -> None:
        _p("Main", "closeEvent — shutting down")
        self._gw_timer.stop()
        ib   = self.worker._ib
        loop = self.worker.loop
        if ib and loop and loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(
                    self._shutdown(ib), loop
                ).result(timeout=10)
            except Exception as e:
                _p("Main", f"Shutdown error: {e}")
        elif ib:
            try:
                ib.disconnect()
            except Exception:
                pass
        self.cfg.stop_gateway()
        self.db.close()
        _p("Main", "Cleanup done")
        if a0:
            a0.accept()

    async def _shutdown(self, ib) -> None:
        try:
            ib.disconnect()
        except Exception:
            pass


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