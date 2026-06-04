# ============================================================
#  ibkr_gui.py  —  PyQt6 GUI layer
# ============================================================
#
#  All Qt classes live here; run.py is a pure pipeline
#  launcher that imports MainWindow and nothing else.
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
import threading
from datetime import datetime
from typing import Any, Callable

import pandas as pd

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTableWidget, QTableWidgetItem, QLabel, QPushButton,
    QLineEdit, QComboBox, QDoubleSpinBox, QSpinBox, QHeaderView,
    QSizePolicy, QGroupBox, QSplitter, QFrame, QTextEdit,
)
from PyQt6.QtCore import Qt, QTimer, QRunnable, QThreadPool, QObject, pyqtSignal
from PyQt6.QtGui import QColor, QCloseEvent, QFont

import matplotlib
matplotlib.use("QtAgg")
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from ibkr_database import DatabaseManager, get_db
from ibkr_client import IBKRWorker, ConnectionConfig, format_contract_info, Contract, TagValue


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
    """
    Minimal, matplotlib-native chart.
    Two subplots: price (top, 70%) + volume (bottom, 30%).
    Uses only spine/tick/grid primitives — no custom patches.
    """

    _PRICE_RATIO = 0.70
    _LABEL_SIZE  = 8
    _TICK_COLOR  = "#666666"
    _SPINE_COLOR = "#2a2a2a"
    _GRID_COLOR  = "#1e1e1e"

    def __init__(self) -> None:
        self.fig = Figure(facecolor=BG)
        super().__init__(self.fig)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._ax: Any  = None
        self._axv: Any = None
        self._build_axes()

    # ── Layout ────────────────────────────────────────────────

    def _build_axes(self) -> None:
        self.fig.clear()
        import matplotlib.gridspec as gridspec
        r  = self._PRICE_RATIO
        gs = gridspec.GridSpec(
            2, 1,
            figure        = self.fig,
            height_ratios = [r, 1 - r],
            hspace        = 0.0,
            left=0.10, right=0.97, top=0.91, bottom=0.13,
        )
        self._ax  = self.fig.add_subplot(gs[0])
        self._axv = self.fig.add_subplot(gs[1], sharex=self._ax)

    def _style_ax(self, ax: Any, show_xticklabels: bool = False) -> None:
        ax.set_facecolor(BG)
        ax.tick_params(
            axis="both", colors=self._TICK_COLOR,
            labelsize=self._LABEL_SIZE, length=3, pad=4,
        )
        ax.tick_params(axis="x", labelbottom=show_xticklabels)
        for side, sp in ax.spines.items():
            if side in ("top", "right"):
                sp.set_visible(False)
            else:
                sp.set_color(self._SPINE_COLOR)
                sp.set_linewidth(0.8)
        ax.grid(True, axis="y",
                color=self._GRID_COLOR, linewidth=0.5, linestyle="-", alpha=1.0)
        ax.yaxis.set_tick_params(labelsize=self._LABEL_SIZE, colors=self._TICK_COLOR)

    # ── Price + Volume plot ───────────────────────────────────

    def plot(self, df: pd.DataFrame, symbol: str = "", bar_size: str = "") -> None:
        if df is None or df.empty:
            return

        self._build_axes()
        ax, axv = self._ax, self._axv

        import matplotlib.dates  as mdates
        import matplotlib.ticker as ticker
        import numpy as np

        x     = pd.to_datetime(df["date"] if "date" in df.columns else df.index)
        close = df["close"].to_numpy(dtype=float)
        open_ = df["open"].to_numpy(dtype=float)

        # ── Price line ──────────────────────────────────────
        ax.plot(x, close, color=AC, linewidth=1.2, zorder=3)
        ax.fill_between(x, close, close.min(), color=AC, alpha=0.06, zorder=2)
        ax.set_ylabel("Price", fontsize=self._LABEL_SIZE,
                      color=self._TICK_COLOR, labelpad=4)
        title = f"{symbol}   {bar_size}" if symbol else bar_size
        ax.set_title(title, color=TX, fontsize=9, pad=5, loc="left",
                     fontfamily="monospace")

        # ── Volume bars ─────────────────────────────────────
        if "volume" in df.columns:
            vol    = df["volume"].to_numpy(dtype=float)
            colors = np.where(close >= open_, G, R)
            try:
                bar_w = np.timedelta64(int(0.6 * 86400), "s")
                axv.bar(x, vol, color=colors, width=bar_w, alpha=0.72, zorder=2)
            except Exception:
                axv.bar(range(len(vol)), vol, color=colors, alpha=0.72, zorder=2)
            axv.yaxis.set_major_formatter(
                ticker.FuncFormatter(
                    lambda v, _: (f"{v/1e6:.1f}M" if v >= 1e6
                                  else f"{v/1e3:.0f}K" if v >= 1e3
                                  else f"{v:.0f}")
                )
            )
            axv.set_ylabel("Vol", fontsize=self._LABEL_SIZE,
                           color=self._TICK_COLOR, labelpad=4)

        # ── X-axis date formatting ───────────────────────────
        span_days = int((x.max() - x.min()).days) if len(x) > 1 else 1
        if span_days <= 10:
            axv.xaxis.set_major_locator(mdates.DayLocator())
            axv.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        elif span_days <= 90:
            axv.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
            axv.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        elif span_days <= 400:
            axv.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            axv.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        else:
            axv.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            axv.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))

        self.fig.autofmt_xdate(rotation=30, ha="right")
        self._style_ax(ax,  show_xticklabels=False)
        self._style_ax(axv, show_xticklabels=True)
        self.draw()

    # ── Net Liquidation equity curve ─────────────────────────

    def plot_equity(self, dates: list, values: list, title: str) -> None:
        self._build_axes()
        ax, axv = self._ax, self._axv

        if not dates:
            self._style_ax(ax)
            self._style_ax(axv)
            self.draw()
            return

        import matplotlib.dates  as mdates
        import matplotlib.ticker as ticker

        x = pd.to_datetime(dates)
        v = [float(vv) for vv in values]

        ax.plot(x, v, color=G, linewidth=1.2, zorder=3)
        ax.fill_between(x, v, min(v) * 0.998, color=G, alpha=0.07, zorder=2)
        ax.set_title(title, color=TX, fontsize=9, pad=5, loc="left",
                     fontfamily="monospace")
        ax.set_ylabel("Value", fontsize=self._LABEL_SIZE,
                      color=self._TICK_COLOR, labelpad=4)
        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(
                lambda val, _: (f"{val/1e6:.2f}M" if abs(val) >= 1e6
                                else f"{val/1e3:.0f}K" if abs(val) >= 1e3
                                else f"{val:.0f}")
            )
        )

        axv.set_visible(False)

        span_days = int((x.max() - x.min()).days) if len(x) > 1 else 1
        if span_days <= 90:
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        else:
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))

        self.fig.autofmt_xdate(rotation=30, ha="right")
        self._style_ax(ax, show_xticklabels=True)
        self.draw()


# ════════════════════════════════════════════════════════════
#  TAB 1 — PORTFOLIO
# ════════════════════════════════════════════════════════════

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
                lbl.setStyleSheet(f"color:{DM}; font-size:10px; font-weight:500;")
                lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
                val = QLabel("—")
                val.setStyleSheet(f"color:{TX}; font-weight:600; font-size:13px;")
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

    def _refresh(self) -> None:
        _p("Portfolio", "Refresh → requesting portfolio + account")
        self.worker.request("portfolio")
        self.worker.request("account")

    def _load_from_db(self) -> None:
        _p("Portfolio", "Loading account data from DB (async)")
        self.db.run(self.db.api.get_latest_account,
                    callback=self._on_acc_loaded)
        self.db.run(self.db.api.get_account_history, 120,
                    callback=self._on_hist_loaded)

    def _on_acc_loaded(self, row) -> None:
        if row:
            _p("Portfolio", "Filling account cards from DB")
            self._fill_acc(_db_row_to_acct(row))

    def _on_hist_loaded(self, rows) -> None:
        if not rows:
            return
        s = list(reversed([r["recorded_at"]          for r in rows]))
        v = list(reversed([r["net_liquidation"] or 0  for r in rows]))
        _p("Portfolio", f"Plotting equity curve: {len(s)} points")
        self._chart.plot_equity(s, v, "Net Liquidation")

    def _fill_pos(self, items: list) -> None:
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
                if j == 7:
                    it.setForeground(QColor(G if (p.unrealizedPNL or 0) >= 0 else R))
                if j == 9:
                    it.setForeground(QColor(G if (p.unrealizedPNL or 0) >= 0 else R))
                self._pos.setItem(i, j, it)
        self._pos.resizeColumnsToContents()

    def _fill_acc(self, d: dict) -> None:
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

    def on_portfolio(self, items: list) -> None:
        _p("Portfolio", f"[SIGNAL] sig_portfolio: {len(items)} items")
        self._fill_pos(items)

    def on_account(self, d: dict) -> None:
        _p("Portfolio", f"[SIGNAL] sig_account: {list(d.keys())}")
        self._fill_acc(d)

        def _write_if_needed(_=None):
            if self.db.api.should_store_account():
                _p("Portfolio", "  → storing account_book row (≥1 day since last)")
                self.db.api.insert_account_book(d)
            return self.db.api.get_account_history(120)

        self.db.run(_write_if_needed, callback=self._on_hist_loaded)


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
# ════════════════════════════════════════════════════════════

_EXEC_COLS = ["Time", "Symbol", "Side", "Qty", "Price",
              "Comm.", "CCY", "Type", "Order ID", "Exec ID"]


class ExecutionTab(QWidget):
    def __init__(self, db: DbBridge, worker: IBKRWorker) -> None:
        super().__init__()
        self.db     = db
        self.worker = worker
        self._build()
        self.db.run(self.db.api.get_execution_log,
                    callback=self._fill_exec_table)

    def _build(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(12)

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
                if j == 2:
                    it.setForeground(QColor(G if side == "BUY" else R))
                self._exec_tbl.setItem(i, j, it)
        self._exec_tbl.resizeColumnsToContents()

    def on_order_placed(self, d: dict) -> None:
        _p("Execution", f"[SIGNAL] sig_order_placed: id={d.get('order_id')} status={d.get('status')}")
        ts    = datetime.now().strftime("%H:%M:%S")
        color = G if d["status"] == "Filled" else DM
        self._log.append(
            f'<span style="color:{DM}">[{ts}]</span> '
            f'Order <b>{d["order_id"]}</b> → '
            f'<span style="color:{color}">{d["status"]}</span>'
        )

    def on_executions(self, records: list) -> None:
        if not records:
            return
        _p("Execution",
           f"[SIGNAL] sig_executions: {len(records)} record(s) → order_book")
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

    def on_orders(self, trades: list) -> None:
        _p("OrderBook", f"[SIGNAL] sig_orders: {len(trades)} trades")
        self._fill_open(trades)

    def on_order_placed(self, _: dict) -> None:
        _p("OrderBook", "[SIGNAL] sig_order_placed → refreshing open orders")
        self.worker.request("orders")

    def on_executions(self, _: list) -> None:
        self._load_history()


# ════════════════════════════════════════════════════════════
#  TAB 4 — CONTRACTS
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

    def on_contract(self, info: dict) -> None:
        _p("Contracts", f"[SIGNAL] sig_contract: {info.get('symbol')}")
        fmt = format_contract_info(info)

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

    def on_fundamentals(self, f: dict) -> None:
        _p("Contracts", f"[SIGNAL] sig_fundamentals: {f.get('symbol')}")

        def _v(key: str, suffix: str = "", scale: float = 1.0,
               unit: str = "", decimals: int = 5) -> str:
            """Format a fundamentals value with optional scaling/unit."""
            raw = f.get(key, "N/A")
            if raw in (None, "N/A", ""):
                return "N/A"
            try:
                v = float(raw) * scale
                # Sentinel for missing data IBKR returns as -99999.99
                if abs(v) >= 99999.98 * scale:
                    return "N/A"
                if scale >= 1e6:
                    text = f"{v / 1e6:.5f} M"
                else:
                    text = f"{v:.{decimals}f}"
                if unit:
                    text += f" {unit}"
                if suffix:
                    text += suffix
                return text
            except (ValueError, TypeError):
                return str(raw)

        def _emp() -> str:
            emp = f.get("employees", "N/A")
            d   = f.get("employees_date", "")
            if emp in (None, "N/A"):
                return "N/A"
            txt = f"{int(float(emp)):,}" if emp != "N/A" else "N/A"
            return f"{txt}  (as of {d})" if d else txt

        def _shares(key: str, date_key: str = "shares_date") -> str:
            raw = f.get(key, "N/A")
            d   = f.get(date_key, "")
            if raw in (None, "N/A"):
                return "N/A"
            try:
                txt = f"{float(raw):,.1f}"
            except (ValueError, TypeError):
                txt = str(raw)
            return f"{txt}  (as of {d})" if d else txt

        def _split() -> str:
            s = f.get("recent_split", "N/A")
            d = f.get("split_date",   "")
            if s in (None, "N/A"):
                return "N/A"
            return f"{s}  on {d}" if d else str(s)

        def _rec() -> str:
            raw = f.get("analyst_recommendation", "N/A")
            if raw in (None, "N/A"):
                return "N/A"
            try:
                v = float(raw)
                if abs(v) >= 99999:
                    return "N/A"
                return f"{v:.5f}  (1=Strong Buy, 5=Sell)"
            except (ValueError, TypeError):
                return str(raw)

        ccy = f.get("currency", "USD")

        sections: list[tuple[str, dict]] = [
            ("Company Info", {
                "Symbol":       f.get("symbol",       "N/A"),
                "Company Name": f.get("company_name", "N/A"),
                "Exchange":     f.get("exchange",     "N/A"),
                "CIK Number":   f.get("cik",          "N/A"),
                "Employees":    _emp(),
                "Shares Out":   _shares("shares_outstanding"),
                "Float":        _shares("float", "shares_date"),
            }),
            ("Stock Split Info", {
                "Most Recent Split": _split(),
            }),
            ("Price & Volume", {
                "Current Price":    _v("current_price",  f" {ccy}", decimals=5),
                "52-Week High":     _v("52w_high",       f" {ccy}", decimals=5),
                "52-Week Low":      _v("52w_low",        f" {ccy}", decimals=5),
                "Avg Volume (10D)": _v("volume_10d_avg", " M",      scale=1e-6, decimals=5),
            }),
            ("Financial Summary (TTM)", {
                "Market Cap":       _v("market_cap",   f" M {ccy}", scale=1e-6, decimals=5),
                "Enterprise Value": _v("enterprise_value", f" M {ccy}", scale=1e-6, decimals=5),
                "Revenue":          _v("revenue_ttm",  f" M {ccy}", scale=1e-6, decimals=5),
                "EBITDA":           _v("ebitda_ttm",   f" M {ccy}", scale=1e-6, decimals=5),
                "Net Income":       _v("net_income_ttm", f" M {ccy}", scale=1e-6, decimals=5),
            }),
            ("Per Share Data (TTM)", {
                "EPS":             _v("eps_ttm",             f" {ccy}", decimals=5),
                "Revenue/Share":   _v("revenue_per_share",   f" {ccy}", decimals=5),
                "Book Value/Share":_v("book_value_per_share",f" {ccy}", decimals=5),
                "Cash/Share":      _v("cash_per_share",      f" {ccy}", decimals=5),
                "Cash Flow/Share": _v("cashflow_per_share",  f" {ccy}", decimals=5),
                "Dividend/Share":  _v("dividend_per_share",  f" {ccy}", decimals=5),
            }),
            ("Valuation Metrics", {
                "P/E Ratio":     _v("pe_ratio",       decimals=5),
                "Price/Book":    _v("price_to_book",  decimals=5),
                "Price/Sales":   _v("price_to_sales", decimals=5),
            }),
            ("Profitability", {
                "Gross Margin": _v("gross_margin", " %", decimals=5),
                "ROE":          _v("roe",          " %", decimals=5),
            }),
            ("Analyst Estimates", {
                "Recommendation":   _rec(),
                "Target Price":     _v("target_price",          f" {ccy}", decimals=5),
                "Projected Growth": _v("projected_growth_rate", " %",      decimals=4),
                "Projected EPS":    _v("projected_eps",         f" {ccy}", decimals=5),
                "Projected Sales":  _v("projected_sales",       f" M {ccy}", scale=1e-6, decimals=5),
                "Projected Profit": _v("projected_profit",      f" M {ccy}", scale=1e-6, decimals=5),
            }),
        ]

        # ── Build a scrollable widget with section groups ─────
        container = QWidget()
        vbox      = QVBoxLayout(container)
        vbox.setContentsMargins(4, 4, 4, 4)
        vbox.setSpacing(8)

        # Header: "SYMBOL  —  Company Name"
        sym  = f.get("symbol",       "")
        name = f.get("company_name", "")
        hdr  = QLabel(f"<b>{sym}</b>  —  {name}" if sym else name)
        hdr.setStyleSheet(f"color:{AC}; font-size:13px; font-weight:700; padding:4px 0;")
        vbox.addWidget(hdr)

        for section_title, rows in sections:
            grp = QGroupBox(section_title)
            grp.setStyleSheet(
                f"QGroupBox{{color:{DM};border:1px solid {BO};border-radius:5px;"
                f"margin-top:10px;padding:8px 6px 6px 6px}}"
                f"QGroupBox::title{{subcontrol-origin:margin;padding:0 6px;"
                f"color:{INF};font-weight:600;font-size:10px}}"
            )
            gl = QVBoxLayout(grp)
            gl.setSpacing(2)
            gl.setContentsMargins(4, 2, 4, 4)
            for label, value in rows.items():
                row_w = QWidget()
                rl    = QHBoxLayout(row_w)
                rl.setContentsMargins(0, 1, 0, 1)
                rl.setSpacing(6)
                lbl_w = QLabel(label + ":")
                lbl_w.setFixedWidth(150)
                lbl_w.setStyleSheet(f"color:{DM}; font-size:11px;")
                val_w = QLabel(str(value))
                val_w.setStyleSheet(f"color:{TX}; font-size:11px; font-weight:500;")
                val_w.setTextInteractionFlags(
                    Qt.TextInteractionFlag.TextSelectableByMouse)
                rl.addWidget(lbl_w)
                rl.addWidget(val_w, 1)
                gl.addWidget(row_w)
            vbox.addWidget(grp)

        # Business Summary
        summary = f.get("business_summary", "").strip() if f.get("business_summary") else ""
        if summary:
            grp = QGroupBox("Business Summary")
            grp.setStyleSheet(
                f"QGroupBox{{color:{DM};border:1px solid {BO};border-radius:5px;"
                f"margin-top:10px;padding:8px 6px 6px 6px}}"
                f"QGroupBox::title{{subcontrol-origin:margin;padding:0 6px;"
                f"color:{INF};font-weight:600;font-size:10px}}"
            )
            gl  = QVBoxLayout(grp)
            txt = QLabel(summary)
            txt.setWordWrap(True)
            txt.setStyleSheet(f"color:{TX}; font-size:11px; line-height:150%;")
            txt.setTextInteractionFlags(
                Qt.TextInteractionFlag.TextSelectableByMouse)
            gl.addWidget(txt)
            vbox.addWidget(grp)

        vbox.addStretch()

        # Wrap in a scroll area so long content doesn't get clipped
        from PyQt6.QtWidgets import QScrollArea
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(container)
        scroll.setStyleSheet(
            f"QScrollArea{{border:none;background:{SU}}}"
            f"QScrollBar:vertical{{background:{BG};width:5px;border:none}}"
            f"QScrollBar::handle:vertical{{background:{BO};border-radius:2px}}"
        )
        _replace(self._fun, scroll)


# ════════════════════════════════════════════════════════════
#  TAB 5 — PRICES
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

    def on_hist_data(self, df, symbol: str, bar_size: str) -> None:
        _p("Prices", f"[SIGNAL] sig_hist_data: {symbol}, {len(df)} bars @ {bar_size}")
        self._chart.plot(df, symbol, bar_size)
        self._fill_bars(df)

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


# ════════════════════════════════════════════════════════════
#  TAB 6 — SCANNERS
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
        self.worker.sig_executions.connect(self.t_exec.on_executions)
        self.worker.sig_executions.connect(self.t_book.on_executions)
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
#  Qt APPLICATION FACTORY
# ════════════════════════════════════════════════════════════

def build_app(cfg: ConnectionConfig) -> tuple[QApplication, MainWindow]:
    """
    Create and configure the QApplication + MainWindow.
    Called by run.py; separated here so tests can import GUI
    components without triggering QApplication construction.
    """
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")
    app.setStyleSheet(STYLE)
    win = MainWindow(cfg)
    return app, win