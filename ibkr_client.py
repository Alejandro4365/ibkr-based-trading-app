# ============================================================
#  ibkr_client.py  —  IBKR async worker thread
# ============================================================

from __future__ import annotations

import asyncio
import math
import os
import subprocess
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime

from PyQt5.QtCore import QThread, pyqtSignal

from ib_insync import (
    IB, Contract, ScannerSubscription,
    MarketOrder, LimitOrder, Order as IBOrder, util,
)

TIMEOUT = 10   # seconds for most requests


# ════════════════════════════════════════════════════════════
#  CONNECTION CONFIG
# ════════════════════════════════════════════════════════════

@dataclass
class ConnectionConfig:
    host:      str = field(default_factory=lambda: os.getenv("API_HOST",       "127.0.0.1"))
    port:      int = field(default_factory=lambda: int(os.getenv("API_PORT",   "4002")))
    client_id: int = field(default_factory=lambda: int(os.getenv("API_CLIENT_ID", "125")))
    # FIX: Raw string r"..." prevents SyntaxWarning on escape sequences
    ibc_path:  str = field(default_factory=lambda: os.getenv("IBC_PATH",       r"C:\Jts\IBC"))

    def start_gateway(self, wait_seconds: int = 30) -> None:
        if not self.ibc_path:
            print("[Gateway] IBC_PATH not set – skipping.")
            return
        print(f"[Gateway] Starting via IBC at {self.ibc_path}…")
        try:
            subprocess.Popen(
                [rf"{self.ibc_path}\StartGateway.bat"],
                cwd=self.ibc_path, shell=True,
            )
            print(f"[Gateway] Waiting {wait_seconds}s for services…")
            time.sleep(wait_seconds)
            print("[Gateway] Ready.")
        except Exception as exc:
            print(f"[Gateway] Launch error: {exc}")

    def stop_gateway(self) -> None:
        if not self.ibc_path:
            return
        print("[Gateway] Stopping…")
        try:
            subprocess.run(
                [rf"{self.ibc_path}\Stop.bat"],
                cwd=self.ibc_path, check=True, shell=True,
            )
        except Exception as exc:
            print(f"[Gateway] Stop error: {exc}")


# ════════════════════════════════════════════════════════════
#  FUNDAMENTALS XML PARSER
# ════════════════════════════════════════════════════════════

def _parse_fundamentals_xml(xml_string: str) -> dict:
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as exc:
        return {"error": str(exc)}

    f: dict = {}

    for coid in root.findall(".//CoID"):
        t = coid.get("Type")
        if   t == "CompanyName": f["company_name"] = coid.text
        elif t == "RepNo":       f["report_no"]    = coid.text
        elif t == "CIKNo":       f["cik"]          = coid.text

    issue = root.find('.//Issue[@Type="C"]')
    if issue is not None:
        tk = issue.find('.//IssueID[@Type="Ticker"]')
        if tk is not None:
            f["symbol"] = tk.text
        ex = issue.find(".//Exchange")
        if ex is not None:
            f["exchange"]      = ex.text
            f["exchange_code"] = ex.get("Code", "N/A")
        cur = issue.get("Currency")
        if cur:
            f["currency"] = cur
        split = issue.find(".//MostRecentSplit")
        if split is not None:
            f["recent_split"] = split.text
            f["split_date"]   = split.get("Date", "N/A")

    gen = root.find(".//CoGeneralInfo")
    if gen is not None:
        emp = gen.find(".//Employees")
        if emp is not None:
            f["employees"]      = emp.text
            f["employees_date"] = emp.get("LastUpdated", "N/A")
        sh = gen.find(".//SharesOut")
        if sh is not None:
            f["shares_outstanding"] = sh.text
            f["shares_date"]        = sh.get("Date", "N/A")
            f["float"]              = sh.get("TotalFloat", "N/A")

    for tag, key in [("Business Summary", "business_summary"),
                     ("Financial Summary", "financial_summary")]:
        el = root.find(f'.//Text[@Type="{tag}"]')
        if el is not None:
            f[key] = el.text

    ratios = root.find(".//Ratios")

    def gr(*fields):
        for name in fields:
            el = (ratios.find(f'.//Ratio[@FieldName="{name}"]')
                  if ratios is not None else None)
            if el is not None and el.text:
                return el.text
        return "N/A"

    f["current_price"]        = gr("NPRICE")
    f["52w_high"]             = gr("NHIG")
    f["52w_low"]              = gr("NLOW")
    f["volume_10d_avg"]       = gr("VOL10DAVG")
    f["enterprise_value"]     = gr("EV")
    f["market_cap"]           = gr("MKTCAP")
    f["revenue_ttm"]          = gr("TTMREV",    "AREV",    "SREV")
    f["ebitda_ttm"]           = gr("TTMEBITD",  "AEBITD",  "AEBT")
    f["net_income_ttm"]       = gr("TTMNIAC",   "ANIAC",   "SNIAC")
    f["eps_ttm"]              = gr("TTMEPSXCLX","AEPSXCLX","SEPSXCLX")
    f["revenue_per_share"]    = gr("TTMREVPS",  "AREVPS")
    f["book_value_per_share"] = gr("QBVPS",     "ABVPS")
    f["cash_per_share"]       = gr("QCSHPS",    "ACSHPS")
    f["cashflow_per_share"]   = gr("TTMCFSHR",  "ACFSHR")
    f["dividend_per_share"]   = gr("TTMDIVSHR", "ADIVSHR")
    f["gross_margin"]         = gr("TTMGROSMGN","AGROSMGN")
    f["roe"]                  = gr("TTMROEPCT", "AROEPCT")
    f["pe_ratio"]             = gr("PEEXCLXOR", "APEEXCLX")
    f["price_to_book"]        = gr("PRICE2BK")
    f["price_to_sales"]       = gr("TTMPR2REV", "APR2REV")

    forecast = root.find(".//ForecastData")

    def gf(field_name):
        if forecast is None:
            return "N/A"
        el = forecast.find(f'.//Ratio[@FieldName="{field_name}"]')
        if el is None:
            return "N/A"
        v = el.find('.//Value[@PeriodType="CURR"]')
        return v.text if v is not None else "N/A"

    f["analyst_recommendation"] = gf("ConsRecom")
    f["target_price"]           = gf("TargetPrice")
    f["projected_growth_rate"]  = gf("ProjLTGrowthRate")
    f["projected_pe"]           = gf("ProjPE")
    f["projected_sales"]        = gf("ProjSales")
    f["projected_eps"]          = gf("ProjEPS")
    f["projected_profit"]       = gf("ProjProfit")

    return f


# ════════════════════════════════════════════════════════════
#  CONTRACT INFO FORMATTER
# ════════════════════════════════════════════════════════════

def format_contract_info(raw: dict) -> dict:
    def parse_hours(hours_str: str) -> list[dict]:
        if not hours_str:
            return []
        sessions = []
        for seg in hours_str.split(";"):
            if "-" not in seg:
                continue
            try:
                start_part, end_part = seg.split("-")
                start_date = start_part.split(":")[0]
                start_time = start_part.split(":")[1] if ":" in start_part else start_part
                end_time   = end_part.split(":")[1]   if ":" in end_part   else end_part
                date_obj   = datetime.strptime(start_date, "%Y%m%d")
                sessions.append({
                    "date":  date_obj.strftime("%Y-%m-%d (%A)"),
                    "start": f"{start_time[:2]}:{start_time[2:]}",
                    "end":   f"{end_time[:2]}:{end_time[2:]}",
                })
            except (ValueError, IndexError):
                continue
        return sessions

    def parse_head_ts(ts: str) -> str | None:
        if not ts:
            return None
        try:
            return datetime.strptime(ts, "%Y%m%d-%H:%M:%S").strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return ts

    return {
        "Contract ID":       raw.get("conId"),
        "Symbol":            raw.get("symbol"),
        "Company":           raw.get("longName"),
        "Primary Exchange":  raw.get("primaryExchange"),
        "Currency":          raw.get("currency"),
        "Category":          f"{raw.get('category')} / {raw.get('subcategory')}",
        "Timezone":          raw.get("timeZoneId"),
        "Min Tick":          raw.get("minTick"),
        "Trading Hours":     parse_hours(raw.get("tradingHours", "")),
        "Liquid Hours":      parse_hours(raw.get("liquidHours", "")),
        "Valid Exchanges":   raw.get("validExchanges", "").split(","),
        "Order Types":       raw.get("orderTypes", "").split(","),
        "Historical Data Since": parse_head_ts(raw.get("headTimestamp", "")),
    }


# ════════════════════════════════════════════════════════════
#  IBKR WORKER
# ════════════════════════════════════════════════════════════

class IBKRWorker(QThread):
    sig_status       = pyqtSignal(str)
    sig_error        = pyqtSignal(str)
    sig_connected    = pyqtSignal(bool)
    sig_portfolio    = pyqtSignal(list)
    sig_account      = pyqtSignal(dict)
    sig_orders       = pyqtSignal(list)
    sig_contract     = pyqtSignal(dict)
    sig_hist_data    = pyqtSignal(object, str, str)
    sig_snapshot     = pyqtSignal(dict, str)
    sig_scanner      = pyqtSignal(list, str, str)
    sig_fundamentals = pyqtSignal(dict)
    sig_order_placed = pyqtSignal(dict)
    sig_executions   = pyqtSignal(list)   # list[dict] — one dict per fill

    def __init__(self, cfg: ConnectionConfig) -> None:
        super().__init__()
        self.cfg  = cfg
        self._ib: IB | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self._pre_queue: list[tuple[str, dict]] = []
        self._lock = threading.Lock()

    def request(self, task: str, **kwargs) -> None:
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self._enqueue, task, kwargs)
        else:
            with self._lock:
                self._pre_queue.append((task, kwargs))

    def _enqueue(self, task: str, kwargs: dict) -> None:
        self._queue.put_nowait((task, kwargs))

    async def _t(self, coro, label: str, timeout: int = TIMEOUT):
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            self.sig_status.emit(f"{label}: timed out ({timeout}s).")
            return None
        except Exception as exc:
            self.sig_status.emit(f"{label}: {exc}")
            return None

    async def _connect(self) -> IB | None:
        ib = IB()
        self._ib = ib
        for attempt in range(1, 4):
            self.sig_status.emit(f"Connecting… (attempt {attempt}/3)")
            try:
                await ib.connectAsync(
                    self.cfg.host, self.cfg.port,
                    clientId=self.cfg.client_id,
                    timeout=15,
                )
                self.sig_status.emit("Connected.")
                self.sig_connected.emit(True)
                ib.execDetailsEvent += self._on_exec_detail   # live fill feed
                return ib
            except Exception as exc:
                self.sig_status.emit(f"Attempt {attempt} failed: {exc}")
                if attempt < 3:
                    await asyncio.sleep(5) # FIX: Avoid ib.sleep crash
        self.sig_connected.emit(False)
        self.sig_error.emit("Could not connect after 3 attempts.")
        return None

    # ── Execution helpers ─────────────────────────────────────

    def _on_exec_detail(self, trade, fill) -> None:
        """Called by ib_insync on every confirmed fill — fires sig_executions."""
        self.sig_executions.emit([self._fill_to_record(fill, trade)])

    def _fill_to_record(self, fill, trade=None) -> dict:
        """Convert an ib_insync Fill (+ optional Trade) to an execution_log dict."""
        ex   = fill.execution
        comm = 0.0
        curr = ""
        if fill.commissionReport:
            comm = fill.commissionReport.commission or 0.0
            curr = fill.commissionReport.currency   or ""
        # Prefer trade.order fields (BUY/SELL, orderType); fall back to Execution
        if trade is not None and hasattr(trade, "order") and trade.order:
            side       = trade.order.action
            order_type = trade.order.orderType
        else:
            raw_side   = (ex.side or "").upper()
            side       = "BUY"  if raw_side in ("BOT", "BUY")  else "SELL"
            order_type = ""
        return {
            "order_id":   ex.orderId,
            "exec_id":    ex.execId,
            "symbol":     fill.contract.symbol,
            "timestamp":  str(fill.time),
            "side":       side,
            "qty":        ex.shares,
            "price":      ex.price,
            "commission": comm,
            "currency":   curr,
            "order_type": order_type,
        }

    async def _fetch_executions(self, ib: IB) -> None:
        """Emit any fills already in the current session (e.g. on reconnect)."""
        self.sig_status.emit("Fetching session executions…")
        fills = ib.fills()   # current-session fills cached by ib_insync
        if fills:
            records = [self._fill_to_record(f) for f in fills]
            self.sig_executions.emit(records)
            self.sig_status.emit(f"Executions: {len(records)} fills loaded")
        else:
            self.sig_status.emit("Executions: none this session")

    async def _dispatch(self, ib: IB, task: str, kw: dict) -> None:
        match task:
            case "portfolio":    await self._fetch_portfolio(ib)
            case "account":      await self._fetch_account(ib)
            case "orders":       await self._fetch_orders(ib)
            case "executions":   await self._fetch_executions(ib)
            case "contract":     await self._fetch_contract(ib, kw)
            case "hist_data":    await self._fetch_hist(ib, kw)
            case "snapshot":     await self._fetch_snapshot(ib, kw)
            case "scanner":      await self._fetch_scanner(ib, kw)
            case "fundamentals": await self._fetch_fundamentals(ib, kw)
            case "place_order":  await self._place_order(ib, kw)
            case "cancel_order": self._cancel_order(ib, kw)
            case _:              self.sig_status.emit(f"Unknown task: {task!r}")

    async def _fetch_portfolio(self, ib: IB) -> None:
        self.sig_status.emit("Fetching portfolio…")
        
        # FIX: Missing required positional argument `account`
        accounts = ib.managedAccounts()
        acct = accounts[0] if accounts else ""
        
        await self._t(ib.reqAccountUpdatesAsync(acct), "Portfolio")
        self.sig_portfolio.emit(list(ib.portfolio()))

    async def _fetch_account(self, ib: IB) -> None:
        self.sig_status.emit("Fetching account summary…")
        raw = await self._t(ib.accountSummaryAsync(), "Account") or []
        want = {
            "NetLiquidation", "TotalCashValue", "AvailableFunds",
            "BuyingPower", "GrossPositionValue", "UnrealizedPnL",
            "RealizedPnL", "InitMarginReq", "MaintMarginReq",
            "ExcessLiquidity", "Cushion", "Leverage", "DayTradesRemaining",
        }
        d: dict[str, float] = {}
        for item in raw:
            if item.tag in want:
                try:
                    d[item.tag] = float(item.value)
                except ValueError:
                    pass
        self.sig_account.emit(d)

    async def _fetch_orders(self, ib: IB) -> None:
        self.sig_status.emit("Fetching open orders…")
        orders = await self._t(ib.reqAllOpenOrdersAsync(), "Orders") or []
        self.sig_orders.emit(list(orders))

    async def _fetch_contract(self, ib: IB, kw: dict) -> None:
        contract = kw["contract"]
        self.sig_status.emit(f"Contract info: {contract.symbol}…")
        details = await self._t(ib.reqContractDetailsAsync(contract), "Contract", timeout=15)
        if not details:
            self.sig_error.emit(f"No contract found for {contract.symbol}.")
            return
        d, c = details[0], details[0].contract
        ts_type = "TRADES" if c.secType in ("STK", "FUT") else "MIDPOINT"
        
        # FIX: Added `formatDate=1` to satisfy updated lib requirement
        head = await self._t(
            ib.reqHeadTimeStampAsync(c, whatToShow=ts_type, useRTH=True, formatDate=1),
            "HeadTimestamp", timeout=8,
        )
        info: dict = {
            "conId":           c.conId,
            "symbol":          c.symbol,
            "secType":         c.secType,
            "exchange":        c.exchange or "SMART",
            "primaryExchange": c.primaryExchange,
            "currency":        c.currency,
            "longName":        d.longName,
            "category":        d.category,
            "subcategory":     d.subcategory,
            "timeZoneId":      d.timeZoneId,
            "minTick":         d.minTick,
            "orderTypes":      d.orderTypes,
            "validExchanges":  d.validExchanges,
            "tradingHours":    d.tradingHours,
            "liquidHours":     d.liquidHours,
            "headTimestamp":   str(head) if head else "",
        }
        self.sig_contract.emit(info)
        self.sig_status.emit(f"Contract ready: {c.symbol}")

    async def _fetch_hist(self, ib: IB, kw: dict) -> None:
        contract = kw["contract"]
        symbol   = contract.symbol
        duration = kw.get("duration", "6 M")
        bar_size = kw.get("bar_size", "1 day")
        end_dt   = kw.get("end_date", "")
        wts = "MIDPOINT" if (contract.secType or "STK") in ("CASH", "CMDTY") else "TRADES"

        self.sig_status.emit(f"Historical: {symbol} {bar_size}…")
        bars = await self._t(
            ib.reqHistoricalDataAsync(
                contract,
                endDateTime    = end_dt,
                durationStr    = duration,
                barSizeSetting = bar_size,
                whatToShow     = wts,
                useRTH         = True,
                formatDate     = 1,
            ),
            "HistData", timeout=40,
        )
        if bars:
            df = util.df(bars)
            if df is not None and not df.empty:
                if "average" in df.columns:
                    df = df.rename(columns={"average": "wap"})
                self.sig_hist_data.emit(df, symbol, bar_size)
                self.sig_status.emit(f"Historical: {symbol} ({len(df)} bars)")
                return
        self.sig_status.emit(f"Historical: no bars for {symbol}")

    async def _fetch_snapshot(self, ib: IB, kw: dict) -> None:
        contract = kw["contract"]
        symbol   = contract.symbol
        self.sig_status.emit(f"Snapshot: {symbol}…")
        tickers = await self._t(ib.reqTickersAsync(contract), "Snapshot", timeout=12)
        if tickers:
            t = tickers[0]
            
            # FIX: Robust NaN sanitization
            vol = 0
            if t.volume is not None:
                try:
                    if not math.isnan(float(t.volume)):
                        vol = int(t.volume)
                except ValueError:
                    pass
            
            self.sig_snapshot.emit({
                "bid":    t.bid,   "ask":    t.ask,
                "last":   t.last,  "volume": vol,
                "open":   getattr(t, "open",  None),
                "high":   getattr(t, "high",  None),
                "low":    getattr(t, "low",   None),
                "close":  getattr(t, "close", None),
            }, symbol)
            self.sig_status.emit(f"Snapshot ready: {symbol}")
        else:
            self.sig_status.emit(f"Snapshot: no data for {symbol}")

    async def _fetch_scanner(self, ib: IB, kw: dict) -> None:
        sub = ScannerSubscription(
            instrument   = kw.get("instrument", "STK"),
            locationCode = kw.get("location",   "STK.EU.MAJOR"),
            scanCode     = kw.get("scan_code",  "TOP_PERC_GAIN"),
            numberOfRows = kw.get("num_rows",   15),
        )
        self.sig_status.emit(f"Scanner: {sub.scanCode}…")
        results = await self._t(
            ib.reqScannerDataAsync(sub,
                scannerSubscriptionFilterOptions=kw.get("filters", [])),
            "Scanner", timeout=30,
        ) or []
        self.sig_scanner.emit(list(results), sub.scanCode, sub.locationCode)
        self.sig_status.emit(f"Scanner: {len(results)} results")

    async def _fetch_fundamentals(self, ib: IB, kw: dict) -> None:
        contract = kw["contract"]
        self.sig_status.emit(f"Fundamentals: {contract.symbol}…")
        xml = await self._t(
            ib.reqFundamentalDataAsync(contract, "ReportSnapshot"),
            "Fundamentals", timeout=20,
        )
        if xml:
            self.sig_fundamentals.emit(_parse_fundamentals_xml(xml))
            self.sig_status.emit(f"Fundamentals ready: {contract.symbol}")
        else:
            self.sig_status.emit(f"Fundamentals: no data for {contract.symbol}")

    async def _place_order(self, ib: IB, kw: dict) -> None:
        contract   = kw["contract"]
        action     = kw["action"]
        quantity   = float(kw["quantity"])
        order_type = kw.get("order_type", "MKT")
        lmt_price  = float(kw.get("lmt_price",    0.0))
        trail_pct  = float(kw.get("trailing_pct", 0.0))
        stop_price = float(kw.get("stop_price",   0.0))
        parent_id  = int(kw.get("parent_id",      0))

        if order_type == "MKT":
            order = MarketOrder(action, quantity)
        elif order_type == "LMT":
            order = LimitOrder(action, quantity, lmt_price)
        elif order_type == "TRAIL":
            order = IBOrder()
            order.action = action; order.totalQuantity = quantity
            order.orderType = "TRAIL"; order.trailingPercent = trail_pct
            order.transmit = True
            if parent_id: order.parentId = parent_id
        elif order_type == "STP":
            order = IBOrder()
            order.action = action; order.totalQuantity = quantity
            order.orderType = "STP"; order.auxPrice = stop_price
            order.transmit = True
        else:
            self.sig_status.emit(f"Unsupported order type: {order_type}"); return

        self.sig_status.emit(f"Placing {order_type} {action} {quantity} {contract.symbol}…")
        trade = ib.placeOrder(contract, order)

        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.5) # FIX: Avoid ib.sleep crash
            if trade.isDone() or trade.orderStatus.status in (
                    "Submitted", "PreSubmitted", "Filled"):
                break

        d = {
            "order_id":     trade.order.orderId,
            "symbol":       contract.symbol,
            "sec_type":     contract.secType,
            "action":       action,
            "quantity":     quantity,
            "order_type":   order_type,
            "lmt_price":    lmt_price,
            "stop_price":   stop_price,
            "trailing_pct": trail_pct,
            "status":       trade.orderStatus.status,
            "parent_id":    parent_id,
        }
        self.sig_order_placed.emit(d)
        self.sig_status.emit(f"Order {d['order_id']} → {d['status']}")

    def _cancel_order(self, ib: IB, kw: dict) -> None:
        oid = int(kw.get("order_id", 0))
        for t in ib.trades():
            if t.order.orderId == oid:
                ib.cancelOrder(t.order)
                self.sig_status.emit(f"Cancel sent: order {oid}")
                return
        self.sig_status.emit(f"Order {oid} not found.")

    async def _main(self) -> None:
        self._queue: asyncio.Queue[tuple[str, dict]] = asyncio.Queue()

        with self._lock:
            for item in self._pre_queue:
                self._queue.put_nowait(item)
            self._pre_queue.clear()

        ib = await self._connect()
        if not ib:
            return

        for task in ("portfolio", "account", "orders", "executions"):
            self._queue.put_nowait((task, {}))

        # FIX: robustly handle event loop queue with wait_for, instead of sleep
        while ib.isConnected():
            try:
                task, kw = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue 
            try:
                await self._dispatch(ib, task, kw)
            except Exception as exc:
                self.sig_error.emit(f"Task '{task}': {exc}")

        self.sig_connected.emit(False)
        self.sig_status.emit("Disconnected.")

    def run(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.sig_error.emit(f"Worker crashed: {exc}")
        finally:
            self.loop.close()