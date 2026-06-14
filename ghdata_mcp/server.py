"""
股海罗盘 GH-Data MCP Server

提供工具：
  - query_financial_report:      业绩报表明细（营收、净利润、EPS、ROE等）
  - query_balance_sheet:         资产负债表（总资产、负债、权益、流动比率等）
  - query_cashflow_statement:    现金流量表（经营/投资/融资现金流等）
  - query_income_statement:      利润表（营业总收入/总成本、费用、利润等）
  - query_realtime_price:        实时行情（最新价、涨跌幅、成交量、市值、市盈率等）
  - get_stock_unlock_data:       限售股解禁数据（按市场/日期查询解禁股票明细）
  - query_shareholder_trade:     股东增减持数据（按股票/日期/方向查询增减持明细）
  - query_research_report:       个股研报数据（按股票/日期/评级查询券商研报及EPS/PE预测）
  - query_institutional_survey:  机构调研数据（按股票/日期查询机构调研记录，含调研内容详情）
  - query_main_holdings:         主力持仓数据（按股票查询机构持仓汇总及明细，含增减仓变化）
  - query_dividend_history:      分红配股数据（按股票查询历史分红送转配股明细记录）
  - query_money_flow:            资金流向数据（按股票查询每日资金流向，含大单/中单/小单净流入）
  - query_margin_trading:        融资融券数据（按股票查询融资余额/融券余额/两融明细）
  - query_executive_hold_change: 高管持股变动数据（按股票/日期/姓名查询增持/减持记录）
  - generate_kline_chart:        生成K线图（日K线+均线+今日分时走势，保存为PNG图片）

启动方式：
  python gh_data_mcp.py

注册到 QwenPaw（控制台 → 智能体 → MCP → + 创建）：
{
  "gh-data": {
    "command": "python",
    "args": ["E:\\GuPiao\\agent\\ghdata\\mcpserver\\gh_data_mcp.py"],
    "env": {}
  }
}
"""

import json
import time
import urllib.request
import urllib.parse
import threading
import functools

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    import subprocess, sys
    print("正在安装 mcp 依赖...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mcp[cli]"])
    from mcp.server.fastmcp import FastMCP

mcp = FastMCP("股海罗盘数据引擎")

# ═══════════════════════════════════════════════════
# 全局反爬限制：互斥锁 + 30 秒最小间隔
# 所有工具的连续调用必须间隔至少 30 秒，且全程不可并发
# ═══════════════════════════════════════════════════
_call_lock = threading.Lock()
_last_call_time: float = 0.0
MIN_CALL_INTERVAL = 30  # 秒


def _rate_limit_decorator(func):
    """
    限流/互斥装饰器。
    持有锁期间才执行工具函数，确保：
    1. 同一时刻只有一个工具在调用
    2. 两次调用至少间隔 MIN_CALL_INTERVAL 秒
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _last_call_time
        with _call_lock:
            elapsed = time.time() - _last_call_time
            if elapsed < MIN_CALL_INTERVAL:
                wait = MIN_CALL_INTERVAL - elapsed
                time.sleep(wait)
            _last_call_time = time.time()
            return func(*args, **kwargs)
    return wrapper

# ═══════════════════════════════════════════════════
# 财报数据 API（数据中心）
# ═══════════════════════════════════════════════════

DATA_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"


def _fetch(code: str, page_size: int, page_number: int, report_name: str,
           sort_column: str = "REPORT_DATE") -> list[dict]:
    """通用财报数据抓取"""
    params = {
        "sortColumns": sort_column,
        "sortTypes": "-1",
        "pageSize": page_size,
        "pageNumber": page_number,
        "columns": "ALL",
        "filter": f'(SECURITY_CODE="{code}")',
        "reportName": report_name,
    }
    url = DATA_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    if data.get("success") and data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    return []


# ═══════════════════════════════════════════════════
# 实时行情 API（GH-Data 数据源 + Playwright + 干净 Edge）
# ═══════════════════════════════════════════════════

# 注意：push2.eastmoney.com 有浏览器反爬检测，
# Playwright 自动管理的浏览器会被拒绝（ERR_EMPTY_RESPONSE），
# 这里使用「手动启动干净 Edge + CDP 连接」的方式绕过。

import asyncio
import atexit
import os
import random
import socket
import string
import subprocess
import threading
import time
import uuid

try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

_EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
_edge_proc: subprocess.Popen | None = None
_cdp_port: int = 0

# 持久化的事件循环和 Playwright 连接（运行在后台线程中）
_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_playwright = None
_browser = None
_loop_ready = threading.Event()


def _find_free_port() -> int:
    """找一个空闲端口"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 15) -> bool:
    """等端口可用"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
            time.sleep(0.3)
    return False


def _make_secid(code: str) -> str:
    """生成 secid 参数"""
    code = code.strip()
    if code.startswith("6"):
        return f"1.{code}"
    elif code.startswith(("0", "3")):
        return f"0.{code}"
    else:
        return f"0.{code}"


def _start_edge() -> bool:
    """启动干净的 Edge 浏览器并开启 CDP"""
    global _edge_proc, _cdp_port

    if _edge_proc is not None:
        return True  # 已启动

    if not os.path.exists(_EDGE_PATH):
        return False

    _cdp_port = _find_free_port()
    _edge_proc = subprocess.Popen(
        [
            _EDGE_PATH,
            f"--remote-debugging-port={_cdp_port}",
            "--no-first-run",
            "--headless=new",
            "--new-window",
            "about:blank",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    if not _wait_for_port(_cdp_port):
        _edge_proc = None
        return False

    # 注册退出清理
    atexit.register(_stop_all)
    return True


def _stop_all() -> None:
    """关闭所有资源（Edge + Playwright + 事件循环）"""
    global _edge_proc, _cdp_port, _playwright, _browser, _loop, _loop_thread

    if _loop and _loop.is_running():
        # 在事件循环中执行清理
        async def cleanup():
            global _playwright, _browser
            if _browser:
                try:
                    await _browser.close()
                except Exception:
                    pass
                _browser = None
            if _playwright:
                try:
                    await _playwright.stop()
                except Exception:
                    pass
                _playwright = None

        try:
            future = asyncio.run_coroutine_threadsafe(cleanup(), _loop)
            future.result(timeout=10)
        except Exception:
            pass

        _loop.call_soon_threadsafe(_loop.stop)
        if _loop_thread:
            _loop_thread.join(timeout=5)

    _loop = None
    _loop_thread = None

    if _edge_proc:
        try:
            _edge_proc.terminate()
            _edge_proc.wait(timeout=5)
        except Exception:
            try:
                _edge_proc.kill()
            except Exception:
                pass
        _edge_proc = None
        _cdp_port = 0


def _get_or_start_loop() -> asyncio.AbstractEventLoop | None:
    """获取或启动持久化事件循环"""
    global _loop, _loop_thread, _loop_ready

    if _loop is not None and _loop.is_running():
        return _loop

    # 首次启动：创建后台线程运行事件循环
    _loop = asyncio.new_event_loop()
    _loop_ready.clear()

    def _run_loop():
        asyncio.set_event_loop(_loop)
        _loop_ready.set()
        _loop.run_forever()

    _loop_thread = threading.Thread(target=_run_loop, daemon=True)
    _loop_thread.start()
    _loop_ready.wait(timeout=5)
    return _loop


async def _ensure_playwright_connected():
    """确保 Playwright 已通过 CDP 连接到 Edge"""
    global _playwright, _browser

    if _playwright is not None and _browser is not None:
        # 检查连接是否仍然有效
        try:
            ctxs = _browser.contexts
            _ = len(ctxs)  # 简单探测
            return
        except Exception:
            pass

    # 连接断开或首次连接
    try:
        if _browser:
            try:
                await _browser.close()
            except Exception:
                pass
            _browser = None
        if _playwright:
            try:
                await _playwright.stop()
            except Exception:
                pass
            _playwright = None

        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_cdp_port}"
        )
        # 关闭所有空白页面，避免残留 about:blank 窗口
        for ctx in _browser.contexts:
            for p in ctx.pages:
                url = p.url.strip()
                if url in ("about:blank", ""):
                    try:
                        await p.close()
                    except Exception:
                        pass
    except Exception:
        _playwright = None
        _browser = None
        raise


async def _fetch_realtime_async(code: str) -> dict | None:
    """异步执行实时行情获取"""
    try:
        await _ensure_playwright_connected()

        context = _browser.contexts[0] if _browser.contexts else await _browser.new_context()
        page = await context.new_page()

        # 构造 URL（模拟 Delphi：动态 ut + 随机后缀）
        ut = str(uuid.uuid4()).replace("-", "").lower()
        rand_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
        secid = _make_secid(code)
        params = {
            "secid": secid,
            "ut": ut,
            "wbp2u": "%7C0%7C0%7C0%7Cweb",
            "dect": "1",
            "fields": "f43,f44,f45,f46,f47,f48,f50,f57,f58,f84,f85,f162,f167,f168,f170",
        }
        query = "&".join([f"{k}={v}" for k, v in params.items()]) + rand_suffix
        url = f"https://push2.eastmoney.com/api/qt/stock/get?{query}"

        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        text = await page.evaluate("() => document.body.innerText")
        await page.close()

        data = json.loads(text)
        if data.get("rc") == 0 and data.get("data"):
            return data["data"]
        return None

    except Exception:
        return None


def _fetch_realtime(code: str) -> dict | None:
    """
    通过 Push2 接口获取实时行情原始数据
    使用「手动启动干净 Edge + CDP 连接」方式绕过反爬
    """
    global _playwright, _browser

    if async_playwright is None:
        return None  # playwright 未安装

    # 确保 Edge 已启动
    if not _start_edge():
        return None

    # 获取/启动持久化事件循环
    loop = _get_or_start_loop()
    if loop is None:
        return None

    # 在线程的事件循环中执行
    future = asyncio.run_coroutine_threadsafe(_fetch_realtime_async(code), loop)
    try:
        return future.result(timeout=25)
    except Exception:
        return None


# ═══════════════════════════════════════════════════
# 通用 Push2 数据获取（通过 Edge CDP，用于资金流向等）
# ═══════════════════════════════════════════════════

async def _fetch_push2_async(url: str) -> dict | None:
    """
    通过 Edge CDP 方式获取 push2 数据。
    page.goto() 直接访问 push2 端点，利用真实 Edge TLS 指纹绕过反爬。
    返回 JSON 反序列化后的 dict（含 data 字段），失败返回 None。
    """
    try:
        await _ensure_playwright_connected()

        context = _browser.contexts[0] if _browser.contexts else await _browser.new_context()
        page = await context.new_page()

        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        text = await page.evaluate("() => document.body.innerText")
        await page.close()

        data = json.loads(text)
        if data.get("rc") == 0 and data.get("data"):
            return data["data"]
        # 有些 push2his 返回的 data 可能是空列表而非 None
        if data.get("data") is not None:
            return data["data"]
        return None

    except Exception:
        return None


def _fetch_push2(url: str) -> dict | None:
    """
    同步包装：通过 Edge CDP 获取 push2 数据。
    """
    if async_playwright is None:
        return None

    if not _start_edge():
        return None

    loop = _get_or_start_loop()
    if loop is None:
        return None

    future = asyncio.run_coroutine_threadsafe(_fetch_push2_async(url), loop)
    try:
        return future.result(timeout=25)
    except Exception:
        return None


def _build_push2_url(base: str, secid: str, params: dict) -> str:
    """
    构造 push2 请求 URL，附加动态 ut 和随机后缀防缓存。
    """
    ut = str(uuid.uuid4()).replace("-", "").lower()
    rand_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    params["ut"] = ut
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{base}?{query}{rand_suffix}"


# ═══════════════════════════════════════════════════
# 工具 1：业绩报表明细
# ═══════════════════════════════════════════════════

@mcp.tool()
@_rate_limit_decorator
def query_financial_report(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """
    查询指定股票的业绩报表明细（包括营收、净利润、EPS、ROE、毛利率等）

    Args:
        code: 股票代码，纯数字，如 "002455"。不要带 .SZ / .SH 后缀
        page_size: 每页返回多少条数据，默认 5，最大 20
        page_number: 查询第几页，默认 1
    """
    items = _fetch(code, page_size, page_number, "RPT_LICO_FN_CPD", "REPORTDATE")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的业绩数据"

    lines = [f"📊 [{code}] 业绩报表明细（共 {len(items)} 条）", "=" * 60]
    for i, item in enumerate(items, 1):
        income = item.get("TOTAL_OPERATE_INCOME", 0)
        profit = item.get("PARENT_NETPROFIT", 0)
        lines.append(f"\n{i}. {item.get('DATATYPE', 'N/A')}  |  公告日: {item.get('NOTICE_DATE', 'N/A')}")
        lines.append(f"   {'代码':<8} {item.get('SECURITY_CODE', 'N/A')}   {'名称':<8} {item.get('SECURITY_NAME_ABBR', 'N/A')}")
        lines.append(f"   {'营收':<8} {income / 1e8:>10.2f} 亿  {'同比':<6} {item.get('YSTZ', 'N/A')}%")
        lines.append(f"   {'净利润':<8} {profit / 1e8:>10.2f} 亿  {'同比':<6} {item.get('SJLTZ', 'N/A')}%")
        lines.append(f"   {'EPS':<8} {item.get('BASIC_EPS', 'N/A')}        {'ROE':<6} {item.get('WEIGHTAVG_ROE', 'N/A')}%")
        lines.append(f"   {'毛利率':<8} {item.get('XSMLL', 'N/A')}%       {'股息率':<6} {item.get('ZXGXL', 'N/A')}%")
        lines.append(f"   {'每股净资产':<8} {item.get('BPS', 'N/A')}    {'每股经营现金流':<6} {item.get('MGJYXJJE', 'N/A')}")
        if item.get("ASSIGNDSCRPT"):
            lines.append(f"   {'分红':<8} {item['ASSIGNDSCRPT']}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 2：资产负债表
# ═══════════════════════════════════════════════════

@mcp.tool()
@_rate_limit_decorator
def query_balance_sheet(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """
    查询指定股票的资产负债表（总资产、总负债、股东权益、货币资金、应收账款、存货、流动比率、资产负债率等）

    Args:
        code: 股票代码，纯数字，如 "002455"。不要带 .SZ / .SH 后缀
        page_size: 每页返回多少条数据，默认 5，最大 20
        page_number: 查询第几页，默认 1
    """
    items = _fetch(code, page_size, page_number, "RPT_DMSK_FN_BALANCE")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的资产负债表数据"

    lines = [f"📋 [{code}] 资产负债表（共 {len(items)} 条）", "=" * 60]
    for i, item in enumerate(items, 1):
        ta = item.get("TOTAL_ASSETS", 0)
        tl = item.get("TOTAL_LIABILITIES", 0)
        te = item.get("TOTAL_EQUITY", 0)
        mf = item.get("MONETARYFUNDS", 0) or 0
        ar = item.get("ACCOUNTS_RECE", 0) or 0
        inv = item.get("INVENTORY", 0) or 0
        ap = item.get("ACCOUNTS_PAYABLE", 0) or 0
        dbr = item.get("DEBT_ASSET_RATIO", "N/A")

        lines.append(f"\n{i}. {item.get('REPORT_DATE', 'N/A')[:10]}  |  公告日: {item.get('NOTICE_DATE', 'N/A')[:10]}")
        lines.append(f"   {'代码':<8} {item.get('SECURITY_CODE', 'N/A')}   {'名称':<8} {item.get('SECURITY_NAME_ABBR', 'N/A')}")
        lines.append(f"   {'总资产':<8} {ta / 1e8:>12.2f} 亿  {'总资产同比':<6} {item.get('TOTAL_ASSETS_RATIO', 'N/A')}%")
        lines.append(f"   {'总负债':<8} {tl / 1e8:>12.2f} 亿  {'总负债同比':<6} {item.get('TOTAL_LIAB_RATIO', 'N/A')}%")
        lines.append(f"   {'股东权益':<8} {te / 1e8:>12.2f} 亿  {'权益同比':<6} {item.get('TOTAL_EQUITY_RATIO', 'N/A')}%")
        lines.append(f"   {'货币资金':<8} {mf / 1e8:>12.2f} 亿  {'货币资金同比':<6} {item.get('MONETARYFUNDS_RATIO', 'N/A')}%")
        lines.append(f"   {'应收账款':<8} {ar / 1e8:>12.2f} 亿  {'应收同比':<6} {item.get('ACCOUNTS_RECE_RATIO', 'N/A')}%")
        lines.append(f"   {'存货':<8} {inv / 1e8:>12.2f} 亿  {'存货同比':<6} {item.get('INVENTORY_RATIO', 'N/A')}%")
        lines.append(f"   {'应付账款':<8} {ap / 1e8:>12.2f} 亿  {'应付同比':<6} {item.get('ACCOUNTS_PAYABLE_RATIO', 'N/A')}%")
        lines.append(f"   {'资产负债率':<8} {dbr}%   {'流动比率':<6} {item.get('CURRENT_RATIO', 'N/A')}%")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 3：现金流量表
# ═══════════════════════════════════════════════════

@mcp.tool()
@_rate_limit_decorator
def query_cashflow_statement(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """
    查询指定股票的现金流量表（经营/投资/融资现金流净额、净现金流等）

    Args:
        code: 股票代码，纯数字，如 "002455"。不要带 .SZ / .SH 后缀
        page_size: 每页返回多少条数据，默认 5，最大 20
        page_number: 查询第几页，默认 1
    """
    items = _fetch(code, page_size, page_number, "RPT_DMSK_FN_CASHFLOW")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的现金流量表数据"

    lines = [f"💰 [{code}] 现金流量表（共 {len(items)} 条）", "=" * 60]
    for i, item in enumerate(items, 1):
        no = item.get("NETCASH_OPERATE", 0) or 0
        ni = item.get("NETCASH_INVEST", 0) or 0
        nf = item.get("NETCASH_FINANCE", 0) or 0
        cce = item.get("CCE_ADD", 0) or 0
        sales = item.get("SALES_SERVICES", 0) or 0
        staff = item.get("PAY_STAFF_CASH", 0) or 0
        construct = item.get("CONSTRUCT_LONG_ASSET", 0) or 0

        lines.append(f"\n{i}. {item.get('REPORT_DATE', 'N/A')[:10]}  |  公告日: {item.get('NOTICE_DATE', 'N/A')[:10]}")
        lines.append(f"   {'代码':<12} {item.get('SECURITY_CODE', 'N/A')}   {'名称':<8} {item.get('SECURITY_NAME_ABBR', 'N/A')}")
        lines.append(f"   {'经营活动现金流':<12} {no / 1e8:>10.2f} 亿  {'同比':<6} {item.get('NETCASH_OPERATE_RATIO', 'N/A')}%")
        lines.append(f"   {'投资活动现金流':<12} {ni / 1e8:>10.2f} 亿  {'同比':<6} {item.get('NETCASH_INVEST_RATIO', 'N/A')}%")
        lines.append(f"   {'融资活动现金流':<12} {nf / 1e8:>10.2f} 亿  {'同比':<6} {item.get('NETCASH_FINANCE_RATIO', 'N/A')}%")
        lines.append(f"   {'净现金流':<12} {cce / 1e8:>10.2f} 亿  {'同比':<6} {item.get('CCE_ADD_RATIO', 'N/A')}%")
        lines.append(f"   {'销售商品收到现金':<12} {sales / 1e8:>10.2f} 亿")
        lines.append(f"   {'支付职工现金':<12} {staff / 1e8:>10.2f} 亿")
        lines.append(f"   {'购建长期资产':<12} {construct / 1e8:>10.2f} 亿")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 4：利润表
# ═══════════════════════════════════════════════════

@mcp.tool()
@_rate_limit_decorator
def query_income_statement(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """
    查询指定股票的利润表（营业总收入/总成本、营业利润、利润总额、净利润、费用明细等）

    Args:
        code: 股票代码，纯数字，如 "002455"。不要带 .SZ / .SH 后缀
        page_size: 每页返回多少条数据，默认 5，最大 20
        page_number: 查询第几页，默认 1
    """
    items = _fetch(code, page_size, page_number, "RPT_DMSK_FN_INCOME")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的利润表数据"

    lines = [f"📈 [{code}] 利润表（共 {len(items)} 条）", "=" * 60]
    for i, item in enumerate(items, 1):
        income = item.get("TOTAL_OPERATE_INCOME", 0) or 0
        cost = item.get("TOTAL_OPERATE_COST", 0) or 0
        op = item.get("OPERATE_PROFIT", 0) or 0
        tp = item.get("TOTAL_PROFIT", 0) or 0
        np = item.get("PARENT_NETPROFIT", 0) or 0
        sale_exp = item.get("SALE_EXPENSE", 0) or 0
        mgmt_exp = item.get("MANAGE_EXPENSE", 0) or 0
        fin_exp = item.get("FINANCE_EXPENSE", 0) or 0
        tax = item.get("INCOME_TAX", 0) or 0
        dnp = item.get("DEDUCT_PARENT_NETPROFIT", 0) or 0

        lines.append(f"\n{i}. {item.get('REPORT_DATE', 'N/A')[:10]}  |  公告日: {item.get('NOTICE_DATE', 'N/A')[:10]}")
        lines.append(f"   {'代码':<12} {item.get('SECURITY_CODE', 'N/A')}   {'名称':<8} {item.get('SECURITY_NAME_ABBR', 'N/A')}")
        lines.append(f"   {'营业总收入':<12} {income / 1e8:>10.2f} 亿  {'同比':<6} {item.get('TOI_RATIO', 'N/A')}%")
        lines.append(f"   {'营业总成本':<12} {cost / 1e8:>10.2f} 亿  {'同比':<6} {item.get('TOE_RATIO', 'N/A')}%")
        lines.append(f"   {'营业利润':<12} {op / 1e8:>10.2f} 亿  {'同比':<6} {item.get('OPERATE_PROFIT_RATIO', 'N/A')}%")
        lines.append(f"   {'利润总额':<12} {tp / 1e8:>10.2f} 亿  {'净利润':<6} {np / 1e8:>10.2f} 亿  {'同比':<6} {item.get('PARENT_NETPROFIT_RATIO', 'N/A')}%")
        lines.append(f"   {'扣非净利润':<12} {dnp / 1e8:>10.2f} 亿  {'同比':<6} {item.get('DPN_RATIO', 'N/A')}%")
        lines.append(f"   {'销售费用':<12} {sale_exp / 1e8:>10.2f} 亿  {'管理费用':<6} {mgmt_exp / 1e8:>10.2f} 亿")
        lines.append(f"   {'财务费用':<12} {fin_exp / 1e8:>10.2f} 亿  {'所得税':<6} {tax / 1e8:>10.2f} 亿")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 5：实时行情
# ═══════════════════════════════════════════════════

@mcp.tool()
@_rate_limit_decorator
def query_realtime_price(code: str) -> str:
    """
    查询指定股票的实时行情（最新价、涨跌幅、最高/最低/今开、昨收、成交量、成交额）

    Args:
        code: 股票代码，纯数字，如 "601899" 或 "002455"。不要带 .SZ / .SH 后缀
    """
    d = _fetch_realtime(code)
    if not d:
        return f"❌ 未查询到股票 [{code}] 的实时行情"

    # Push2 字段映射（注：价格类字段需 ÷100，百分比类字段需 ÷100）
    name = d.get("f58", "N/A")
    code_out = d.get("f57", code)

    def div100(v):
        try:
            return float(v) / 100.0
        except (TypeError, ValueError):
            return 0.0

    def div1e8(v):
        try:
            return float(v) / 1e8
        except (TypeError, ValueError):
            return 0.0

    price = div100(d.get("f43", 0))       # 最新价（元）
    high = div100(d.get("f44", 0))         # 最高
    low = div100(d.get("f45", 0))          # 最低
    open_p = div100(d.get("f46", 0))       # 今开
    change_pct = div100(d.get("f170", 0))  # 涨跌幅（%）
    turnover_rate = div100(d.get("f168", 0))  # 换手率（%）
    pe_ttm = div100(d.get("f162", 0))      # 市盈率（动）
    pb = div100(d.get("f167", 0))          # 市净率
    volume_lot = float(d.get("f47", 0) or 0)   # 成交量（手）
    amount_val = float(d.get("f48", 0) or 0)   # 成交额
    total_shares = float(d.get("f84", 0) or 0)  # 总股本
    float_shares = float(d.get("f85", 0) or 0)  # 流通股本
    volume_ratio = div100(d.get("f50", 0))      # 量比

    # 计算昨收
    prev_close = 0.0
    if price > 0 and abs(change_pct) > 0.0001:
        prev_close = round(price / (1 + change_pct / 100.0), 2)
    else:
        prev_close = price  # 无法计算时≈最新价

    change_symbol = "+" if change_pct > 0 else ""
    updown = abs(price - prev_close) if prev_close > 0 else 0

    lines = [
        f"【{code_out}】{name} 实时行情",
        f"{'=' * 50}",
        f"  最新价: {price:.2f}元  涨跌幅: {change_symbol}{change_pct:.2f}%  "
        f"涨跌: {change_symbol}{updown:.2f}元",
        f"  今开: {open_p:.2f}  最高: {high:.2f}  最低: {low:.2f}  昨收: {prev_close:.2f}",
        f"  成交量: {volume_lot / 10000:.2f}万手  成交额: {amount_val / 1e8:.2f}亿元",
        f"  换手率: {turnover_rate:.2f}%  量比: {volume_ratio:.2f}",
        f"  市盈率(动): {pe_ttm:.2f}  市净率: {pb:.2f}",
        f"  总股本: {total_shares / 1e8:.2f}亿  流通股本: {float_shares / 1e8:.2f}亿",
    ]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 6：限售股解禁数据
# ═══════════════════════════════════════════════════

# 市场代码映射
MARKET_CODES = {
    "沪市": '("069001001001","069001001006","069001001003")',
    "深市": '("069001002001","069001002002","069001002005")',
    "全部": '("069001001001","069001001006","069001001003","069001002001","069001002002","069001002005")',
}

# 解禁数据 API 字段
UNLOCK_COLUMNS = (
    "SECURITY_CODE,SECURITY_NAME_ABBR,FREE_DATE,"
    "CURRENT_FREE_SHARES,ABLE_FREE_SHARES,LIFT_MARKET_CAP,"
    "FREE_RATIO,NEW,B20_ADJCHRATE,A20_ADJCHRATE,"
    "FREE_SHARES_TYPE,TOTAL_RATIO,NON_FREE_SHARES,BATCH_HOLDER_NUM"
)

# 字段中文名
UNLOCK_FIELD_CN = {
    "SECURITY_CODE": "股票代码",
    "SECURITY_NAME_ABBR": "股票简称",
    "FREE_DATE": "解禁日期",
    "CURRENT_FREE_SHARES": "实际解禁数量(万股)",
    "ABLE_FREE_SHARES": "解禁数量(万股)",
    "LIFT_MARKET_CAP": "实际解禁市值(万元)",
    "FREE_RATIO": "占解禁前流通市值比例(%)",
    "NEW": "最新收盘价(元)",
    "B20_ADJCHRATE": "解禁前20日涨跌幅(%)",
    "A20_ADJCHRATE": "解禁后20日涨跌幅(%)",
    "FREE_SHARES_TYPE": "限售股类型",
    "TOTAL_RATIO": "占总股本比例(%)",
    "NON_FREE_SHARES": "解禁后剩余限售股(万股)",
    "BATCH_HOLDER_NUM": "解禁持有人数",
}


@mcp.tool()
@_rate_limit_decorator
def get_stock_unlock_data(
    market: str = "全部",
    start_date: str = "",
    end_date: str = "",
    page_size: int = 30,
    page_number: int = 1,
) -> str:
    """
    获取A股限售股解禁数据

    Args:
        market: 市场分类，"沪市"、"深市" 或 "全部"（默认全部）
        start_date: 起始解禁日期，格式 YYYY-MM-DD（默认当天）
        end_date:   截止解禁日期，格式 YYYY-MM-DD（默认当天起一年后）
        page_size:  每页条数，默认30，最大50
        page_number: 页码，默认1

    Returns:
        格式化的解禁股票数据表格，包含股票代码、简称、解禁日期、
        解禁数量、市值、涨跌幅等字段
    """
    market = market.strip()
    if market not in MARKET_CODES:
        return f"❌ 无效的市场分类: {market}，可选值: 沪市, 深市, 全部"

    # 默认日期
    from datetime import date
    today = date.today().isoformat()
    if not start_date:
        start_date = today
    if not end_date:
        y = int(today[:4]) + 1
        end_date = f"{y}-{today[5:7]}-{today[8:]}"

    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)

    # 构造 filter
    market_filter = MARKET_CODES[market]
    date_filter = f"(FREE_DATE>='{start_date}')(FREE_DATE<='{end_date}')"
    filter_str = f"(TRADE_MARKET_CODE in {market_filter}){date_filter}"

    # 请求参数
    params = {
        "sortColumns": "FREE_DATE,CURRENT_FREE_SHARES",
        "sortTypes": "1,1",
        "pageSize": page_size,
        "pageNumber": page_number,
        "reportName": "RPT_LIFT_STAGE",
        "columns": UNLOCK_COLUMNS,
        "source": "WEB",
        "client": "WEB",
        "filter": filter_str,
    }
    url = DATA_API + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not data.get("success") or not data.get("result"):
        return f"❌ API 返回异常: {data.get('message', '未知错误')}"

    result = data["result"]
    total_pages = result.get("pages", 0)
    total_count = result.get("count", 0)
    records = result.get("data", [])

    if not records:
        return f"📭 未查询到 [{market}] {start_date} ~ {end_date} 期间的解禁数据"

    # 格式化输出
    lines = [
        f"🔓 [{market}A股] 限售股解禁数据  {start_date} ~ {end_date}",
        f"   共 {total_count} 条，当前第 {page_number}/{total_pages} 页",
        "=" * 120,
        f"{'序号':<4} {'代码':<8} {'简称':<8} {'解禁日期':<12} {'限售股类型':<18} "
        f"{'解禁数量(万股)':<14} {'实际解禁(万股)':<14} {'解禁市值(万元)':<14} "
        f"{'占流通市值比例':<10} {'最新价':<8} {'前20日涨跌%':<10}",
        "-" * 120,
    ]

    for idx, rec in enumerate(records, 1):
        code = rec.get("SECURITY_CODE", "")
        name = rec.get("SECURITY_NAME_ABBR", "")
        free_date = (rec.get("FREE_DATE") or "")[:10]
        stype = rec.get("FREE_SHARES_TYPE", "")
        able_shares = rec.get("ABLE_FREE_SHARES", 0) or 0
        cur_shares = rec.get("CURRENT_FREE_SHARES", 0) or 0
        mkt_cap = rec.get("LIFT_MARKET_CAP", 0) or 0
        free_ratio = (rec.get("FREE_RATIO", 0) or 0) * 100
        new_price = rec.get("NEW", 0) or 0
        b20 = rec.get("B20_ADJCHRATE", 0) or 0

        lines.append(
            f"{idx:<4} {code:<8} {name:<8} {free_date:<12} {stype:<18} "
            f"{able_shares:<14.2f} {cur_shares:<14.2f} {mkt_cap:<14.2f} "
            f"{free_ratio:<10.2f} {new_price:<8.2f} {b20:<10.2f}"
        )

    lines.append("=" * 120)
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 7：解禁持有人明细
# ═══════════════════════════════════════════════════

# 解禁持有人明细 API 字段
HOLDER_COLUMNS = (
    "SECURITY_CODE,SECUCODE,LIMITED_HOLDER_NAME,"
    "ADD_LISTING_SHARES,ACTUAL_LISTED_SHARES,ADD_LISTING_CAP,"
    "ACTUAL_LISTED_CAP,LOCK_MONTH,RESIDUAL_LIMITED_SHARES,"
    "FREE_SHARES_TYPE,PLAN_FEATURE,FREE_DATE,"
    "LIFT_SHARES,LIFT_HOLDER_ALL,LIFT_SHARES_ALL,LIMITED_TYPE"
)

HOLDER_FIELD_CN = {
    "LIMITED_HOLDER_NAME": "股东名称",
    "FREE_DATE": "解禁日期",
    "ADD_LISTING_SHARES": "本次可解禁数量(股)",
    "ACTUAL_LISTED_SHARES": "实际上市流通(股)",
    "ADD_LISTING_CAP": "解禁市值(元)",
    "ACTUAL_LISTED_CAP": "实际上市流通市值(元)",
    "LOCK_MONTH": "锁定期(月)",
    "RESIDUAL_LIMITED_SHARES": "剩余未解禁数量(股)",
    "FREE_SHARES_TYPE": "限售股类型",
    "PLAN_FEATURE": "进度",
    "LIFT_SHARES": "所在批次解禁数量(万股)",
    "LIFT_HOLDER_ALL": "该批次持有人总数",
    "LIFT_SHARES_ALL": "该批次总解禁数量(万股)",
    "LIMITED_TYPE": "限售类型",
    "SECURITY_CODE": "股票代码",
    "SECUCODE": "证券代码",
}


@mcp.tool()
@_rate_limit_decorator
def get_stock_unlock_holders(
    code: str = "",
    free_date: str = "",
    page_size: int = 50,
    page_number: int = 1,
) -> str:
    """
    获取指定股票解禁的持有人明细数据（股东名称、解禁数量、锁定期等）

    数据来源：RPT_LIFT_GD 报表。
    可查到每个解禁批次对应的具体股东名称、解禁数量、锁定期、剩余未解禁数量等。

    Args:
        code: 股票代码，纯数字，如 "301371"。不要带 .SZ / .SH 后缀
        free_date: 解禁日期，格式 YYYY-MM-DD（可选）
                   不传则返回该股票所有历史+未来的解禁批次持有人数据
        page_size: 每页条数，默认50，最大100
        page_number: 页码，默认1

    Returns:
        格式化的持有人明细数据表格
    """
    # ── 构建过滤条件 ──
    filters = []
    if code:
        filters.append(f'(SECURITY_CODE="{code}")')
    if free_date:
        filters.append(f"(FREE_DATE='{free_date}')")
    filter_str = "".join(filters) if filters else ""

    page_size = max(1, min(page_size, 100))
    page_number = max(1, page_number)

    # ── 请求参数 ──
    params = {
        "sortColumns": "FREE_DATE,ADD_LISTING_SHARES",
        "sortTypes": "1,-1",
        "pageSize": page_size,
        "pageNumber": page_number,
        "reportName": "RPT_LIFT_GD",
        "columns": HOLDER_COLUMNS,
        "source": "WEB",
        "client": "WEB",
        "filter": filter_str,
    }
    url = DATA_API + "?" + urllib.parse.urlencode(params)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not data.get("success") or not data.get("result"):
        return f"❌ API 返回异常: {data.get('message', '未知错误')}"

    result = data["result"]
    total_count = result.get("count", 0)
    records = result.get("data", [])

    if not records:
        return f"📭 未查询到股票 [{code}] 的解禁持有人数据"

    # ── 按解禁日期分组输出 ──
    from collections import OrderedDict
    groups = OrderedDict()
    for r in records:
        fd = (r.get("FREE_DATE") or "")[:10]
        if fd not in groups:
            groups[fd] = {
                "holders": [],
                "holder_count": r.get("LIFT_HOLDER_ALL", 0),
                "batch_total": r.get("LIFT_SHARES_ALL", 0),
                "free_type": r.get("FREE_SHARES_TYPE", ""),
                "plan": r.get("PLAN_FEATURE", ""),
            }
        groups[fd]["holders"].append(r)

    lines = [
        f"🔍 股票 [{code}] 解禁持有人明细（共 {total_count} 条记录）",
        "=" * 90,
    ]

    for fd, g in groups.items():
        lines.append(f"\n📅 解禁日期: {fd}  |  类型: {g['free_type']}  |  "
                     f"批次持有人: {g['holder_count']}户  |  "
                     f"进度: {g['plan']}")
        lines.append("-" * 90)
        lines.append(
            f"  {'股东名称':<18} {'本次可解禁(股)':<14} "
            f"{'实际上市(股)':<12} {'解禁市值(元)':<14} "
            f"{'锁定期(月)':<10} {'剩余锁定(股)':<12}"
        )
        lines.append("-" * 90)
        for r in g["holders"]:
            name = r.get("LIMITED_HOLDER_NAME", "")
            add_shares = r.get("ADD_LISTING_SHARES", 0) or 0
            act_shares = r.get("ACTUAL_LISTED_SHARES", 0) or 0
            add_cap = r.get("ADD_LISTING_CAP", 0) or 0
            lock_m = r.get("LOCK_MONTH", 0) or 0
            residual = r.get("RESIDUAL_LIMITED_SHARES", 0) or 0
            lines.append(
                f"  {name:<18} {add_shares:>12}  "
                f"{act_shares:>10}  {add_cap:>12.0f}  "
                f"{lock_m:>8}  {residual:>12}"
            )
        lines.append("-" * 90)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 8：股东增减持查询
# ═══════════════════════════════════════════════════

_SH_REPORT_NAME = "RPT_SHARE_HOLDER_INCREASE"
_SH_SORT_COLUMNS = "END_DATE,SECURITY_CODE,EITIME"
_SH_SORT_TYPES = "-1,-1,-1"
_SH_QUOTE_COLUMNS = "f2~01~SECURITY_CODE~NEWEST_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE_QUOTES"

_SH_FIELD_CN = {
    "SECURITY_CODE": "股票代码",
    "SECURITY_NAME_ABBR": "股票简称",
    "HOLDER_NAME": "股东名称",
    "DIRECTION": "方向",
    "CHANGE_NUM": "变动数量(万股)",
    "CHANGE_NUM_SYMBOL": "变动数量(带符号)",
    "CHANGE_RATE": "变动比例(%)",
    "CHANGE_FREE_RATIO": "变动占流通股比(%)",
    "AFTER_HOLDER_NUM": "变动后持股(万股)",
    "AFTER_CHANGE_RATE": "变动后占总股本比(%)",
    "HOLD_RATIO": "占比(%)",
    "FREE_SHARES": "持流通股(万股)",
    "FREE_SHARES_RATIO": "占流通股比(%)",
    "START_DATE": "变动开始",
    "END_DATE": "变动截止",
    "TRADE_DATE": "交易日期",
    "NOTICE_DATE": "公告日期",
    "CLOSE_PRICE": "收盘价",
    "TRADE_AVERAGE_PRICE": "均价",
    "REAL_PRICE": "实际价格",
    "MARKET": "交易方式",
    "NEWEST_PRICE": "最新价",
    "CHANGE_RATE_QUOTES": "涨跌幅(%)",
    "EITIME": "录入时间",
}


def _fmt(v, default="--", fmt_spec=".2f"):
    """安全格式化数值，处理 None"""
    if v is None:
        return default
    try:
        return format(v, fmt_spec)
    except (ValueError, TypeError):
        return str(v)


def _build_shareholder_filter(
    stock_codes: list[str] | None = None,
    start_date: str = "",
    end_date: str = "",
    direction: str = "",
    holder_name: str = "",
) -> str:
    """构建股东增减持查询的 filter 参数"""
    parts = []

    # 日期范围（单引号）
    if start_date and end_date:
        parts.append(f"(END_DATE>='{start_date}')(END_DATE<='{end_date}')")
    elif start_date:
        parts.append(f"(END_DATE>='{start_date}')")
    elif end_date:
        parts.append(f"(END_DATE<='{end_date}')")

    # 股票代码（双引号）
    if stock_codes:
        codes_str = ",".join(f'"{c}"' for c in stock_codes)
        parts.append(f"(SECURITY_CODE in ({codes_str}))")

    # 方向（双引号）
    if direction:
        parts.append(f'(DIRECTION="{direction}")')

    # 股东名称（双引号）
    if holder_name:
        parts.append(f'(HOLDER_NAME="{holder_name}")')

    return "".join(parts)


def _fetch_shareholder_trades(
    filter_str: str,
    page_size: int = 50,
    page_number: int = 1,
) -> list[dict]:
    """调取股东增减持原始数据"""
    params = {
        "sortColumns": _SH_SORT_COLUMNS,
        "sortTypes": _SH_SORT_TYPES,
        "pageSize": page_size,
        "pageNumber": page_number,
        "reportName": _SH_REPORT_NAME,
        "quoteColumns": _SH_QUOTE_COLUMNS,
        "quoteType": 0,
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": filter_str,
    }
    url = DATA_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    if data.get("success") and data.get("result"):
        return data["result"].get("data", [])
    return []


def _format_shareholder_record(r: dict) -> str:
    """格式化单条增减持记录"""
    code = r.get("SECURITY_CODE", "")
    name = r.get("SECURITY_NAME_ABBR", "")
    holder = r.get("HOLDER_NAME", "")
    direct = r.get("DIRECTION", "")
    change_num = _fmt(r.get("CHANGE_NUM"), "0.00")
    change_rate = _fmt(r.get("CHANGE_RATE"), "0.00")
    change_free = _fmt(r.get("CHANGE_FREE_RATIO"), "0.00")
    after_num = _fmt(r.get("AFTER_HOLDER_NUM"), "0.00")
    after_rate = _fmt(r.get("AFTER_CHANGE_RATE"), "0.00")
    start_d = (r.get("START_DATE") or "")[:10]
    end_d = (r.get("END_DATE") or "")[:10]
    market = r.get("MARKET") or "--"
    close_p = _fmt(r.get("CLOSE_PRICE"))
    newest_p = _fmt(r.get("NEWEST_PRICE"))
    chg_pct = _fmt(r.get("CHANGE_RATE_QUOTES"))

    return (
        f"  [{code}] {name}\n"
        f"    股东: {holder}\n"
        f"    方向: {direct}  {change_num}万股 "
        f"(占总股本 {change_rate}%, 占流通股 {change_free}%)\n"
        f"    期间: {start_d} ~ {end_d}  方式: {market}\n"
        f"    变动后: 持股{after_num}万股 / 占{after_rate}%\n"
        f"    收盘价: {close_p}  最新价: {newest_p}  涨跌幅: {chg_pct}%\n"
    )


@mcp.tool()
@_rate_limit_decorator
def query_shareholder_trade(
    stock_codes: str = "",
    start_date: str = "",
    end_date: str = "",
    direction: str = "",
    holder_name: str = "",
    page_size: int = 50,
    page_number: int = 1,
) -> str:
    """
    查询指定时间段内股票的股东增减持数据（高管、大股东等）

    Args:
        stock_codes: 股票代码列表，多个用逗号分隔，如 "601899,600519"
        start_date:  起始日期，格式 YYYY-MM-DD，如 "2026-01-01"
        end_date:    截止日期，格式 YYYY-MM-DD，如 "2026-06-10"
        direction:   变动方向，"增持" 或 "减持"（不传查全部）
        holder_name: 股东名称关键字
        page_size:   每页条数，默认50，最大100
        page_number: 页码，默认1

    Returns:
        格式化的股东增减持数据表格
    """
    # 解析股票代码
    codes_list = None
    if stock_codes.strip():
        codes_list = [c.strip() for c in stock_codes.split(",") if c.strip()]

    # 校验方向
    if direction and direction not in ("增持", "减持"):
        return "❌ direction 参数只能为 '增持' 或 '减持'（不传则查全部）"

    # 校验页码
    page_size = max(1, min(page_size, 100))
    page_number = max(1, page_number)

    # 构造筛选条件
    filter_str = _build_shareholder_filter(
        stock_codes=codes_list,
        start_date=start_date,
        end_date=end_date,
        direction=direction,
        holder_name=holder_name,
    )

    try:
        items = _fetch_shareholder_trades(filter_str, page_size, page_number)
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not items:
        return "未查询到符合条件的股东增减持数据。"

    # 统计
    total_increase = sum(
        r.get("CHANGE_NUM", 0) for r in items if r.get("DIRECTION") == "增持"
    )
    total_decrease = sum(
        r.get("CHANGE_NUM", 0) for r in items if r.get("DIRECTION") == "减持"
    )
    increase_count = sum(1 for r in items if r.get("DIRECTION") == "增持")
    decrease_count = sum(1 for r in items if r.get("DIRECTION") == "减持")

    # 构建输出
    lines = [
        "📊 股东增减持数据",
        f"   共 {len(items)} 条记录  |  增持 {increase_count} 条 "
        f"({total_increase:.2f}万股)  |  减持 {decrease_count} 条 "
        f"({total_decrease:.2f}万股)",
        "=" * 60,
    ]

    for i, item in enumerate(items, 1):
        lines.append(f"【{i}】")
        lines.append(_format_shareholder_record(item))
        if i < len(items):
            lines.append("-" * 40)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 个股研报数据 API
# ═══════════════════════════════════════════════════

REPORT_API = "https://reportapi.eastmoney.com/report/list2"

# 评级代码映射
RATING_CODE_MAP = {
    "买入": "007",
    "增持": "006",
    "中性": "005",
    "减持": "004",
    "卖出": "003",
}

# 评级变动映射
RATING_CHANGE_MAP = {
    "调高": "1",
    "维持": "2",
    "调低": "3",
}


def _fmt_report_date(d: str) -> str:
    if not d:
        return "--"
    return d[:10] if len(d) >= 10 else d


def _fmt_eps(v: str) -> str:
    if not v or v.strip() == "":
        return "--"
    return f"{float(v):.2f}"


def _fmt_pe(v: str) -> str:
    if not v or v.strip() == "":
        return "--"
    return f"{float(v):.2f}"


@mcp.tool()
@_rate_limit_decorator
def query_research_report(
    code: str = "",
    start_date: str = "",
    end_date: str = "",
    rating: str = "",
    rating_change: str = "",
    org_code: str = "",
    industry_code: str = "",
    researcher: str = "",
    page_size: int = 10,
    page_number: int = 1,
) -> str:
    """
    查询指定时间段内某只股票的券商研报数据（个股研报）

    数据源：个股研报数据中心
    涵盖买入、增持、中性、减持、卖出等评级，以及 EPS/PE 预测值。

    Args:
        code:           股票代码，纯数字，如 "601899"。不传查全部
        start_date:     起始日期，格式 YYYY-MM-DD，如 "2026-01-01"
        end_date:       截止日期，格式 YYYY-MM-DD，如 "2026-06-10"
        rating:         评级过滤："买入"/"增持"/"中性"/"减持"/"卖出"（不传查全部）
        rating_change:  评级变动："调高"/"维持"/"调低"（不传查全部）
        org_code:       机构代码（如 "80036717"），不传查全部
        industry_code:  行业代码，不传查全部
        researcher:     研究员姓名关键字
        page_size:      每页条数，默认10，最大50
        page_number:    页码，默认1

    Returns:
        格式化的个股研报数据表格
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)

    rating_code = RATING_CODE_MAP.get(rating, "*") if rating else "*"
    rating_change_code = RATING_CHANGE_MAP.get(rating_change, "*") if rating_change else "*"

    payload = {
        "beginTime": start_date or "",
        "endTime": end_date or "",
        "industryCode": industry_code if industry_code else "*",
        "ratingChange": rating_change_code,
        "rating": rating_code,
        "orgCode": org_code if org_code else "*",
        "code": code if code else "*",
        "rcode": researcher if researcher else "",
        "pageSize": page_size,
        "pageNo": page_number,
    }

    req_data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        REPORT_API,
        data=req_data,
        headers={
            "Content-Type": "application/json; charset=UTF-8",
            "User-Agent": "Mozilla/5.0",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            result = json.loads(resp.read())
    except Exception as e:
        return f"请求失败: {str(e)}"

    items = result.get("data", [])
    total_hits = result.get("hits", 0)
    total_pages = result.get("TotalPage", 0)
    current_page = result.get("pageNo", page_number)

    if not items:
        return "未查询到符合条件的研报数据。"

    lines = [
        "个股研报数据",
        f"   共 {total_hits} 条结果  |  当前第 {current_page}/{total_pages} 页  |  每页 {page_size} 条",
        "=" * 70,
    ]

    for i, item in enumerate(items, 1):
        title = item.get("title", "")
        stock_name = item.get("stockName", "")
        stock_code = item.get("stockCode", "")
        org_name = item.get("orgSName") or item.get("orgName", "")
        pub_date = _fmt_report_date(item.get("publishDate", ""))
        em_rating = item.get("emRatingName", "")
        last_rating = item.get("lastEmRatingName", "")
        rc = item.get("ratingChange")
        rc_str = {1: "调高", 2: "维持", 3: "调低"}.get(rc, "")
        researcher_names = item.get("researcher", "")
        eps_this = _fmt_eps(item.get("predictThisYearEps", ""))
        eps_next = _fmt_eps(item.get("predictNextYearEps", ""))
        eps_next2 = _fmt_eps(item.get("predictNextTwoYearEps", ""))
        pe_this = _fmt_pe(item.get("predictThisYearPe", ""))
        pe_next = _fmt_pe(item.get("predictNextYearPe", ""))
        pe_next2 = _fmt_pe(item.get("predictNextTwoYearPe", ""))
        attach_pages = item.get("attachPages", 0)

        lines.append(f"【{i}】{title}")
        lines.append(f"    股票: {stock_name}({stock_code})")
        lines.append(f"    机构: {org_name}  |  日期: {pub_date}")
        lines.append(f"    评级: {em_rating} {rc_str}  |  研究员: {researcher_names}")
        lines.append(f"    页数: {attach_pages}页")

        eps_parts = []
        if eps_this != "--":
            eps_parts.append(f"今年EPS={eps_this}")
        if eps_next != "--":
            eps_parts.append(f"明年EPS={eps_next}")
        if eps_next2 != "--":
            eps_parts.append(f"后年EPS={eps_next2}")
        if eps_parts:
            lines.append(f"    EPS预测: {' | '.join(eps_parts)}")

        pe_parts = []
        if pe_this != "--":
            pe_parts.append(f"今年PE={pe_this}")
        if pe_next != "--":
            pe_parts.append(f"明年PE={pe_next}")
        if pe_next2 != "--":
            pe_parts.append(f"后年PE={pe_next2}")
        if pe_parts:
            lines.append(f"    PE预测: {' | '.join(pe_parts)}")

        if last_rating and last_rating != em_rating:
            lines.append(f"    上次评级: {last_rating}")

        if i < len(items):
            lines.append("-" * 70)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 机构调研数据 API
# ═══════════════════════════════════════════════════

SURVEY_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
SURVEY_REPORT_NAME = "RPT_ORG_SURVEY"

SURVEY_COLUMNS = (
    "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,"
    "NOTICE_DATE,RECEIVE_START_DATE,RECEIVE_END_DATE,"
    "RECEIVE_OBJECT,RECEIVE_PLACE,RECEIVE_WAY,RECEIVE_WAY_EXPLAIN,"
    "INVESTIGATORS,RECEPTIONIST,ORG_TYPE,SUM,CONTENT"
)

SURVEY_QUOTE_COLUMNS = "f2~01~SECURITY_CODE~CLOSE_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE"


def _build_survey_filter(
    code: str,
    start_date: str = "",
    end_date: str = "",
) -> str:
    """构造机构调研的 filter 参数字符串"""
    parts = ['(NUMBERNEW="1")(IS_SOURCE="1")']
    if code:
        parts.append(f'(SECURITY_CODE="{code}")')
    # 日期使用单引号包裹（API 要求）
    if start_date:
        parts.append(f"(RECEIVE_START_DATE>'{start_date}')")
    if end_date:
        parts.append(f"(RECEIVE_START_DATE<'{end_date}')")
    return "".join(parts)


def _fetch_surveys(
    filter_str: str,
    page_size: int,
    page_number: int,
) -> list[dict]:
    """调用机构调研 API 获取数据"""
    params = {
        "reportName": SURVEY_REPORT_NAME,
        "columns": SURVEY_COLUMNS,
        "quoteColumns": SURVEY_QUOTE_COLUMNS,
        "pageSize": page_size,
        "pageNumber": page_number,
        "sortColumns": "NOTICE_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
        "filter": filter_str,
    }
    url = SURVEY_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    if data.get("success") and data.get("result") and data["result"].get("data"):
        return data["result"]["data"]
    return []


def _fmt_survey_date(d: str) -> str:
    """格式化日期"""
    if not d or d == "null":
        return "--"
    return d[:10] if len(d) >= 10 else d


def _fmt_content(content: str, max_len: int = 200) -> str:
    """截断长内容"""
    if not content:
        return ""
    if len(content) > max_len:
        return content[:max_len] + "..."
    return content


@mcp.tool()
@_rate_limit_decorator
def query_institutional_survey(
    code: str = "",
    start_date: str = "",
    end_date: str = "",
    page_size: int = 10,
    page_number: int = 1,
) -> str:
    """
    查询指定时间段内某只股票的机构调研数据

    数据源：机构调研数据中心（RPT_ORG_SURVEY）
    涵盖调研日期、调研机构、调研方式、接待人员、调研内容详情等。

    Args:
        code:       股票代码，纯数字，如 "002517"。不传查全部
        start_date: 起始日期，格式 YYYY-MM-DD，如 "2026-01-01"
        end_date:   截止日期，格式 YYYY-MM-DD，如 "2026-06-10"
        page_size:  每页条数，默认10，最大50
        page_number: 页码，默认1

    Returns:
        格式化的机构调研数据表格
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)

    filter_str = _build_survey_filter(
        code=code,
        start_date=start_date,
        end_date=end_date,
    )

    try:
        items = _fetch_surveys(filter_str, page_size, page_number)
    except Exception as e:
        return f"请求失败: {str(e)}"

    if not items:
        return "未查询到符合条件的机构调研数据。"

    lines = [
        "机构调研数据",
        f"   共 {len(items)} 条记录",
        "=" * 70,
    ]

    for i, item in enumerate(items, 1):
        # 基础信息
        stock_name = item.get("SECURITY_NAME_ABBR", "")
        stock_code = item.get("SECURITY_CODE", "")
        notice_date = _fmt_survey_date(item.get("NOTICE_DATE", ""))
        survey_date = _fmt_survey_date(item.get("RECEIVE_START_DATE", ""))
        survey_end_date = _fmt_survey_date(item.get("RECEIVE_END_DATE", ""))

        # 调研信息
        survey_obj = item.get("RECEIVE_OBJECT", "")
        survey_place = item.get("RECEIVE_PLACE", "")
        survey_way = item.get("RECEIVE_WAY_EXPLAIN", "")
        org_type = item.get("ORG_TYPE", "")
        investor_count = item.get("SUM", "")

        # 接待人员
        receptionist = item.get("RECEPTIONIST", "")
        investigators = item.get("INVESTIGATORS", "")

        # 调研内容
        content = item.get("CONTENT", "")

        # 行情
        close_price = item.get("CLOSE_PRICE", "")
        change_rate = item.get("CHANGE_RATE", "")

        lines.append(f"【{i}】{stock_name}({stock_code})")
        lines.append(f"    公告日: {notice_date}  |  调研日: {survey_date}" +
                     (f" ~ {survey_end_date}" if survey_end_date != "--" else ""))
        lines.append(f"    调研对象: {survey_obj or '--'}")
        lines.append(f"    调研方式: {survey_way}  |  地点: {survey_place or '--'}")

        if investor_count:
            lines.append(f"    参与机构数: {investor_count}家  |  机构类型: {org_type or '--'}")
        else:
            lines.append(f"    机构类型: {org_type or '--'}")

        if close_price:
            lines.append(f"    收盘价: {close_price}元  |  涨跌幅: {change_rate}%")

        if receptionist:
            lines.append(f"    接待人员: {receptionist[:120]}" +
                         ("..." if len(receptionist) > 120 else ""))

        if investigators:
            lines.append(f"    调研人员: {investigators[:120]}" +
                         ("..." if len(investigators) > 120 else ""))

        # 调研内容摘要
        if content:
            content_text = _fmt_content(content, 300)
            lines.append(f"    调研内容:")
            # 按问答分段显示
            paragraphs = content_text.split("\n\n")
            for p in paragraphs[:5]:
                p = p.strip()
                if p:
                    lines.append(f"      {p[:200]}")
        else:
            lines.append(f"    调研内容: (无详细内容)")

        if i < len(items):
            lines.append("-" * 70)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 主力持仓数据 API
# ═══════════════════════════════════════════════════

HOLD_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
HOLD_REPORT_NAME = "RPT_MAIN_ORGHOLD"

# 机构类型映射
ORG_TYPE_MAP = {
    "00": "机构汇总",
    "01": "基金",
    "02": "QFII",
    "03": "社保",
    "04": "券商",
    "05": "保险",
    "06": "信托",
    "07": "其他",
    "08": "一般法人",
}


def _fmt_hold_num(v) -> str:
    """格式化持股数（股→万/亿）"""
    try:
        n = float(v) if v else 0
    except (ValueError, TypeError):
        return "--"
    if abs(n) >= 1e8:
        return f"{n/1e8:.2f}亿"
    elif abs(n) >= 1e4:
        return f"{n/1e4:.2f}万"
    else:
        return f"{n:.0f}"


def _fmt_hold_value(v) -> str:
    """格式化持股市值"""
    try:
        n = float(v) if v else 0
    except (ValueError, TypeError):
        return "--"
    if abs(n) >= 1e8:
        return f"{n/1e8:.2f}亿"
    elif abs(n) >= 1e4:
        return f"{n/1e4:.2f}万"
    else:
        return f"{n:.2f}"


def _fmt_ratio(v) -> str:
    """格式化百分比"""
    try:
        n = float(v) if v else 0
    except (ValueError, TypeError):
        return "--"
    return f"{n:.2f}%"


@mcp.tool()
@_rate_limit_decorator
def query_main_holdings(
    code: str = "",
    report_date: str = "",
    page_size: int = 10,
    page_number: int = 1,
) -> str:
    """
    查询指定股票的机构主力持仓数据

    数据源：主力持仓数据中心（RPT_MAIN_ORGHOLD）
    涵盖机构汇总、基金、保险、券商等各类机构的持仓数量、市值、占流通股比例及增减仓变化。

    Args:
        code:        股票代码，纯数字，如 "002517"。不传查全部
        report_date: 报告期，格式 YYYY-MM-DD，如 "2026-03-31"。
                     不传则自动获取最新报告期
        page_size:   每页条数，默认10，最大50
        page_number: 页码，默认1

    Returns:
        格式化的主力持仓数据表格
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)

    # 如果没有指定报告期，自动获取最新的
    if not report_date:
        try:
            date_params = {
                "reportName": "RPT_MAIN_REPORTDATE",
                "columns": "REPORT_DATE",
                "pageSize": 1,
                "pageNumber": 1,
                "sortColumns": "REPORT_DATE",
                "sortTypes": "-1",
                "source": "WEB",
                "client": "WEB",
            }
            date_url = HOLD_API + "?" + urllib.parse.urlencode(date_params)
            req = urllib.request.Request(date_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                date_data = json.loads(resp.read())
            if date_data.get("success") and date_data.get("result", {}).get("data"):
                report_date = date_data["result"]["data"][0]["REPORT_DATE"][:10]
        except Exception:
            report_date = ""

    if not report_date:
        return "无法获取报告期数据。"

    # 构建 filter（单引号包裹日期）
    filter_parts = []
    if code:
        filter_parts.append(f'(SECURITY_CODE="{code}")')
    if report_date:
        filter_parts.append(f"(REPORT_DATE='{report_date}')")
    filter_str = "".join(filter_parts) if filter_parts else ""

    params = {
        "reportName": HOLD_REPORT_NAME,
        "columns": "ALL",
        "pageSize": page_size,
        "pageNumber": page_number,
        "sortColumns": "HOLD_VALUE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
    }
    if filter_str:
        params["filter"] = filter_str

    try:
        url = HOLD_API + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return f"请求失败: {str(e)}"

    if not data.get("success") or not data.get("result", {}).get("data"):
        return "未查询到符合条件的持仓数据。"

    items = data["result"]["data"]
    stock_name = items[0].get("SECURITY_NAME_ABBR", "")
    stock_code = items[0].get("SECURITY_CODE", "")
    rep_date = items[0].get("REPORT_DATE", "")[:10] if items[0].get("REPORT_DATE") else report_date

    lines = [
        f"主力持仓数据",
        f"   股票: {stock_name}({stock_code})  |  报告期: {rep_date}",
        "=" * 75,
    ]

    for i, item in enumerate(items, 1):
        org_type_code = item.get("ORG_TYPE", "")
        org_type_name = item.get("ORG_TYPE_NAME") or ORG_TYPE_MAP.get(org_type_code, org_type_code)
        hold_num = item.get("HOULD_NUM", 0)
        total_shares = item.get("TOTAL_SHARES", 0)
        hold_value = item.get("HOLD_VALUE", 0)
        free_ratio = _fmt_ratio(item.get("FREESHARES_RATIO", 0))
        total_ratio = _fmt_ratio(item.get("TOTALSHARES_RATIO", 0))
        holdcha = item.get("HOLDCHA", "")
        holdcha_num = item.get("HOLDCHA_NUM", 0)
        holdcha_ratio = item.get("HOLDCHA_RATIO", 0)
        holdcha_value = item.get("HOLDCHA_VALUE", 0)
        qchange = item.get("QCHANGE_RATE", "")
        free_market_cap = item.get("FREE_MARKET_CAP", 0)

        # 增减仓符号
        if holdcha == "增仓":
            holdcha_symbol = "增仓+"
        elif holdcha == "减仓":
            holdcha_symbol = "减仓"
        else:
            holdcha_symbol = "--"

        lines.append(f"【{i}】{org_type_name}")
        lines.append(f"    持有机构: {hold_num}家  |  持股: {_fmt_hold_num(total_shares)}股")
        lines.append(f"    持股市值: {_fmt_hold_value(hold_value)}元")
        lines.append(f"    占流通股: {free_ratio}  |  占总股本: {total_ratio}")

        # 增减仓信息
        change_parts = []
        if holdcha_num and float(holdcha_num) != 0:
            change_parts.append(f"变动: {_fmt_hold_num(holdcha_num)}股 ({holdcha_symbol})")
        if holdcha_ratio is not None and str(holdcha_ratio) not in ("", "None"):
            change_parts.append(f"变动比例: {holdcha_ratio}%")
        if holdcha_value and float(holdcha_value) != 0:
            change_parts.append(f"变动市值: {_fmt_hold_value(holdcha_value)}元")
        if change_parts:
            lines.append(f"    增减仓: {' | '.join(change_parts)}")

        # 季度涨跌幅
        if qchange is not None and str(qchange) not in ("", "None"):
            lines.append(f"    季度涨跌幅: {qchange}%")

        # 流通市值
        if free_market_cap and float(free_market_cap) > 0:
            lines.append(f"    流通市值: {_fmt_hold_value(free_market_cap)}元")

        if i < len(items):
            lines.append("-" * 75)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 分红配股数据 API
# ═══════════════════════════════════════════════════

DIVIDEND_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
DIVIDEND_REPORT_NAME = "RPT_SHAREBONUS_DET"


def _fmt_money(v) -> str:
    """格式化金额"""
    try:
        n = float(v) if v else 0
    except (ValueError, TypeError):
        return "--"
    return f"{n:.2f}"


def _fmt_dividend_date(d) -> str:
    """格式化日期"""
    if not d or d == "null":
        return "--"
    s = str(d)[:10]
    return s


@mcp.tool()
@_rate_limit_decorator
def query_dividend_history(
    code: str = "",
    page_size: int = 20,
    page_number: int = 1,
) -> str:
    """
    查询指定股票的历史分红送转配股数据

    数据源：分红配股数据中心（RPT_SHAREBONUS_DET）
    涵盖分红方案、股权登记日、除权除息日、每股派息、送转比例、股息率等。

    Args:
        code:        股票代码，纯数字，如 "002517"。不传查全部
        page_size:   每页条数，默认20，最大50
        page_number: 页码，默认1

    Returns:
        格式化的分红配股数据表格
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)

    # 构造 filter
    filter_str = ""
    if code:
        filter_str = f'(SECURITY_CODE="{code}")'

    params = {
        "reportName": DIVIDEND_REPORT_NAME,
        "columns": "ALL",
        "pageSize": page_size,
        "pageNumber": page_number,
        "sortColumns": "PLAN_NOTICE_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
    }
    if filter_str:
        params["filter"] = filter_str

    try:
        url = DIVIDEND_API + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return f"请求失败: {str(e)}"

    if not data.get("success") or not data.get("result", {}).get("data"):
        return "未查询到分红配股数据。"

    items = data["result"]["data"]
    total = data["result"].get("count", len(items))
    stock_name = items[0].get("SECURITY_NAME_ABBR", "")
    stock_code = items[0].get("SECURITY_CODE", "")

    lines = [
        f"分红配股数据",
        f"   股票: {stock_name}({stock_code})  |  共 {total} 条记录",
        "=" * 75,
    ]

    for i, item in enumerate(items, 1):
        # 方案描述
        impl_plan = item.get("IMPL_PLAN_PROFILE", "")
        report_date = _fmt_dividend_date(item.get("REPORT_DATE", ""))
        plan_notice = _fmt_dividend_date(item.get("PLAN_NOTICE_DATE", ""))
        equity_date = _fmt_dividend_date(item.get("EQUITY_RECORD_DATE", ""))
        ex_div_date = _fmt_dividend_date(item.get("EX_DIVIDEND_DATE", ""))
        notice_date = _fmt_dividend_date(item.get("NOTICE_DATE", ""))
        progress = item.get("ASSIGN_PROGRESS", "")
        pretax_bonus = item.get("PRETAX_BONUS_RMB", "")
        basic_eps = item.get("BASIC_EPS", "")
        bvps = item.get("BVPS", "")
        per_capital = item.get("PER_CAPITAL_RESERVE", "")
        per_profit = item.get("PER_UNASSIGN_PROFIT", "")
        pnp_yoy = item.get("PNP_YOY_RATIO", "")
        total_shares = item.get("TOTAL_SHARES", "")
        div_ratio = item.get("DIVIDENT_RATIO", "")

        lines.append(f"【{i}】{report_date}  |  {impl_plan}")
        lines.append(f"    进度: {progress}  |  预案公告: {plan_notice}")

        # 除权除息信息
        div_parts = []
        if equity_date and equity_date != "--":
            div_parts.append(f"股权登记: {equity_date}")
        if ex_div_date and ex_div_date != "--":
            div_parts.append(f"除权除息: {ex_div_date}")
        if notice_date and notice_date != "--":
            div_parts.append(f"实施公告: {notice_date}")
        if div_parts:
            lines.append(f"    关键日期: {' | '.join(div_parts)}")

        # 派息信息
        bonus_parts = []
        if pretax_bonus is not None and str(pretax_bonus) not in ("", "None"):
            bonus_parts.append(f"每股税前: {_fmt_money(pretax_bonus)}元")
        if div_ratio is not None and str(div_ratio) not in ("", "None"):
            try:
                bonus_parts.append(f"股息率: {float(div_ratio)*100:.2f}%")
            except (ValueError, TypeError):
                pass
        if bonus_parts:
            lines.append(f"    派息: {' | '.join(bonus_parts)}")

        # 财务数据
        fin_parts = []
        if basic_eps is not None and str(basic_eps) not in ("", "None"):
            fin_parts.append(f"EPS={_fmt_money(basic_eps)}")
        if bvps is not None and str(bvps) not in ("", "None"):
            fin_parts.append(f"BPS={_fmt_money(bvps)}")
        if per_capital is not None and str(per_capital) not in ("", "None"):
            fin_parts.append(f"资本公积={_fmt_money(per_capital)}")
        if per_profit is not None and str(per_profit) not in ("", "None"):
            fin_parts.append(f"未分配利润={_fmt_money(per_profit)}")
        if fin_parts:
            lines.append(f"    财务指标: {' | '.join(fin_parts)}")

        # 净利润增长
        if pnp_yoy is not None and str(pnp_yoy) not in ("", "None"):
            lines.append(f"    净利润同比: {_fmt_money(pnp_yoy)}%")

        # 总股本
        if total_shares is not None and str(total_shares) not in ("", "None"):
            try:
                shares = float(total_shares)
                if shares >= 1e8:
                    lines.append(f"    总股本: {shares/1e8:.2f}亿股")
                else:
                    lines.append(f"    总股本: {shares/1e4:.2f}万股")
            except (ValueError, TypeError):
                pass

        if i < len(items):
            lines.append("-" * 75)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 融资融券数据工具
# ═══════════════════════════════════════════════════

_RZRQ_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_RZRQ_REPORT_NAME = "RPTA_WEB_RZRQ_GGMX"


@mcp.tool()
@_rate_limit_decorator
def query_margin_trading(
    code: str = "",
    start_date: str = "",
    end_date: str = "",
    page_size: int = 30,
    page_number: int = 1,
) -> str:
    """
    查询指定股票的融资融券（两融）数据

    数据源：融资融券数据中心（RPTA_WEB_RZRQ_GGMX）
    涵盖融资余额、融券余额、融资买入/偿还/净买入、融券卖出/偿还/净卖量等。

    Args:
        code:        股票代码，纯数字，如 "002517"。不传查全部
        start_date:  起始日期，格式 YYYY-MM-DD，如 "2026-05-01"
        end_date:    截止日期，格式 YYYY-MM-DD，如 "2026-06-10"
        page_size:   每页条数，默认30，最大240
        page_number: 页码，默认1

    Returns:
        格式化的融资融券数据表格 + 趋势影响分析
    """
    page_size = max(1, min(page_size, 240))
    page_number = max(1, page_number)

    # 构造 filter
    filter_parts = []
    if code:
        filter_parts.append(f'(SCODE="{code}")')
    if start_date:
        filter_parts.append(f"(DATE>='{start_date}')")
    if end_date:
        filter_parts.append(f"(DATE<='{end_date}')")

    filter_str = "".join(filter_parts) if filter_parts else ""

    params = {
        "reportName": _RZRQ_REPORT_NAME,
        "columns": "ALL",
        "source": "WEB",
        "sortColumns": "DATE",
        "sortTypes": "-1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str

    try:
        url = _RZRQ_API + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not data.get("success") or not data.get("result", {}).get("data"):
        return "未查询到融资融券数据。"

    items = data["result"]["data"]
    total = data["result"].get("count", len(items))
    stock_name = items[0].get("SECNAME", "") if code else ""
    stock_code = items[0].get("SCODE", "") if code else ""

    def _fmt_yuan(v):
        """格式化金额（元→亿/万）"""
        try:
            val = float(v)
            if abs(val) >= 1e8:
                return f"{val/1e8:.2f}亿"
            elif abs(val) >= 1e4:
                return f"{val/1e4:.2f}万"
            else:
                return f"{val:.2f}元"
        except (TypeError, ValueError):
            return str(v) if v else "--"

    def _fmt_pct(v):
        """格式化百分比"""
        try:
            val = float(v)
            return f"{val:+.2f}%"
        except (TypeError, ValueError):
            return str(v) if v else "--"

    def _fmt_shares(v):
        """格式化股数"""
        try:
            val = int(float(v))
            return f"{val:,.0f}"
        except (TypeError, ValueError):
            return str(v) if v else "--"

    lines = [
        f"融资融券数据",
        f"   股票: {stock_name}({stock_code})  |  共 {total} 条记录",
        "=" * 80,
    ]

    header = (
        f"{'日期':<12} {'收盘价':>7} {'涨跌幅':>8} "
        f"{'融资余额':>12} {'融资买入':>10} {'融资偿还':>10} {'融资净买':>10} "
        f"{'融券余量':>10} {'融券卖出':>10} {'融券偿还':>10} {'融券净卖':>8} "
        f"{'两融余额':>12} {'融余占比':>8}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for item in items:
        date = (item.get("DATE") or "")[:10]
        spj = item.get("SPJ", "")
        zdf = item.get("ZDF", "")

        # 融资数据
        rzye = _fmt_yuan(item.get("RZYE", 0))
        rzmre = _fmt_yuan(item.get("RZMRE", 0))
        rzche = _fmt_yuan(item.get("RZCHE", 0))
        rzjme = _fmt_yuan(item.get("RZJME", 0))

        # 融券数据
        rqyl = _fmt_shares(item.get("RQYL", 0))
        rqmcl = _fmt_shares(item.get("RQMCL", 0))
        rqchl = _fmt_shares(item.get("RQCHL", 0))
        rqjmg = _fmt_shares(item.get("RQJMG", 0))

        # 综合
        rzrqye = _fmt_yuan(item.get("RZRQYE", 0))
        rzyezb = f"{item.get('RZYEZB', 0):.2f}%" if item.get("RZYEZB") is not None else "--"

        row = (
            f"{date:<12} {spj:>7} {_fmt_pct(zdf):>8} "
            f"{rzye:>12} {rzmre:>10} {rzche:>10} {rzjme:>10} "
            f"{rqyl:>10} {rqmcl:>10} {rqchl:>10} {rqjmg:>8} "
            f"{rzrqye:>12} {rzyezb:>8}"
        )
        lines.append(row)

    # ════════════════════════════════════════════
    # 趋势影响分析
    # ════════════════════════════════════════════
    if code and stock_name:
        latest = items[0]  # 最新一条（sortTypes=-1 倒序）
        prev = items[1] if len(items) > 1 else None
        
        # 提取最新数据
        cur_rzjme = float(latest.get("RZJME", 0))
        cur_rqjmg = float(latest.get("RQJMG", 0))
        cur_rzye = float(latest.get("RZYE", 0))
        cur_rqyl = float(latest.get("RQYL", 0))
        cur_zdf = float(latest.get("ZDF", 0))
        cur_spj = float(latest.get("SPJ", 0))
        
        # 计算融资余额变化方向
        rz_direction = None
        rq_direction = None
        if prev:
            prev_rzye = float(prev.get("RZYE", 0))
            prev_rqyl = float(prev.get("RQYL", 0))
            if prev_rzye > 0:
                rz_change = (cur_rzye - prev_rzye) / prev_rzye * 100
                rz_direction = "↑" if cur_rzye > prev_rzye else ("↓" if cur_rzye < prev_rzye else "→")
            if prev_rqyl > 0:
                rq_change = (cur_rqyl - prev_rqyl) / prev_rqyl * 100
                rq_direction = "↑" if cur_rqyl > prev_rqyl else ("↓" if cur_rqyl < prev_rqyl else "→")
        
        # 趋势分析信号
        signal = "中性"
        signal_color = "⚪"
        margin_desc = ""
        short_desc = ""
        
        # ── 融资分析 ──
        if cur_rzjme > 0 and cur_zdf > 0:
            margin_desc = f"融资净买入{_fmt_yuan(abs(cur_rzjme))} + 股价上涨{cur_zdf:+.2f}% → 多头加杠杆追涨，趋势有望延续"
            signal = "偏积极"
        elif cur_rzjme > 0 and cur_zdf <= 0:
            margin_desc = f"融资净买入{_fmt_yuan(abs(cur_rzjme))} + 股价下跌{cur_zdf:+.2f}% → 抄底资金入场，关注企稳信号"
            signal = "中性偏积极" if signal == "中性" else signal
        elif cur_rzjme < 0 and cur_zdf > 0:
            margin_desc = f"融资净偿还{_fmt_yuan(abs(cur_rzjme))} + 股价上涨{cur_zdf:+.2f}% → 多头获利了结，注意回调风险"
            signal = "偏谨慎"
        elif cur_rzjme < 0 and cur_zdf <= 0:
            margin_desc = f"融资净偿还{_fmt_yuan(abs(cur_rzjme))} + 股价下跌{cur_zdf:+.2f}% → 多头离场，趋势偏弱"
            signal = "偏消极"
        else:
            margin_desc = f"融资余额{_fmt_yuan(cur_rzye)}"
            if rz_direction:
                margin_desc += f"（{rz_direction}）"
        
        # ── 融券分析 ──
        if cur_rqjmg > 0:
            short_desc = f"融券净卖出{_fmt_shares(cur_rqjmg)}股 → 看空力量增强，短期或承压"
            if signal in ("偏积极", "中性偏积极"):
                signal = "中性（多空博弈）"
        elif cur_rqjmg < 0:
            short_desc = f"融券净偿还{_fmt_shares(abs(cur_rqjmg))}股 → 看空力量减弱，抛压缓解"
        else:
            short_desc = f"融券余量{_fmt_shares(cur_rqyl)}股"
            if rq_direction:
                short_desc += f"（{rq_direction}）"
        
        # 信号图标
        signal_icons = {
            "偏积极": "🟢",
            "中性偏积极": "🟡",
            "中性": "⚪",
            "中性（多空博弈）": "🟡",
            "偏谨慎": "🟠",
            "偏消极": "🔴",
        }
        sig_icon = signal_icons.get(signal, "⚪")
        
        # 综合研判
        analysis_lines = [
            "",
            "━" * 90,
            f"📊 融资融券趋势影响分析 — {stock_name}({stock_code})",
            "━" * 90,
            f"💰 融资: {margin_desc}",
        ]
        if short_desc:
            analysis_lines.append(f"📄 融券: {short_desc}")
        analysis_lines.append(f"🎯 综合信号: {sig_icon} {signal}")
        analysis_lines.append(f"💡 研判: {margin_desc}；{short_desc}。综合信号：{signal}。")
        analysis_lines.append(
            "    （融资融券反映市场情绪和杠杆资金动向，"
            "需结合成交量、市场环境综合判断，不构成投资建议）"
        )
        analysis_lines.append("━" * 90)
        
        return "\n".join(lines + analysis_lines)
    
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 高管持股变动数据工具
# ═══════════════════════════════════════════════════

_EXEC_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_EXEC_REPORT_NAME = "RPT_EXECUTIVE_HOLD_DETAILS"


@mcp.tool()
@_rate_limit_decorator
def query_executive_hold_change(
    code: str = "",
    start_date: str = "",
    end_date: str = "",
    person_name: str = "",
    direction: str = "",
    page_size: int = 30,
    page_number: int = 1,
) -> str:
    """
    查询指定股票的高管持股变动数据

    数据源：高管持股数据中心（RPT_EXECUTIVE_HOLD_DETAILS）
    涵盖高管增持/减持记录，含变动股数、成交均价、变动金额、变动原因等。

    Args:
        code:        股票代码，纯数字，如 "002517"。不传查全部
        start_date:  起始日期，格式 YYYY-MM-DD，如 "2026-01-01"
        end_date:    截止日期，格式 YYYY-MM-DD，如 "2026-06-10"
        person_name: 高管姓名关键字，如 "金锋"（可选）
        direction:   变动方向，"增持" 或 "减持"（可选，不传查全部）
        page_size:   每页条数，默认30，最大240
        page_number: 页码，默认1

    Returns:
        格式化的高管持股变动数据表格
    """
    page_size = max(1, min(page_size, 240))
    page_number = max(1, page_number)

    # 构造 filter：字符串字段用双引号，日期字段用单引号
    filter_parts = []
    if code:
        filter_parts.append(f'(SECURITY_CODE="{code}")')
    if person_name:
        filter_parts.append(f'(PERSON_NAME="{person_name}")')
    if start_date:
        filter_parts.append(f"(CHANGE_DATE>='{start_date}')")
    if end_date:
        filter_parts.append(f"(CHANGE_DATE<='{end_date}')")

    filter_str = "".join(filter_parts) if filter_parts else ""

    params = {
        "reportName": _EXEC_REPORT_NAME,
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "sortColumns": "CHANGE_DATE,SECURITY_CODE,PERSON_NAME",
        "sortTypes": "-1,1,1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str

    try:
        url = _EXEC_API + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not data.get("success") or not data.get("result", {}).get("data"):
        return "未查询到高管持股变动数据。"

    items = data["result"]["data"]
    total = data["result"].get("count", len(items))

    def _fmt_shares(v):
        """格式化股数"""
        try:
            val = int(float(v))
            return f"{val:,}"
        except (TypeError, ValueError):
            return str(v) if v else "--"

    def _fmt_money(v):
        """格式化金额（元）"""
        try:
            val = float(v)
            if abs(val) >= 1e8:
                return f"{val/1e8:.2f}亿"
            elif abs(val) >= 1e4:
                return f"{val/1e4:.2f}万"
            else:
                return f"{val:.2f}"
        except (TypeError, ValueError):
            return str(v) if v else "--"

    stock_name = items[0].get("SECURITY_NAME", "") if code else ""
    stock_code = items[0].get("SECURITY_CODE", "") if code else ""

    lines = [
        f"高管持股变动数据",
        f"   股票: {stock_name}({stock_code}){'  |  高管: '+person_name if person_name else ''}  |  共 {total} 条记录",
        "=" * 90,
    ]

    header = (
        f"{'日期':<12} {'代码':>7} {'名称':<6} {'变动人':<7} "
        f"{'方向':>4} {'变动股数':>12} {'均价(元)':>9} {'变动金额(万)':>12} "
        f"{'变动比例(‰)':>10} {'变动前':>10} {'变动后':>10} "
        f"{'职务':<10} {'原因':<12}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for item in items:
        date = (item.get("CHANGE_DATE") or "")[:10]
        scode = item.get("SECURITY_CODE", "")
        sname = item.get("SECURITY_NAME", "")
        pname = item.get("PERSON_NAME", "")

        shares = 0
        try:
            shares = int(float(item.get("CHANGE_SHARES", 0)))
        except (TypeError, ValueError):
            pass

        if shares > 0:
            direction_str = "增持"
            shares_str = f"+{shares:,}"
        elif shares < 0:
            direction_str = "减持"
            shares_str = f"{shares:,}"  # already has minus
        else:
            direction_str = "--"
            shares_str = _fmt_shares(item.get("CHANGE_SHARES", 0))

        avg_price = item.get("AVERAGE_PRICE", "")

        # 变动金额（元→万）
        change_amount = _fmt_money(item.get("CHANGE_AMOUNT", 0))

        # 变动比例(‰)
        change_ratio = item.get("CHANGE_RATIO", "")
        if change_ratio:
            try:
                change_ratio = f"{float(change_ratio):.4f}"
            except (TypeError, ValueError):
                pass
        else:
            change_ratio = "--"

        # 变动前/后持股
        begin_hold = _fmt_shares(item.get("BEGIN_HOLD_NUM", ""))
        end_hold = _fmt_shares(item.get("END_HOLD_NUM", ""))

        position = item.get("POSITION_NAME", "") or "--"
        reason = item.get("CHANGE_REASON", "") or "--"

        row = (
            f"{date:<12} {scode:>7} {sname:<6} {pname:<7} "
            f"{direction_str:>4} {shares_str:>12} {avg_price:>9} {change_amount:>12} "
            f"{change_ratio:>10} {begin_hold:>10} {end_hold:>10} "
            f"{position:<10} {reason:<12}"
        )
        lines.append(row)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 资金流向工具（通过 同花顺 stockpage.10jqka.com.cn）
# ═══════════════════════════════════════════════════

# 同花顺资金流向页面 URL 模板
_THS_FUNDS_URL = "https://stockpage.10jqka.com.cn/{code}/funds/"


@mcp.tool()
@_rate_limit_decorator
def query_money_flow(
    code: str,
    days: int = 10,
) -> str:
    """
    查询指定股票的每日资金流向数据（历史日级）

    数据源：同花顺 stockpage.10jqka.com.cn
    涵盖大单(主力)、中单、小单的每日净流入/流出及占比。
    数据每日盘后更新，最多提供最近60个交易日。

    Args:
        code: 股票代码，纯数字，如 "002517" 或 "601899"。不要带 .SZ / .SH 后缀
        days: 返回最近多少天的日级数据，默认10，最大60

    Returns:
        格式化的资金流向数据表格
    """
    import re

    # ── 1. 从同花顺获取资金流向页面 ──
    url = _THS_FUNDS_URL.format(code=code)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
    except Exception as e:
        return f"❌ 获取资金流向数据失败: {str(e)}"

    # ── 2. 去除 HTML 注释，然后提取 data rows ──
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)

    # 匹配每一行数据 <tr>...<td>...</td>...<td>...</td>...</tr>
    # 数据行包含日期（8位数字），非数据行不包含
    data_rows = []
    for tr_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL):
        tr_html = tr_match.group(1)
        if not re.search(r"20\d{6}", tr_html):
            continue  # 跳过非数据行
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr_html)
        # 清理每个 cell 的 HTML 标签
        clean = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        # 需要至少 9 个有效值（日期+收盘+涨跌幅+净流入+5日+大单额+大占比+中单额+中占比+小单额+小占比）
        if len(clean) >= 11:
            data_rows.append(clean)

    days = max(1, min(days, 60))
    data_rows = data_rows[:days]

    if not data_rows:
        return f"❌ 未查询到股票 [{code}] 的资金流向数据"

    # ── 3. 格式化输出 ──
    lines = [
        f"资金流向数据 [{code}]",
        f"数据源: 同花顺 (单位: 万元)",
        "=" * 75,
    ]

    header = (
        f"{'日期':<12} {'收盘价':>8} {'涨跌幅':>8} "
        f"{'资金净流入':>12} {'5日主力':>10} "
        f"{'大单净额':>12} {'大占比':>8} "
        f"{'中单净额':>12} {'中占比':>8} "
        f"{'小单净额':>12} {'小占比':>8}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for row in data_rows:
        if len(row) < 11:
            continue

        date = row[0]
        close = row[1]
        chg_pct = row[2]
        total_flow = row[3]       # 资金净流入(总)
        five_day_main = row[4]    # 5日主力净额
        main_amt = row[5]         # 大单(主力)净额
        main_pct = row[6]         # 大单(主力)净占比
        mid_amt = row[7]          # 中单净额
        mid_pct = row[8]          # 中单净占比
        small_amt = row[9]        # 小单净额
        small_pct = row[10]       # 小单净占比

        # 格式化日期：YYYYMMDD → YYYY-MM-DD
        if len(date) == 8:
            date_fmt = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        else:
            date_fmt = date

        # 收盘价统一保留两位小数
        try:
            close_fmt = f"{float(close):.2f}"
        except ValueError:
            close_fmt = close

        row_str = (
            f"{date_fmt:<12} {close_fmt:>8} {chg_pct:>8} "
            f"{total_flow:>12} {five_day_main:>10} "
            f"{main_amt:>12} {main_pct:>8} "
            f"{mid_amt:>12} {mid_pct:>8} "
            f"{small_amt:>12} {small_pct:>8}"
        )
        lines.append(row_str)

    lines.append("")
    lines.append("💡 说明：正数为净流入，负数为净流出。单位：万元")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 16：K线图生成
# ═══════════════════════════════════════════════════

@mcp.tool()
@_rate_limit_decorator
def generate_kline_chart(code: str, days: int = 60, output_dir: str = "E:/GuPiao/doc",
                         date: str = "") -> str:
    """
    生成指定股票的K线图（日K线 + 均线 + 今日分时走势），保存为PNG图片

    数据来源：
      日K线 → 腾讯财经(ifzq.gtimg.cn)
      今日分时 → 腾讯财经分钟API
      实时行情 → 新浪财经(hq.sinajs.cn)

    Args:
        code:       股票代码，纯数字，如 "601899" 或 "002455"。不要带 .SZ / .SH 后缀
        days:       显示最近多少个交易日（默认60，最大120）
        output_dir: 图片保存目录（默认 E:/GuPiao/doc）
        date:       指定日期，格式 YYYY-MM-DD 或 YYYYMMDD（可选，默认为今日）
                    e.g. "2026-06-11" 或 "20260611"
    """
    # ── 懒加载绘图依赖 ──
    try:
        import matplotlib
    except ImportError:
        return "❌ 缺少 matplotlib 库，请先执行: pip install matplotlib"

    matplotlib.use('Agg')

    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.font_manager as fm
    from matplotlib.patches import Rectangle
    from datetime import datetime, timedelta
    import os, json

    # ── 解析指定日期 ──
    if date:
        # 支持 YYYY-MM-DD 或 YYYYMMDD 两种格式
        clean = date.replace('-', '')
        if len(clean) == 8:
            ref_dt = datetime(int(clean[:4]), int(clean[4:6]), int(clean[6:8]))
        else:
            return f"❌ 日期格式错误: {date}，请使用 YYYY-MM-DD 或 YYYYMMDD"
    else:
        ref_dt = datetime.now()
    today_str = ref_dt.strftime('%Y%m%d')
    today_date = ref_dt.date()

    # ── 中文字体 ──
    zh_font_path = 'C:\\Windows\\Fonts\\simhei.ttf'
    if os.path.exists(zh_font_path):
        fm.fontManager.addfont(zh_font_path)
        plt.rcParams['font.sans-serif'] = ['SimHei']
    else:
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    ua = {'User-Agent': 'Mozilla/5.0'}
    market = 'sh' if code.startswith('6') else 'sz'
    tencent_code = f"{market}{code}"

    # ═══ 1. 获取日K线数据（腾讯财经） ═══
    try:
        # 计算起始日期
        start_dt = ref_dt - timedelta(days=days + 30)  # 多取一些确保足够
        start_date = start_dt.strftime('%Y-%m-%d')
        end_date = ref_dt.strftime('%Y-%m-%d')
        param = f'{tencent_code},day,{start_date},{end_date},{days + 30},qfq'
        req = urllib.request.Request(
            f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={urllib.parse.quote(param)}',
            headers=ua
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))

        if not (data.get('data') and data['data'].get(tencent_code)):
            return f"❌ 获取日K线数据失败：{code}"

        day_data = data['data'][tencent_code].get('qfqday', [])
        if not day_data:
            day_data = data['data'][tencent_code].get('day', [])
        if not day_data:
            day_data = data['data'][tencent_code].get('data', [])

        if not day_data:
            return f"❌ 未找到 {code} 的日K线数据"

        dates_k, opens_k, closes_k, highs_k, lows_k, volumes_k = [], [], [], [], [], []
        for d in day_data:
            ds = str(d[0])
            if len(ds) == 10:  # YYYY-MM-DD
                dt = datetime(int(ds[:4]), int(ds[5:7]), int(ds[8:10]))
            elif len(ds) == 8:  # YYYYMMDD
                dt = datetime(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
            else:
                continue
            dates_k.append(dt)
            opens_k.append(float(d[1]))
            closes_k.append(float(d[2]))
            highs_k.append(float(d[3]))
            lows_k.append(float(d[4]))
            volumes_k.append(float(d[5]) if d[5] else 0)

        # ═══ 2. 获取今日实时行情（新浪财经） ═══
        try:
            sina_req = urllib.request.Request(
                f'http://hq.sinajs.cn/list={tencent_code}',
                headers={**ua, 'Referer': 'https://finance.sina.com.cn'}
            )
            with urllib.request.urlopen(sina_req, timeout=10) as resp:
                raw = resp.read().decode('gbk')
            sf = raw.split(',')
            today_open = float(sf[1]) if sf[1] else closes_k[-1]
            yest_close = float(sf[2]) if sf[2] else closes_k[-1]
            today_close = float(sf[3]) if sf[3] else closes_k[-1]
            today_high = float(sf[4]) if sf[4] else highs_k[-1]
            today_low = float(sf[5]) if sf[5] else lows_k[-1]
            today_vol = int(sf[8]) if sf[8] else 0
            today_amt = float(sf[9]) if sf[9] else 0
            sina_ok = True
        except Exception:
            sina_ok = False
            today_open = today_close = today_high = today_low = yest_close = 0
            today_vol = 0

        # 如果指定日期已收盘且与最新日K线日期不同，追加当日数据
        if sina_ok and (not dates_k or dates_k[-1].date() != today_date):
            dates_k.append(ref_dt)
            opens_k.append(today_open)
            closes_k.append(today_close)
            highs_k.append(today_high)
            lows_k.append(today_low)
            volumes_k.append(today_vol)

        # ═══ 3. 获取今日分钟数据 ═══
        min_times, min_prices = [], []
        try:
            min_req = urllib.request.Request(
                f'http://ifzq.gtimg.cn/appstock/app/minute/query?code={tencent_code}&_var=min_data',
                headers=ua
            )
            with urllib.request.urlopen(min_req, timeout=10) as resp:
                min_raw = resp.read().decode('utf-8')
            if 'min_data=' in min_raw:
                min_json = min_raw.split('min_data=', 1)[1].rstrip(';')
                min_data = json.loads(min_json)
                min_records = min_data['data'][tencent_code]['data']['data']
                for rec in min_records:
                    parts = rec.split()
                    hhmm, price = int(parts[0]), float(parts[1])
                    mt = ref_dt.replace(hour=hhmm // 100, minute=hhmm % 100, second=0)
                    min_times.append(mt)
                    min_prices.append(price)
            minutes_ok = len(min_times) > 0
        except Exception:
            minutes_ok = False

    except Exception as e:
        return f"❌ 获取K线数据失败：{str(e)}"

    # ═══ 4. 绘图 ═══
    try:
        fig = plt.figure(figsize=(16, 11))
        fig.patch.set_facecolor('#0f0f1a')

        ax = plt.subplot2grid((6, 4), (0, 0), rowspan=3, colspan=4, facecolor='#1a1a2e')
        ax_vol = plt.subplot2grid((6, 4), (3, 0), rowspan=1, colspan=4, facecolor='#1a1a2e')
        ax_min = plt.subplot2grid((6, 4), (4, 0), rowspan=2, colspan=4, facecolor='#1a1a2e')

        # ── 日K线图 ──
        last_close = closes_k[-1]
        ax.set_title(f'{code} 日K线图  (截至{today_str[:4]}-{today_str[4:6]}-{today_str[6:]})',
                     color='white', fontsize=16, fontweight='bold', pad=15)

        n = min(days, len(dates_k))
        d_plot = dates_k[-n:]
        o_plot = opens_k[-n:]
        c_plot = closes_k[-n:]
        h_plot = highs_k[-n:]
        l_plot = lows_k[-n:]
        v_plot = volumes_k[-n:]

        # 均线
        close_arr = c_plot
        n5 = min(5, len(close_arr))
        n10 = min(10, len(close_arr))
        n20 = min(20, len(close_arr))
        n60 = min(60, len(close_arr))

        import numpy as np
        def rolling_mean(data, window):
            arr = np.array(data, dtype=float)
            if len(arr) < window:
                return [np.nan] * len(arr)
            ret = np.full(len(arr), np.nan)
            for i in range(window - 1, len(arr)):
                ret[i] = np.mean(arr[i - window + 1:i + 1])
            return ret

        ma5 = rolling_mean(close_arr, n5)
        ma10 = rolling_mean(close_arr, n10)
        ma20 = rolling_mean(close_arr, n20)
        ma60 = rolling_mean(close_arr, n60)

        p_min = min(l_plot) * 0.97
        p_max = max(h_plot) * 1.03
        bw = 0.6

        for i in range(n):
            is_up = c_plot[i] >= o_plot[i]
            color = '#ff4757' if is_up else '#2ed573'
            ax.plot([i, i], [l_plot[i], h_plot[i]], color=color, linewidth=0.8, alpha=0.6)
            if is_up:
                rect = Rectangle((i - bw/2, o_plot[i]), bw, c_plot[i] - o_plot[i],
                                facecolor=color, edgecolor=color, alpha=0.85)
            else:
                rect = Rectangle((i - bw/2, c_plot[i]), bw, o_plot[i] - c_plot[i],
                                facecolor=color, edgecolor=color, alpha=0.85)
            ax.add_patch(rect)

        x = range(n)
        ax.plot(x, ma5, color='#ffd93d', linewidth=1.1, label='MA5', alpha=0.8)
        ax.plot(x, ma10, color='#6bcbff', linewidth=1.1, label='MA10', alpha=0.8)
        ax.plot(x, ma20, color='#b8b8ff', linewidth=1.1, label='MA20', alpha=0.8)
        ax.plot(x, ma60, color='#7bed9f', linewidth=0.8, label='MA60', alpha=0.6)

        ax.set_xlim(-0.5, n - 0.5)
        ax.set_ylim(p_min, p_max)
        ax.set_ylabel('价格 (元)', color='white', fontsize=10)
        ax.tick_params(colors='white', labelsize=8)
        ax.legend(loc='upper left', facecolor='#1a1a2e', edgecolor='gray', labelcolor='white', fontsize=9)
        ax.grid(True, alpha=0.12, color='gray')

        tick_n = max(1, n // 8)
        tick_idx = list(range(0, n, tick_n))
        if tick_idx[-1] != n - 1:
            tick_idx.append(n - 1)
        ax.set_xticks(tick_idx)
        ax.set_xticklabels([d_plot[i].strftime('%m-%d') for i in tick_idx], color='white', fontsize=8)

        # 最新价标注
        ax.axhline(y=last_close, color='white', linestyle='--', linewidth=0.7, alpha=0.4, xmin=0.85)
        bbox = dict(boxstyle='round,pad=0.3', facecolor='#ff4757', alpha=0.85)
        ax.annotate(f'{last_close:.2f}', xy=(n - 1, last_close),
                    xytext=(n + 0.5, last_close), color='white', fontsize=12, fontweight='bold', bbox=bbox)

        # ── 成交量 ──
        for i in range(n):
            color = '#ff4757' if c_plot[i] >= o_plot[i] else '#2ed573'
            ax_vol.bar(i, v_plot[i] / 10000, width=bw, color=color, alpha=0.5)
        ax_vol.set_ylabel('万股', color='white', fontsize=8)
        ax_vol.tick_params(colors='white', labelsize=7)
        ax_vol.set_xticks(tick_idx)
        ax_vol.set_xticklabels([d_plot[i].strftime('%m-%d') for i in tick_idx], color='white', fontsize=8)
        ax_vol.grid(True, alpha=0.12, color='gray')
        ax_vol.set_title('成交量', color='white', fontsize=10, pad=8)

        # ── 今日分时 ──
        if minutes_ok and sina_ok:
            ax_min.set_title(
                f'今日分时走势  ({today_str[:4]}-{today_str[4:6]}-{today_str[6:]} | '
                f'开盘{today_open:.2f} 最高{today_high:.2f} 最低{today_low:.2f} 收盘{today_close:.2f})',
                color='white', fontsize=12, pad=10)

            ax_min.plot(min_times, min_prices, color='#ffd93d', linewidth=2, label='价格')
            ax_min.axhline(y=yest_close, color='#888', linestyle='--', linewidth=0.8, alpha=0.6,
                          label=f'昨收{yest_close:.2f}')

            ax_min.fill_between(min_times, min_prices, yest_close,
                               where=[p >= yest_close for p in min_prices],
                               color='#ff4757', alpha=0.12)
            ax_min.fill_between(min_times, min_prices, yest_close,
                               where=[p < yest_close for p in min_prices],
                               color='#2ed573', alpha=0.12)

            ax_min.set_ylabel('价格', color='white', fontsize=9)
            ax_min.tick_params(colors='white', labelsize=8)
            ax_min.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax_min.xaxis.set_major_locator(mdates.HourLocator(interval=1))
            ax_min.set_xlim(min_times[0], min_times[-1])
            ax_min.grid(True, alpha=0.12, color='gray')
            ax_min.legend(loc='upper left', facecolor='#1a1a2e', edgecolor='gray',
                         labelcolor='white', fontsize=9)

            # 标注日内高点低点
            max_p = max(min_prices)
            min_p = min(min_prices)
            t_max = min_times[min_prices.index(max_p)]
            t_min = min_times[min_prices.index(min_p)]
            ax_min.annotate(f'H:{max_p:.2f}', xy=(t_max, max_p),
                           xytext=(t_max, max_p + 0.03), color='#ff4757', fontsize=10, fontweight='bold',
                           arrowprops=dict(arrowstyle='->', color='#ff4757', lw=1.2))
            ax_min.annotate(f'L:{min_p:.2f}', xy=(t_min, min_p),
                           xytext=(t_min, min_p - 0.03), color='#2ed573', fontsize=10, fontweight='bold',
                           arrowprops=dict(arrowstyle='->', color='#2ed573', lw=1.2))
        else:
            msg_parts = []
            if not minutes_ok:
                msg_parts.append("分时数据")
            if not sina_ok:
                msg_parts.append("实时行情")
            ax_min.text(0.5, 0.5, f'今日{"、".join(msg_parts)}获取失败',
                       color='gray', fontsize=14, ha='center', va='center', transform=ax_min.transAxes)

        plt.tight_layout(pad=2)

        # ── 保存 ──
        os.makedirs(output_dir, exist_ok=True)
        filename = f'{code}_kline_{today_str}.png'
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='#0f0f1a')
        plt.close()

        filesize = os.path.getsize(filepath)

        # ── 返回信息 ──
        lines = [
            f"✅ K线图已生成",
            f"{'=' * 50}",
            f"  保存路径: {filepath}",
            f"  文件大小: {filesize:,} 字节",
            f"  展示周期: {d_plot[0].strftime('%Y-%m-%d')} ~ {d_plot[-1].strftime('%Y-%m-%d')}",
            f"  交易日数: {n} 天",
            f"  当前价格: {last_close:.2f} 元",
            f"  今日数据: {'✅ 已包含' if sina_ok else '❌ 未获取'}",
            f"  分时图:   {'✅ 已绘制' if minutes_ok else '❌ 未绘制'}",
        ]
        if sina_ok:
            change_pct = (today_close - yest_close) / yest_close * 100
            lines.append(f"  今日行情: O={today_open:.2f} H={today_high:.2f} L={today_low:.2f} C={today_close:.2f} "
                        f"{'+' if change_pct > 0 else ''}{change_pct:.2f}%")
        return "\n".join(lines)

    except Exception as e:
        return f"❌ 生成K线图失败：{str(e)}"


def main():
    """启动 MCP Server 入口函数"""
    print("=" * 50)
    print("  股海罗盘 GH-Data MCP Server v1.0.0")
    print("  16个工具已就绪")
    print("=" * 50)
    mcp.run()


if __name__ == "__main__":
    main()
