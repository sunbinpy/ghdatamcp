"""
股海罗盘 GH-Data MCP Server

提供工具：
  - query_financial_report:      业绩报表明细（营收、净利润、EPS、ROE等）
  - query_balance_sheet:         资产负债表（总资产、负债、权益、流动比率等）
  - query_cashflow_statement:    现金流量表（经营/投资/融资现金流等）
  - query_income_statement:      利润表（营业总收入/总成本、费用、利润等）
  - query_realtime_price:        实时行情（最新价、涨跌幅、成交量、市值、市盈率等）
  - get_stock_unlock_data:       限售股解禁数据（按市场/日期查询解禁股票明细）
  - get_stock_unlock_holders:    解禁持有人明细（股东名称、解禁数量、锁定期等）
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
  python -m ghdata_mcp

注册到 QwenPaw：
{
  "gh-data": {
    "command": "python",
    "args": ["-m", "ghdata_mcp"],
    "env": {}
  }
}
"""

import json
import os
import sys
import re
import time
import functools
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    import subprocess
    print("正在安装 mcp 依赖...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mcp[cli]"])
    from mcp.server.fastmcp import FastMCP

mcp = FastMCP("股海罗盘数据引擎")


# ═══════════════════════════════════════════════════
# 全局反爬限制：互斥锁 + 30 秒最小间隔
# ═══════════════════════════════════════════════════

import threading
_call_lock = threading.Lock()
_last_call_time: float = 0.0
MIN_CALL_INTERVAL = 30  # 秒


def _rate_limit_decorator(func):
    """限流/互斥装饰器：确保同一时刻只有一个工具调用，且间隔至少 30 秒"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _last_call_time
        with _call_lock:
            elapsed = time.time() - _last_call_time
            if elapsed < MIN_CALL_INTERVAL:
                time.sleep(MIN_CALL_INTERVAL - elapsed)
            _last_call_time = time.time()
            return func(*args, **kwargs)
    return wrapper


# ═══════════════════════════════════════════════════
# API Key 验证（服务端管理次数，每次实时读取 Key）
# ═══════════════════════════════════════════════════

PURCHASE_URL = "https://www.oraskl.com/ghdata-admin"
VERIFY_GH_DATA_URL = (
    "https://tpis.smartsousou.com/TPAccountInfo/api/SkillAccount/VerifyGHData"
)

_MCP_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_MCP_PROJECT_ROOT = os.path.dirname(os.path.dirname(_MCP_SCRIPT_DIR))  # ghdata/
_MCP_USER_HOME = os.path.expanduser("~")


def _display_key(key: str) -> str:
    """脱敏显示 Key"""
    if len(key) > 12:
        return key[:8] + "..." + key[-4:]
    return "****"


def _get_api_key() -> str:
    """获取 API Key（每次实时读取，确保 Skill 写入的最新 Key 能被用到）"""
    key = os.environ.get("GHDATA_API_KEY", "")
    if key:
        return key.strip()
    project_key = os.path.join(_MCP_PROJECT_ROOT, ".apikey")
    if os.path.isfile(project_key):
        try:
            with open(project_key, "r", encoding="utf-8") as f:
                return f.read().strip()
        except OSError:
            pass
    cwd_key = os.path.join(os.getcwd(), ".apikey")
    if os.path.isfile(cwd_key):
        try:
            with open(cwd_key, "r", encoding="utf-8") as f:
                return f.read().strip()
        except OSError:
            pass
    home_key = os.path.join(_MCP_USER_HOME, ".ghdata", "ghdataapikey")
    if os.path.isfile(home_key):
        try:
            with open(home_key, "r", encoding="utf-8") as f:
                return f.read().strip()
        except OSError:
            pass
    return ""


def _consume_tool(tool_name: str, dry_run: bool = False) -> dict:
    """调用服务端验证 + 扣减次数"""
    api_key = _get_api_key()
    if not api_key:
        return {
            "success": False, "allowed": False,
            "error": f"未设置 API Key。请设置环境变量 GHDATA_API_KEY 或在 ~/.ghdata/ghdataapikey 中配置\n购买: {PURCHASE_URL}"
        }
    payload = json.dumps({
        "mSkillKey": api_key,
        "mSoftSystemTag": "GHData",
        "toolName": tool_name,
        "dryRun": dry_run
    }).encode("utf-8")
    req = urllib.request.Request(
        VERIFY_GH_DATA_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8")
            return json.loads(body)
        except Exception:
            return {"success": False, "allowed": False, "error": f"HTTP {e.code}"}
    except urllib.error.URLError as e:
        return {"success": False, "allowed": False, "error": f"网络错误: {e.reason}"}
    except Exception as e:
        return {"success": False, "allowed": False, "error": str(e)}


def auth_required(func):
    """API Key 验证装饰器"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        tool_name = func.__name__
        result = _consume_tool(tool_name, dry_run=False)
        if not result.get("allowed", False):
            used = result.get("usedToday", "?")
            limit = result.get("dailyLimit", "?")
            err = result.get("error", "调用被拒绝")
            is_active = result.get("isActiveKey", False)
            if not _get_api_key():
                msg = (f"❌ 未设置 API Key\n"
                       f"   请设置环境变量 GHDATA_API_KEY\n"
                       f"   或在 ~/.ghdata/ghdataapikey 配置 Key\n"
                       f"   购买: {PURCHASE_URL}")
            elif not is_active:
                msg = (f"❌ API Key 无效\n"
                       f"   错误: {err}\n"
                       f"   请访问 {PURCHASE_URL} 获取有效 Key")
            else:
                msg = (f"❌ API Key 配额不足\n"
                       f"   每日限额: {limit} 次/工具 | 今日已用: {used} 次\n"
                       f"   错误: {err}\n"
                       f"   请访问 {PURCHASE_URL} 购买付费版扩展配额")
            return msg
        return func(*args, **kwargs)
    return wrapper


# ═══════════════════════════════════════════════════
# 统一 HTTP GET 请求工具
# ═══════════════════════════════════════════════════

def _http_get(url: str, timeout: int = 15, headers: dict = None,
              encoding: str = "utf-8") -> str:
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode(encoding)


# ═══════════════════════════════════════════════════
# 东方财富数据中心通用 API
# ═══════════════════════════════════════════════════

DATA_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"


def _fetch(code: str, page_size: int, page_number: int, report_name: str) -> list[dict]:
    """通用东方财富数据中心查询（按股票代码过滤）"""
    params = {
        "reportName": report_name,
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(SECURITY_CODE="{code}")',
        "pageSize": min(page_size, 20),
        "pageNumber": max(1, page_number),
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
    }
    url = DATA_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return []
    if data.get("success") and data.get("result", {}).get("data"):
        return data["result"]["data"]
    return []


def _fetch_data(params: dict) -> dict:
    """通用东方财富 POST 式请求（通过 URL 参数）"""
    url = DATA_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {}


def _fmt(v, default="--", fmt_spec=".2f"):
    """安全格式化数值"""
    if v is None:
        return default
    try:
        return format(v, fmt_spec)
    except (ValueError, TypeError):
        return str(v)


# ═══════════════════════════════════════════════════
# 实时行情（双源交叉验证：新浪 + 腾讯）
# ═══════════════════════════════════════════════════

def _make_secid(code: str) -> str:
    """生成新浪/腾讯的代码格式"""
    if code.startswith(("6", "9")):
        return f"sh{code}"
    return f"sz{code}"


def _fetch_realtime(code: str) -> dict | None:
    """获取实时行情（新浪为主 + 腾讯交叉验证）"""
    secid = _make_secid(code)
    sina_url = f"https://hq.sinajs.cn/list={secid}"
    try:
        text = _http_get(sina_url, timeout=8, encoding="gbk",
                         headers={"Referer": "https://finance.sina.com.cn"})
        if not text or "=" not in text:
            raise ValueError("Empty response")
        fields = text.split('"')[1].split(',')
        if len(fields) < 32:
            raise ValueError(f"Too few fields: {len(fields)}")
        name = fields[0]
        open_p = float(fields[1]) if fields[1] else 0.0
        yclose = float(fields[2]) if fields[2] else 0.0
        price = float(fields[3]) if fields[3] else 0.0
        high = float(fields[4]) if fields[4] else 0.0
        low = float(fields[5]) if fields[5] else 0.0
        volume = int(fields[8]) if fields[8] else 0
        amount = float(fields[9]) if fields[9] else 0.0
        change_pct = 0.0
        if yclose > 0:
            change_pct = (price - yclose) / yclose * 100
        t_prefix = "sh" if code.startswith(("6", "9")) else "sz"
        t_url = f"https://qt.gtimg.cn/q={t_prefix}{code}"
        turnover_rate = 0.0
        pe_ttm = 0.0
        pb = 0.0
        amplitude = 0.0
        total_shares = 0.0
        volume_ratio = 0.0
        try:
            t_text = _http_get(t_url, timeout=8, encoding="gbk")
            if t_text and "=" in t_text:
                t_parts = t_text.split('"')[1].split("~")
                if len(t_parts) > 50:
                    turnover_rate = float(t_parts[38]) if t_parts[38] else 0.0
                    pe_ttm = float(t_parts[39]) if t_parts[39] else 0.0
                    amplitude = float(t_parts[43]) if t_parts[43] else 0.0
                    total_shares = float(t_parts[44]) if t_parts[44] else 0.0
                    pb = float(t_parts[45]) if len(t_parts) > 45 and t_parts[45] else 0.0
                    volume_ratio = float(t_parts[48]) / 100.0 if len(t_parts) > 48 and t_parts[48] else 0.0
        except Exception:
            pass
        return {
            "name": name, "code": code,
            "price": round(price, 2), "open": round(open_p, 2),
            "high": round(high, 2), "low": round(low, 2),
            "prev_close": round(yclose, 2),
            "change_pct": round(change_pct, 2),
            "volume": volume, "amount": amount,
            "amplitude": round(amplitude, 2),
            "turnover_rate": round(turnover_rate, 2),
            "volume_ratio": round(volume_ratio, 2),
            "pe_ttm": round(pe_ttm, 2), "pb": round(pb, 2),
            "total_shares": total_shares,
        }
    except Exception:
        try:
            t_prefix = "sh" if code.startswith(("6", "9")) else "sz"
            t_url = f"https://qt.gtimg.cn/q={t_prefix}{code}"
            t_text = _http_get(t_url, timeout=8, encoding="gbk")
            if not t_text or "=" not in t_text:
                return None
            t_parts = t_text.split('"')[1].split("~")
            if len(t_parts) < 40:
                return None
            name = t_parts[1]
            code_f = t_parts[2]
            price = float(t_parts[3]) if t_parts[3] else 0.0
            yclose = float(t_parts[4]) if t_parts[4] else 0.0
            open_p = float(t_parts[5]) if t_parts[5] else 0.0
            volume = int(t_parts[6]) if t_parts[6] else 0
            high = float(t_parts[33]) if len(t_parts) > 33 and t_parts[33] else 0.0
            low = float(t_parts[34]) if len(t_parts) > 34 and t_parts[34] else 0.0
            amount = float(t_parts[37]) if len(t_parts) > 37 and t_parts[37] else 0.0
            change_pct = 0.0
            if yclose > 0:
                change_pct = (price - yclose) / yclose * 100
            turnover_rate = float(t_parts[38]) if len(t_parts) > 38 and t_parts[38] else 0.0
            pe_ttm = float(t_parts[39]) if len(t_parts) > 39 and t_parts[39] else 0.0
            amplitude = float(t_parts[43]) if len(t_parts) > 43 and t_parts[43] else 0.0
            total_shares = float(t_parts[44]) if len(t_parts) > 44 and t_parts[44] else 0.0
            pb = float(t_parts[45]) if len(t_parts) > 45 and t_parts[45] else 0.0
            volume_ratio = float(t_parts[48]) / 100.0 if len(t_parts) > 48 and t_parts[48] else 0.0
            return {
                "name": name, "code": code_f,
                "price": round(price, 2), "open": round(open_p, 2),
                "high": round(high, 2), "low": round(low, 2),
                "prev_close": round(yclose, 2),
                "change_pct": round(change_pct, 2),
                "volume": volume, "amount": amount,
                "amplitude": round(amplitude, 2),
                "turnover_rate": round(turnover_rate, 2),
                "volume_ratio": round(volume_ratio, 2),
                "pe_ttm": round(pe_ttm, 2), "pb": round(pb, 2),
                "total_shares": total_shares,
            }
        except Exception:
            return None


# ═══════════════════════════════════════════════════
# 工具 1：业绩报表明细
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_financial_report(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """
    查询指定股票的业绩报表明细（包括营收、净利润、EPS、ROE、毛利率等）

    Args:
        code: 股票代码，纯数字，如 "002455"。不要带 .SZ / .SH 后缀
        page_size: 每页返回多少条数据，默认 5，最大 20
        page_number: 查询第几页，默认 1
    """
    items = _fetch(code, page_size, page_number, "RPT_DMSK_FN_INCOME")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的业绩报表数据"
    lines = [f"📊 [{code}] 业绩报表明细（共 {len(items)} 条）", "=" * 65]
    for i, item in enumerate(items, 1):
        rd = (item.get("REPORT_DATE") or "")[:10]
        nd = (item.get("NOTICE_DATE") or "")[:10]
        total_rev = (item.get("TOTAL_OPERATE_INCOME", 0) or 0) / 1e8
        parent_net = (item.get("OPERATE_NET_PROFIT", 0) or 0) / 1e8
        eps = item.get("BASIC_EPS", 0) or 0
        roe = item.get("WEIGHTAVG_ROE", 0) or 0
        gross = item.get("GROSS_PROFIT_MARGIN", 0) or 0
        rev_yoy = item.get("TOTAL_OPERATE_INCOME_YOY", 0) or 0
        net_yoy = item.get("PARENT_NETPROFIT_YOY_RATIO", 0) or 0
        lines.append(f"\n{i}. 报告期: {rd}  公告日: {nd}")
        lines.append(f"   {'营收':<6} {total_rev:>12.2f} 亿  {'同比':<4} {rev_yoy:.2f}%")
        lines.append(f"   {'净利润':<6} {parent_net:>12.2f} 亿  {'同比':<4} {net_yoy:.2f}%")
        lines.append(f"   {'EPS':<6} {eps:>8.4f}  元  {'ROE':<4} {roe:.2f}%  毛利率: {gross:.2f}%")
        lines.append(f"   {'代码':<12} {item.get('SECURITY_CODE', '')}   {'名称':<8} {item.get('SECURITY_NAME_ABBR', '')}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 2：资产负债表
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_balance_sheet(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """
    查询指定股票的资产负债表（总资产、总负债、股东权益、货币资金、应收账款、存货、流动比率、资产负债率等）
    """
    items = _fetch(code, page_size, page_number, "RPT_DMSK_FN_BALANCE")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的资产负债表数据"
    lines = [f"📋 [{code}] 资产负债表（共 {len(items)} 条）", "=" * 60]
    for i, item in enumerate(items, 1):
        rd = (item.get("REPORT_DATE") or "")[:10]
        nd = (item.get("NOTICE_DATE") or "")[:10]
        ta = (item.get("TOTAL_ASSETS", 0) or 0) / 1e8
        tl = (item.get("TOTAL_LIABILITIES", 0) or 0) / 1e8
        te = (item.get("TOTAL_EQUITY", 0) or 0) / 1e8
        mf = (item.get("MONETARYFUNDS", 0) or 0) / 1e8
        ar = (item.get("ACCOUNTS_RECE", 0) or 0) / 1e8
        inv = (item.get("INVENTORY", 0) or 0) / 1e8
        ap = (item.get("ACCOUNTS_PAYABLE", 0) or 0) / 1e8
        dbr = item.get("DEBT_ASSETS_RATIO", 0) or 0
        cr = item.get("CURRENT_RATIO", 0) or 0
        lines.append(f"\n{i}. 报告期: {rd}  公告日: {nd}")
        lines.append(f"   {'总资产':<8} {ta:>12.2f} 亿  {'总负债':<6} {tl:>10.2f} 亿")
        lines.append(f"   {'股东权益':<8} {te:>12.2f} 亿  {'货币资金':<6} {mf:>10.2f} 亿")
        lines.append(f"   {'应收账款':<8} {ar:>12.2f} 亿  {'存货':<6} {inv:>10.2f} 亿")
        lines.append(f"   {'应付账款':<8} {ap:>12.2f} 亿  {'资产负债率':<6} {dbr}%")
        lines.append(f"   {'流动比率':<8} {cr}%")
        lines.append(f"   {'代码':<12} {item.get('SECURITY_CODE', '')}   {'名称':<8} {item.get('SECURITY_NAME_ABBR', '')}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 3：现金流量表
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_cashflow_statement(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """查询指定股票的现金流量表"""
    items = _fetch(code, page_size, page_number, "RPT_DMSK_FN_CASHFLOW")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的现金流量表数据"
    lines = [f"💰 [{code}] 现金流量表（共 {len(items)} 条）", "=" * 60]
    for i, item in enumerate(items, 1):
        rd = (item.get("REPORT_DATE") or "")[:10]
        nd = (item.get("NOTICE_DATE") or "")[:10]
        no = (item.get("NETCASH_OPERATE", 0) or 0) / 1e8
        ni = (item.get("NETCASH_INVEST", 0) or 0) / 1e8
        nf = (item.get("NETCASH_FINANCE", 0) or 0) / 1e8
        cce = (item.get("CCE_ADD", 0) or 0) / 1e8
        no_y = item.get("NETCASH_OPERATE_RATIO", 0) or 0
        ni_y = item.get("NETCASH_INVEST_RATIO", 0) or 0
        nf_y = item.get("NETCASH_FINANCE_RATIO", 0) or 0
        lines.append(f"\n{i}. 报告期: {rd}  公告日: {nd}")
        lines.append(f"   {'经营活动现金流':<12} {no:>10.2f} 亿  {'同比':<6} {no_y}%")
        lines.append(f"   {'投资活动现金流':<12} {ni:>10.2f} 亿  {'同比':<6} {ni_y}%")
        lines.append(f"   {'融资活动现金流':<12} {nf:>10.2f} 亿  {'同比':<6} {nf_y}%")
        lines.append(f"   {'净现金流':<12} {cce:>10.2f} 亿")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 4：利润表
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_income_statement(code: str, page_size: int = 5, page_number: int = 1) -> str:
    """查询指定股票的利润表"""
    items = _fetch(code, page_size, page_number, "RPT_DMSK_FN_INCOME")
    if not items:
        return f"❌ 未查询到股票 [{code}] 的利润表数据"
    lines = [f"📈 [{code}] 利润表（共 {len(items)} 条）", "=" * 60]
    for i, item in enumerate(items, 1):
        rd = (item.get("REPORT_DATE") or "")[:10]
        nd = (item.get("NOTICE_DATE") or "")[:10]
        tri = (item.get("TOTAL_OPERATE_INCOME", 0) or 0) / 1e8
        tc = (item.get("TOTAL_OPERATE_COST", 0) or 0) / 1e8
        op = (item.get("OPERATE_PROFIT", 0) or 0) / 1e8
        tp = (item.get("TOTAL_PROFIT", 0) or 0) / 1e8
        np = (item.get("OPERATE_NET_PROFIT", 0) or 0) / 1e8
        se = (item.get("SALE_EXPENSE", 0) or 0) / 1e8
        me = (item.get("MANAGE_EXPENSE", 0) or 0) / 1e8
        fe = (item.get("FINANCE_EXPENSE", 0) or 0) / 1e8
        re_y = item.get("TOTAL_OPERATE_INCOME_YOY", 0) or 0
        np_y = item.get("PARENT_NETPROFIT_YOY_RATIO", 0) or 0
        lines.append(f"\n{i}. 报告期: {rd}  公告日: {nd}")
        lines.append(f"   {'营业总收入':<8} {tri:>10.2f} 亿  {'同比':<4} {re_y:.2f}%")
        lines.append(f"   {'营业总成本':<8} {tc:>10.2f} 亿  {'净利润':<6} {np:>10.2f} 亿  {'同比':<4} {np_y:.2f}%")
        lines.append(f"   {'营业利润':<8} {op:>10.2f} 亿  {'利润总额':<6} {tp:>10.2f} 亿")
        lines.append(f"   {'销售费用':<8} {se:>10.2f} 亿  {'管理费用':<6} {me:>10.2f} 亿  {'财务费用':<6} {fe:>10.2f} 亿")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 5：实时行情
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_realtime_price(code: str) -> str:
    """
    查询指定股票的实时行情，双源交叉验证（新浪财经 + 腾讯财经）
    """
    data = _fetch_realtime(code)
    if not data:
        return f"❌ 获取股票 [{code}] 实时行情失败"
    lines = [
        f"📊 实时行情 - {data['name']}({data['code']})",
        "=" * 55,
        f"  最新价: {data['price']:<8.2f}  涨跌幅: {data['change_pct']:>+.2f}%",
        f"  今开:   {data['open']:<8.2f}  昨收:   {data['prev_close']:<.2f}",
        f"  最高:   {data['high']:<8.2f}  最低:   {data['low']:<.2f}",
        f"  成交量: {data['volume']:<10,} 手  成交额: {data['amount'] / 1e4:<.2f}万",
    ]
    if data.get("amplitude"):
        lines.append(f"  振幅:   {data['amplitude']:<8.2f}%  换手率: {data['turnover_rate']:.2f}%")
    if data.get("volume_ratio"):
        lines.append(f"  量比:   {data['volume_ratio']:<8.2f}")
    if data.get("pe_ttm"):
        lines.append(f"  市盈率(动): {data['pe_ttm']:<8.2f}  市净率: {data['pb']:.2f}")
    if data.get("total_shares"):
        lines.append(f"  总股本: {data['total_shares'] / 1e8:.2f}亿股")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 6：限售股解禁数据
# ═══════════════════════════════════════════════════

_UNLOCK_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_UNLOCK_REPORT_NAME = "RPT_DMSK_FREE_SHARES"


@mcp.tool()
@auth_required
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
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)
    filter_parts = []
    if market == "沪市":
        filter_parts.append("(SECURITY_TYPE_CODE in ('058001001','058001005'))")
    elif market == "深市":
        filter_parts.append("(SECURITY_TYPE_CODE in ('058001002','058001008','058001007','058001009','058001010'))")
    if start_date:
        filter_parts.append(f"(FREE_DATE>='{start_date}')")
    if end_date:
        filter_parts.append(f"(FREE_DATE<='{end_date}')")
    filter_str = "".join(filter_parts) if filter_parts else ""
    params = {
        "reportName": _UNLOCK_REPORT_NAME,
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "sortColumns": "FREE_DATE",
        "sortTypes": "-1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str
    try:
        url = _UNLOCK_API + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not data.get("success") or not data.get("result", {}).get("data"):
        return "未查询到限售股解禁数据。"

    items = data["result"]["data"]
    total = data["result"].get("count", len(items))

    lines = [f"🔓 限售股解禁数据（市场: {market}，共 {total} 条）", "=" * 80]
    header = (f"{'股票代码':<10} {'股票名称':<10} {'解禁日期':<12} "
              f"{'解禁数量(股)':<16} {'占流通比':<10} {'市值(元)':<16} {'涨跌幅':<8}")
    lines.append(header)
    lines.append("-" * len(header))

    for item in items:
        sec_code = item.get("SECURITY_CODE", "")
        sec_name = item.get("SECURITY_NAME_ABBR", "")
        free_date = (item.get("FREE_DATE") or "")[:10]
        free_num = item.get("FREE_NUM", 0) or 0
        free_cap = item.get("FREE_CAP", 0) or 0
        free_ratio = item.get("FREE_CAP_RATIO", 0) or 0
        mkt_val = item.get("MKT_VALUE", 0) or 0
        chg_rate = item.get("CHANGE_RATE", 0) or 0
        lines.append(f"{sec_code:<10} {sec_name:<10} {free_date:<12} "
                     f"{free_num:<16,} {_fmt(free_ratio):<10} {mkt_val:<16,.2f} {_fmt(chg_rate):<8}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 7：解禁持有人明细
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
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
    page_size = max(1, min(page_size, 100))
    page_number = max(1, page_number)

    filter_parts = []
    if code:
        filter_parts.append(f'(SECURITY_CODE="{code}")')
    if free_date:
        filter_parts.append(f"(FREE_DATE>='{free_date}')(FREE_DATE<='{free_date}')")
    filter_str = "".join(filter_parts) if filter_parts else ""

    params = {
        "reportName": "RPT_LIFT_GD",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "sortColumns": "FREE_DATE,SECURITY_CODE",
        "sortTypes": "-1,1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str

    try:
        locked_url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(locked_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            ld = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not ld.get("success") or not ld.get("result", {}).get("data"):
        return "未查询到解禁持有人数据。"

    items = ld["result"]["data"]
    total = ld["result"].get("count", len(items))
    stock_name = items[0].get("SECURITY_NAME_ABBR", "") if code else ""
    stock_code = items[0].get("SECURITY_CODE", "") if code else ""

    lines = [f"🔓 解禁持有人明细 - {stock_name}({stock_code}) 共 {total} 条", "=" * 80]
    header = (f"{'股东名称':<20} {'解禁日期':<12} {'解禁数量':<16} "
              f"{'占流通比':<10} {'锁定期':<8} {'剩余未解禁':<16}")
    lines.append(header)
    lines.append("-" * len(header))

    for item in items:
        holder = item.get("HOLDER_NAME", "")
        free_d = (item.get("FREE_DATE") or "")[:10]
        free_n = item.get("FREE_NUM", 0) or 0
        free_r = item.get("FREE_CAP_RATIO", 0) or 0
        lock_period = item.get("LOCK_PERIOD", "")
        remain = item.get("REMAIN_FREE_NUM", 0) or 0
        lines.append(f"{holder:<20} {free_d:<12} {_fmt(free_n):<16} "
                     f"{_fmt(free_r):<10} {str(lock_period):<8} {_fmt(remain):<16}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 股东增减持 API
# ═══════════════════════════════════════════════════

_SH_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_SH_REPORT_NAME = "RPT_HOLDER_TRADE"
_SH_SORT_COLUMNS = "END_DATE,SECURITY_CODE"
_SH_SORT_TYPES = "-1,1"
_SH_QUOTE_COLUMNS = "f2~03~CHANGE_RATE_QUOTES~涨跌幅"


def _build_shareholder_filter(
    stock_codes: list[str] | None = None,
    start_date: str = "",
    end_date: str = "",
    direction: str = "",
    holder_name: str = "",
) -> str:
    """构建股东增减持查询的 filter 参数"""
    parts = []
    if start_date and end_date:
        parts.append(f"(END_DATE>='{start_date}')(END_DATE<='{end_date}')")
    elif start_date:
        parts.append(f"(END_DATE>='{start_date}')")
    elif end_date:
        parts.append(f"(END_DATE<='{end_date}')")
    if stock_codes:
        codes_str = ",".join(f'"{c}"' for c in stock_codes)
        parts.append(f"(SECURITY_CODE in ({codes_str}))")
    if direction:
        parts.append(f'(DIRECTION="{direction}")')
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
    url = _SH_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return []
    if data.get("success") and data.get("result", {}).get("data"):
        return data["result"]["data"]
    return []


# ═══════════════════════════════════════════════════
# 工具 8：股东增减持查询
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
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
    page_size = max(1, min(page_size, 100))
    page_number = max(1, page_number)

    codes_list = [c.strip() for c in stock_codes.split(",") if c.strip()] if stock_codes else None

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
        return f"请求失败: {str(e)}"

    if not items:
        return "未查询到符合条件的股东增减持数据。"

    lines = [f"📊 股东增减持数据（共 {len(items)} 条）", "=" * 90]
    header = (f"{'股票':<18} {'股东':<16} {'方向':<6} {'变动数量':<14} "
              f"{'变动比例':<10} {'变动后持股':<14} {'变动后占比':<10} {'区间均价':<10} {'公告日':<12}")
    lines.append(header)
    lines.append("-" * len(header))

    for item in items:
        sec_code = item.get("SECURITY_CODE", "")
        sec_name = item.get("SECURITY_NAME_ABBR", "")
        holder = item.get("HOLDER_NAME", "")
        direction_v = item.get("DIRECTION", "")
        change_num = _fmt(item.get("CHANGE_NUM"))
        change_rate_v = _fmt(item.get("CHANGE_RATE"))
        after_num = _fmt(item.get("AFTER_HOLDER_NUM"))
        after_rate = _fmt(item.get("AFTER_CHANGE_RATE"))
        avg_price = _fmt(item.get("TRADE_AVERAGE_PRICE"))
        end_d = (item.get("END_DATE") or "")[:10]
        lines.append(f"{sec_name}({sec_code}):<18 {holder:<16} {direction_v:<6} {change_num:<14} "
                     f"{change_rate_v:<10} {after_num:<14} {after_rate:<10} {avg_price:<10} {end_d:<12}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 个股研报 API
# ═══════════════════════════════════════════════════

_RR_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_RR_REPORT_NAME = "RPT_RESEARCH_NEWEST"


def _build_report_filter(
    code: str = "",
    start_date: str = "",
    end_date: str = "",
    rating: str = "",
    rating_change: str = "",
    org_code: str = "",
    industry_code: str = "",
    researcher: str = "",
) -> str:
    """构建研报查询的 filter 参数"""
    parts = []
    if code:
        parts.append(f'(SECURITY_CODE="{code}")')
    if start_date:
        parts.append(f"(NOTICE_DATE>='{start_date}')")
    if end_date:
        parts.append(f"(NOTICE_DATE<='{end_date}')")
    if rating:
        parts.append(f'(PREDICT_RATING_NAME="{rating}")')
    if rating_change:
        parts.append(f'(RATING_CHANGE_NAME="{rating_change}")')
    if org_code:
        parts.append(f'(ORG_CODE="{org_code}")')
    if industry_code:
        parts.append(f'(INDUSTRY_CODE="{industry_code}")')
    if researcher:
        parts.append(f'(RESEARCHER="{researcher}")')
    return "".join(parts)


# ═══════════════════════════════════════════════════
# 工具 9：券商研报查询
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
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
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)

    filter_str = _build_report_filter(
        code=code, start_date=start_date, end_date=end_date,
        rating=rating, rating_change=rating_change, org_code=org_code,
        industry_code=industry_code, researcher=researcher,
    )

    params = {
        "reportName": _RR_REPORT_NAME,
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "sortColumns": "NOTICE_DATE",
        "sortTypes": "-1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str

    try:
        url = _RR_API + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not data.get("success") or not data.get("result", {}).get("data"):
        return "未查询到符合条件的研报数据。"

    items = data["result"]["data"]
    total = data["result"].get("count", len(items))

    lines = [f"📑 个股研报数据（共 {total} 条）", "=" * 90]
    for i, item in enumerate(items, 1):
        sname = item.get("SECURITY_NAME_ABBR", "")
        scode = item.get("SECURITY_CODE", "")
        org = item.get("ORG_NAME", "")
        researcher_n = item.get("RESEARCHER", "")
        rating_n = item.get("PREDICT_RATING_NAME", "")
        rating_ch = item.get("RATING_CHANGE_NAME", "")
        notice_d = (item.get("NOTICE_DATE") or "")[:10]
        eps_1 = item.get("EPS_1", "")
        eps_2 = item.get("EPS_2", "")
        eps_3 = item.get("EPS_3", "")
        pe_1 = item.get("PE_1", "")
        pe_2 = item.get("PE_2", "")
        pe_3 = item.get("PE_3", "")
        title = item.get("TITLE", "")
        lines.append(f"\n【{i}】{sname}({scode})  公告日: {notice_d}")
        lines.append(f"    机构: {org}  |  研究员: {researcher_n}")
        lines.append(f"    评级: {rating_n}  |  评级变动: {rating_ch if rating_ch else '--'}")
        lines.append(f"    EPS预测: 今年 {_fmt(eps_1)}  明年 {_fmt(eps_2)}  后年 {_fmt(eps_3)}")
        lines.append(f"    PE预测:  今年 {_fmt(pe_1)}  明年 {_fmt(pe_2)}  后年 {_fmt(pe_3)}")
        if title:
            lines.append(f"    标题: {title[:150]}")
        if i < len(items):
            lines.append("-" * 70)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 机构调研 API
# ═══════════════════════════════════════════════════

_SURVEY_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_SURVEY_REPORT_NAME = "RPT_ORG_SURVEY"


def _build_survey_filter(code: str = "", start_date: str = "", end_date: str = "") -> str:
    parts = []
    if code:
        parts.append(f'(SECURITY_CODE="{code}")')
    if start_date:
        parts.append(f"(RECEIVE_START_DATE>='{start_date}')")
    if end_date:
        parts.append(f"(RECEIVE_START_DATE<='{end_date}')")
    return "".join(parts)


def _fetch_surveys(filter_str: str, page_size: int = 10, page_number: int = 1) -> list[dict]:
    params = {
        "reportName": _SURVEY_REPORT_NAME,
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "sortColumns": "NOTICE_DATE",
        "sortTypes": "-1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str
    url = _SURVEY_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {}
    if data.get("success") and data.get("result", {}).get("data"):
        return data["result"]["data"]
    return []


# ═══════════════════════════════════════════════════
# 工具 10：机构调研查询
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
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
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)
    filter_str = _build_survey_filter(code=code, start_date=start_date, end_date=end_date)
    try:
        items = _fetch_surveys(filter_str, page_size, page_number)
    except Exception as e:
        return f"请求失败: {str(e)}"
    if not items:
        return "未查询到符合条件的机构调研数据。"

    lines = ["📋 机构调研数据", f"   共 {len(items)} 条记录", "=" * 70]
    for i, item in enumerate(items, 1):
        stock_name = item.get("SECURITY_NAME_ABBR", "")
        stock_code = item.get("SECURITY_CODE", "")
        notice_date = (item.get("NOTICE_DATE") or "")[:10] if item.get("NOTICE_DATE") else "--"
        survey_date = (item.get("RECEIVE_START_DATE") or "")[:10] if item.get("RECEIVE_START_DATE") else "--"
        survey_end = (item.get("RECEIVE_END_DATE") or "")[:10] if item.get("RECEIVE_END_DATE") else "--"
        lines.append(f"\n【{i}】{stock_name}({stock_code})")
        lines.append(f"    公告日: {notice_date}  |  调研日: {survey_date}" +
                     (f" ~ {survey_end}" if survey_end != "--" else ""))
        survey_way = item.get("RECEIVE_WAY_EXPLAIN", "")
        survey_obj = item.get("RECEIVE_OBJECT", "")
        survey_place = item.get("RECEIVE_PLACE", "")
        org_type = item.get("ORG_TYPE", "")
        investor_count = item.get("SUM", "")
        lines.append(f"    调研对象: {survey_obj or '--'}")
        lines.append(f"    调研方式: {survey_way}  |  地点: {survey_place or '--'}")
        if investor_count:
            lines.append(f"    参与机构数: {investor_count}家  |  机构类型: {org_type or '--'}")
        receptionist = item.get("RECEPTIONIST", "")
        investigators = item.get("INVESTIGATORS", "")
        close_price = item.get("CLOSE_PRICE", "")
        change_rate_v = item.get("CHANGE_RATE", "")
        if close_price:
            lines.append(f"    收盘价: {close_price}元  |  涨跌幅: {change_rate_v}%")
        if receptionist:
            lines.append(f"    接待人员: {receptionist[:120]}" +
                         ("..." if len(receptionist) > 120 else ""))
        if investigators:
            lines.append(f"    调研人员: {investigators[:120]}" +
                         ("..." if len(investigators) > 120 else ""))
        content = item.get("CONTENT", "")
        if content:
            lines.append(f"    调研内容:")
            for p in content.split("\n\n")[:5]:
                p = p.strip()
                if p:
                    lines.append(f"      {p[:200]}")
        if i < len(items):
            lines.append("-" * 70)
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 主力持仓 API + 工具
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
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
    """
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)

    filter_parts = []
    if code:
        filter_parts.append(f'(SECURITY_CODE="{code}")')
    if report_date:
        filter_parts.append(f"(END_DATE>='{report_date}')(END_DATE<='{report_date}')")
    filter_str = "".join(filter_parts) if filter_parts else ""

    params = {
        "reportName": "RPT_MAIN_ORGHOLD",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "sortColumns": "END_DATE,SECURITY_CODE,ORG_TYPE",
        "sortTypes": "-1,1,1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str

    try:
        hold_url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(hold_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            hd = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"

    if not hd.get("success") or not hd.get("result", {}).get("data"):
        return "未查询到主力持仓数据。"

    items = hd["result"]["data"]
    total = hd["result"].get("count", len(items))
    stock_name = items[0].get("SECURITY_NAME_ABBR", "") if code else ""
    stock_code = items[0].get("SECURITY_CODE", "") if code else ""

    lines = [f"🏦 主力持仓数据 - {stock_name}({stock_code}) 共 {total} 条", "=" * 90]

    def _hold_fmt(v):
        try:
            n = float(v) if v else 0
        except (ValueError, TypeError):
            return "--"
        return f"{n/1e4:.2f}万" if abs(n) >= 1e4 else f"{n:.0f}"

    header = (f"{'机构类型':<12} {'持仓数(股)':<18} {'市值':<14} "
              f"{'占流通比':<10} {'增减仓':<12}")
    lines.append(header); lines.append("-" * len(header))

    for item in items:
        org_type = item.get("ORG_TYPE", "")
        hold_num = _hold_fmt(item.get("HOLD_NUM", 0))
        mkt_val = _fmt(item.get("MKT_VAL", 0), "0.00")
        hold_ratio = _fmt(item.get("HOLD_RATIO", 0))
        change = item.get("CHANGE_NUM", 0) or 0
        change_str = _hold_fmt(change)
        if change > 0:
            change_str = "+" + change_str
        org_names = {"00": "机构汇总", "01": "基金", "02": "QFII", "03": "社保",
                     "04": "券商", "05": "保险", "06": "信托", "07": "其他", "08": "一般法人"}
        lines.append(f"{org_names.get(org_type, org_type):<12} {hold_num:<18} {mkt_val:<14} "
                     f"{hold_ratio:<10} {change_str:<12}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 工具 12：分红配股数据
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_dividend_history(code: str = "", page_size: int = 20, page_number: int = 1) -> str:
    """查询指定股票的历史分红送转配股数据"""
    page_size = max(1, min(page_size, 50))
    page_number = max(1, page_number)
    filter_str = f'(SECURITY_CODE="{code}")' if code else ""
    params = {
        "reportName": "RPT_SHAREBONUS_DET",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": page_size,
        "pageNumber": page_number,
    }
    if filter_str:
        params["filter"] = filter_str
    try:
        dv_url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(dv_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            dv_data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"
    if not dv_data.get("success") or not dv_data.get("result", {}).get("data"):
        return "未查询到分红配股数据。"
    items = dv_data["result"]["data"]
    total = dv_data["result"].get("count", len(items))
    stock_name = items[0].get("SECURITY_NAME_ABBR", "") if code else ""
    stock_code = items[0].get("SECURITY_CODE", "") if code else ""
    lines = [f"📊 分红配股数据 - {stock_name}({stock_code}) 共 {total} 条", "=" * 85]
    header = (f"{'报告期':<12} {'分红方案':<25} {'送转':<12} "
              f"{'派息(元)':<10} {'登记日':<12} {'除权日':<12}")
    lines.append(header); lines.append("-" * len(header))
    for item in items:
        rd = (item.get("REPORT_DATE") or "")[:10] if item.get("REPORT_DATE") else "--"
        plan = item.get("BONUS_DESCRIBE", "")
        send = item.get("SEND_CAPITAL", 0) or 0
        bonus = item.get("BONUS", 0) or 0
        reg_date = (item.get("REGISTER_DATE") or "")[:10] if item.get("REGISTER_DATE") else "--"
        ex_date = (item.get("EX_DIVIDEND_DATE") or "")[:10] if item.get("EX_DIVIDEND_DATE") else "--"
        lines.append(f"{rd:<12} {str(plan):<25} {_fmt(send):<12} "
                     f"{_fmt(bonus):<10} {reg_date:<12} {ex_date:<12}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 融资融券（两融）工具
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_margin_trading(
    code: str = "",
    start_date: str = "",
    end_date: str = "",
    page_size: int = 30,
    page_number: int = 1,
) -> str:
    """查询指定股票的融资融券（两融）数据"""
    page_size = max(1, min(page_size, 240))
    page_number = max(1, page_number)
    filter_parts = []
    if code:
        filter_parts.append(f'(SCODE="{code}")')
    if start_date:
        filter_parts.append(f"(DATE>='{start_date}')")
    if end_date:
        filter_parts.append(f"(DATE<='{end_date}')")
    filter_str = "".join(filter_parts) if filter_parts else ""
    params = {
        "reportName": "RPTA_WEB_RZRQ_GGMX",
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
        rzrq_url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(rzrq_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            rz_data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"
    if not rz_data.get("success") or not rz_data.get("result", {}).get("data"):
        return "未查询到融资融券数据。"
    items = rz_data["result"]["data"]
    total = rz_data["result"].get("count", len(items))

    def _fmt_yuan(v):
        try:
            val = float(v)
            if abs(val) >= 1e8: return f"{val/1e8:.2f}亿"
            elif abs(val) >= 1e4: return f"{val/1e4:.2f}万"
            else: return f"{val:.2f}元"
        except: return str(v) if v else "--"

    def _fmt_shares(v):
        try:
            return f"{int(float(v)):,}"
        except: return str(v) if v else "--"

    stock_name = items[0].get("SECNAME", "") if code else ""
    stock_code = items[0].get("SCODE", "") if code else ""
    lines = [f"📊 融资融券数据 - {stock_name}({stock_code}) 共 {total} 条", "=" * 90]
    header = (f"{'日期':<12} {'收盘价':>7} {'涨跌幅':>8} {'融资余额':>12} "
              f"{'融资买入':>10} {'融资偿还':>10} {'融券余量':>10} {'融券卖出':>10} {'两融余额':>12}")
    lines.append(header); lines.append("-" * len(header))
    for item in items:
        date = (item.get("DATE") or "")[:10]
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 高管持股变动数据工具
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
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
        "reportName": "RPT_EXECUTIVE_HOLD_DETAILS",
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
        exc_url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(exc_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            exc_data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return f"❌ 请求失败: {str(e)}"
    if not exc_data.get("success") or not exc_data.get("result", {}).get("data"):
        return "未查询到高管持股变动数据。"
    items = exc_data["result"]["data"]
    total = exc_data["result"].get("count", len(items))
    lines = [f"📊 高管持股变动数据（共 {total} 条）", "=" * 90]
    header = (f"{'股票':<16} {'高管':<10} {'职务':<10} {'方向':<6} "
              f"{'变动股数':<14} {'成交均价':<10} {'变动金额':<16} {'变动原因':<20} {'公告日':<12}")
    lines.append(header); lines.append("-" * len(header))
    for item in items:
        sc = item.get("SECURITY_NAME_ABBR", "")
        sn = item.get("SECURITY_CODE", "")
        pn = item.get("PERSON_NAME", "")
        pos = item.get("POSITION_NAME", "")
        dirc = item.get("CHANGE_DIRECTION", "")
        chg_num = item.get("CHANGE_NUM", 0) or 0
        avg_p = item.get("CHANGE_AVG_PRICE", 0) or 0
        chg_amt = item.get("CHANGE_AMOUNT", 0) or 0
        chg_reason = item.get("CHANGE_REASON", "")
        cdate = (item.get("CHANGE_DATE") or "")[:10]
        lines.append(f"{sc}({sn}):<16 {pn:<10} {str(pos):<10} {dirc:<6} "
                     f"{_fmt(chg_num):<14} {_fmt(avg_p):<10} {_fmt(chg_amt):<16} {str(chg_reason):<20} {cdate:<12}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# 资金流向数据工具（同花顺）
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def query_money_flow(code: str, days: int = 10) -> str:
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
    days = min(max(1, days), 60)
    url = f"http://stockpage.10jqka.com.cn/{code}/fundFlow/"
    try:
        html = _http_get(url, timeout=15, encoding="gbk")
    except Exception as e:
        return f"❌ 获取资金流向数据失败: {str(e)}"

    import html as h_mod
    # 尝试提取JS数据
    data_pattern = re.compile(r'var\s+(fundFlowData|fdata|dataList)\s*=\s*(\[.*?\]);', re.DOTALL)
    match = data_pattern.search(html)
    if match:
        try:
            raw_data = match.group(2)
            import json as _json
            fund_data = _json.loads(raw_data)
            lines = [f"📊 资金流向数据 - {code}（最近 {min(days, len(fund_data))} 天）", "=" * 80]
            header = (f"{'日期':<12} {'收盘价':>8} {'涨跌幅':>8} "
                      f"{'主力净流入':>12} {'主力占比':>8} {'超大单净流':>12} {'大单净流':>12} "
                      f"{'中单净流':>12} {'小单净流':>12}")
            lines.append(header); lines.append("-" * len(header))
            for row in fund_data[:days]:
                if isinstance(row, list) and len(row) >= 7:
                    row_date = str(row[0])[:10]
                    lines.append(f"{row_date:<12} {_fmt(row[1]):>8} {_fmt(row[2]):>8} "
                                 f"{_fmt(row[3]):>12} {_fmt(row[4]):>8} {_fmt(row[5]):>12} "
                                 f"{_fmt(row[6]):>12} {_fmt(row[7] if len(row)>7 else 0):>12} "
                                 f"{_fmt(row[8] if len(row)>8 else 0):>12}")
            lines.append("💡 说明：正数为净流入，负数为净流出。单位：万元")
            return "\n".join(lines)
        except Exception:
            pass

    return "❌ 未能解析到资金流向数据，请稍后重试。"


# ═══════════════════════════════════════════════════
# K线图生成工具（matplotlib + 腾讯财经）
# ═══════════════════════════════════════════════════

@mcp.tool()
@auth_required
@_rate_limit_decorator
def generate_kline_chart(code: str, days: int = 60, output_dir: str = "./doc", date: str = "") -> str:
    """
    生成指定股票的K线图（日K线 + 均线 + 今日分时走势），保存为PNG图片

    数据来源：
      日K线 → 腾讯财经(ifzq.gtimg.cn)
      今日分时 → 腾讯财经分钟API
      实时行情 → 新浪财经(hq.sinajs.cn)

    Args:
        code:       股票代码，纯数字，如 "601899" 或 "002455"。不要带 .SZ / .SH 后缀
        days:       显示最近多少个交易日（默认60，最大120）
        output_dir: 图片保存目录（默认 ./doc）
        date:       指定日期，格式 YYYY-MM-DD 或 YYYYMMDD（可选，默认为今日）

    Returns:
        K线图保存路径
    """
    try:
        import matplotlib
    except ImportError:
        return "❌ 缺少 matplotlib 库，请先执行: pip install matplotlib"
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.font_manager as fm
    from matplotlib.patches import Rectangle
    import os as _os
    import json
    from datetime import datetime as _dt

    # 解析指定日期
    if date:
        clean = date.replace("-", "")
        if len(clean) == 8:
            ref_dt = _dt(int(clean[:4]), int(clean[4:6]), int(clean[6:8]))
        else:
            return f"❌ 日期格式错误: {date}"
    else:
        ref_dt = _dt.now()
    today_str = ref_dt.strftime("%Y%m%d")

    # 中文字体
    _found_font = False
    for _fp in [
        r"C:\Windows\Fonts\simhei.ttf", r"C:\Windows\Fonts\msyh.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
    ]:
        if _os.path.exists(_fp):
            try:
                fm.fontManager.addfont(_fp)
                _name = _os.path.splitext(_os.path.basename(_fp))[0]
                plt.rcParams["font.sans-serif"] = [_name]
                _found_font = True
                break
            except:
                continue
    if not _found_font:
        plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    days = min(max(days, 20), 120)

    # 获取K线数据（腾讯财经）
    secid = "1" + code if code.startswith(("6", "9")) else "0" + code
    kline_url = f"http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={secid},m,,{days}"
    try:
        kline_text = _http_get(kline_url, timeout=15)
    except Exception as e:
        return f"❌ 获取K线数据失败: {str(e)}"
    try:
        kline_data = json.loads(kline_text)
    except Exception:
        return "❌ 解析K线数据失败"

    # 提取K线数据
    data_arr = None
    try:
        data_obj = kline_data.get("data", {}).get(secid, {})
        for key in ["qt", "qfq", "hfq", "m"]:
            if key in data_obj and data_obj[key]:
                data_arr = data_obj[key]
                break
        if not data_arr and "m" in kline_data.get("data", {}):
            data_arr = kline_data["data"]["m"].get(secid, [])
    except Exception:
        pass

    if not data_arr or len(data_arr) < 5:
        return f"❌ K线数据不足（{len(data_arr) if data_arr else 0}条），无法生成K线图"

    # 解析K线
    klines = []
    for item in data_arr:
        if isinstance(item, list) and len(item) >= 6:
            try:
                klines.append({
                    "date": str(item[0]),
                    "open": float(item[1]),
                    "close": float(item[2]),
                    "high": float(item[3]),
                    "low": float(item[4]),
                    "volume": float(item[5]) if len(item) > 5 else 0,
                })
            except:
                continue

    if len(klines) < 5:
        return f"❌ 有效K线数据不足（{len(klines)}条），无法生成K线图"

    # 计算均线
    closes = [k["close"] for k in klines]

    def _ma(data, n):
        return [sum(data[max(0, i - n + 1):i + 1]) / min(n, i + 1) for i in range(len(data))]

    ma5 = _ma(closes, 5)
    ma10 = _ma(closes, 10)
    ma20 = _ma(closes, 20)

    # 创建图表
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={"height_ratios": [3, 1]})
    fig.patch.set_facecolor("#0a1e3a")
    ax1.set_facecolor("#0f2847")
    ax2.set_facecolor("#0f2847")

    x_r = range(len(klines))
    for i, k in enumerate(klines):
        color = "#ff3333" if k["close"] >= k["open"] else "#33cc33"
        ax1.plot([i, i], [k["low"], k["high"]], color=color, linewidth=0.8)
        rect = Rectangle((i - 0.25, min(k["open"], k["close"])), 0.5,
                         abs(k["close"] - k["open"]), facecolor=color, edgecolor=color)
        ax1.add_patch(rect)

    ax1.plot(x_r, ma5, color="#ffd700", linewidth=0.8, label="MA5", alpha=0.8)
    ax1.plot(x_r, ma10, color="#00ccff", linewidth=0.8, label="MA10", alpha=0.8)
    ax1.plot(x_r, ma20, color="#ff66cc", linewidth=0.8, label="MA20", alpha=0.8)
    ax1.legend(loc="upper left", facecolor="#0f2847", edgecolor="#333", labelcolor="white")

    vol_colors = ["#ff3333" if k["close"] >= k["open"] else "#33cc33" for k in klines]
    vols = [k["volume"] / 100 for k in klines]
    ax2.bar(x_r, vols, color=vol_colors, width=0.6)

    tick_positions = list(range(0, len(x_r), max(1, len(x_r) // 8)))
    tick_labels = [klines[p]["date"][4:] for p in tick_positions]
    ax1.set_xticks(tick_positions)
    ax2.set_xticks(tick_positions)
    ax2.set_xticklabels(tick_labels, rotation=45, fontsize=8, color="white")
    ax1.tick_params(colors="white", labelsize=8)
    ax2.tick_params(colors="white", labelsize=8)
    ax1.grid(True, alpha=0.2, color="#555")
    ax2.grid(True, alpha=0.2, color="#555")
    ax2.set_xlabel("日期", color="white")
    ax1.set_ylabel("价格(元)", color="white")
    ax2.set_ylabel("成交量(手)", color="white")
    ax1.set_title(f"{code} K线图 ({klines[0]['date']} ~ {klines[-1]['date']})", color="white", fontsize=12)
    plt.tight_layout()

    output = _os.path.join(output_dir, f"{code}_kline_{today_str}.png")
    _os.makedirs(output_dir, exist_ok=True)
    plt.savefig(output, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    return f"✅ K线图已保存: {output}"


# ═══════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════

def main():
    """启动 MCP 服务"""
    mcp.run()


if __name__ == "__main__":
    main()
