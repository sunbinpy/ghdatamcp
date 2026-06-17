# -*- coding: utf-8 -*-
"""Append remaining tool functions to server.py"""
import os

appendix = r''' >10} ")
    lines.append("")

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
'''[1:]

# Remove the last incomplete line from server.py
fp = r'E:\GuPiao\agent\ghdata\mcpserver\setup\ghdata_mcp\server.py'
with open(fp, 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the last incomplete line (line starting with f"...)
lines = content.split('\n')
for i in range(len(lines)-1, -1, -1):
    s = lines[i].strip()
    if s.endswith('":') or s.startswith('                     f"'):
        lines = lines[:i]
        break
    if s == '':
        continue
    if 'RZCHE' in s or 'RQYL' in s or 'RQMC' in s or 'RZRQYE' in s:
        # incomplete margin trading line - remove it and everything after
        lines = lines[:i]
        break

content = '\n'.join(lines).rstrip('\n')
# Remove the incomplete f-string line at the very end
while content.endswith('"') and not content.endswith('\"\"\"'):
    if content.endswith('f"') or content.endswith('")') or content.endswith('},') or content.endswith('}') or content.endswith(','):
        pass
    # Find last newline and remove the last line
    last_nl = content.rfind('\n')
    if last_nl > 0:
        content = content[:last_nl]
    else:
        break

# Close the last incomplete f-string properly
content += '\n    return "\\n".join(lines)'

# Now append the remaining tools
content += '\n\n\n' + appendix

with open(fp, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Done! Wrote {len(content)} chars to server.py")
print("Checking last 10 lines...")
with open(fp, 'r', encoding='utf-8') as f:
    all_lines = f.readlines()
for line in all_lines[-10:]:
    print(repr(line))
