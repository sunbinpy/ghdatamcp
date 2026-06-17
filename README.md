# 股海罗盘 GH-Data MCP Server

> **A 股全维度数据引擎 — 16 个工具覆盖财务/行情/解禁/增减持/研报/调研/持仓/分红/资金流向/两融/高管变动/K线**

**ghdata-mcp** 是一个基于 FastMCP 协议的 A 股数据服务 Python 包，为 QwenPaw 等 MCP 客户端提供一站式股票数据分析能力。

---

## 🔑 API Key 验证（v3.0 — 服务端管理次数）

本 MCP Server 在每次工具调用前需经由**服务端验证 API Key**，按工具/日进行次数管理。

### 首次使用
- 无 Key 时自动生成 GUID 格式 Key，保存到 `~/.ghdata/ghdataapikey`
- 免费版每工具每日 **3 次**，付费版 **100 次**
- 次数用完提示购买：https://www.oraskl.com/ghdata-admin

### 设置 Key 的方式（优先级从高到低）
```bash
# 方式 1：环境变量（推荐）
export GHDATA_API_KEY=你的Key

# 方式 2：CWD/.apikey 文件
echo -n "你的Key" > .apikey

# 方式 3：~/.ghdata/ghdataapikey（Skill 自动生成时写入）
echo -n "你的Key" > ~/.ghdata/ghdataapikey
```

### 购买付费版
👉 https://www.oraskl.com/ghdata-admin

---

## 🚀 快速安装

### 方式一：从 PyPI 安装（推荐）

```bash
pip install ghdata-mcp
playwright install chromium
```

### 方式二：从 GitHub 安装

```bash
pip install git+https://github.com/sunbinpy/ghdatamcp.git
playwright install chromium
```

### 方式三：本地安装

```bash
cd setup
pip install .
playwright install chromium
```

### 方式四：直接运行（无需安装）

```bash
pip install mcp[cli] matplotlib numpy playwright
playwright install chromium
python server.py
```

---

## 🔌 注册到 QwenPaw

### 方式 A：通过 pip 安装后注册（推荐）

```json
{
  "gh-data": {
    "command": "python",
    "args": ["-m", "ghdata_mcp"],
    "env": {}
  }
}
```

### 方式 B：直接运行文件注册

```json
{
  "gh-data": {
    "command": "python",
    "args": ["路径/server.py"],
    "env": {}
  }
}
```

---

## 🛠️ 工具清单

| # | 工具名 | 功能 | 数据源 |
|---|--------|------|--------|
| 1 | `query_financial_report` | 业绩报表明细（营收/净利润/EPS/ROE/毛利率） | 东方财富 |
| 2 | `query_balance_sheet` | 资产负债表（总资产/负债/权益/货币资金） | 东方财富 |
| 3 | `query_cashflow_statement` | 现金流量表（经营/投资/融资现金流） | 东方财富 |
| 4 | `query_income_statement` | 利润表（营收/成本/费用明细/净利润） | 东方财富 |
| 5 | `query_realtime_price` | 实时行情（双源交叉验证：新浪+腾讯） | 新浪财经+腾讯财经 |
| 6 | `get_stock_unlock_data` | 限售股解禁数据（按市场/日期查询） | 东方财富 |
| 7 | `get_stock_unlock_holders` | 解禁持有人明细（股东名称/解禁数量） | 东方财富 |
| 8 | `query_shareholder_trade` | 股东增减持数据 | 东方财富 |
| 9 | `query_research_report` | 个股研报（评级/EPS/PE预测） | 东方财富 |
| 10 | `query_institutional_survey` | 机构调研记录 | 东方财富 |
| 11 | `query_main_holdings` | 机构主力持仓 | 东方财富 |
| 12 | `query_dividend_history` | 分红配股（方案/登记日/股息率） | 东方财富 |
| 13 | `query_money_flow` | 资金流向（大单/中单/小单净流入） | 同花顺 |
| 14 | `query_margin_trading` | 融资融券（融资余额/融券余量） | 东方财富 |
| 15 | `query_executive_hold_change` | 高管持股变动 | 东方财富 |
| 16 | `generate_kline_chart` | K线图生成（PNG输出，含均线+分时） | 腾讯+新浪 |

---

## ⚙️ 依赖

| 包 | 用途 |
|---|------|
| `mcp[cli]>=1.0.0` | FastMCP 框架 |
| `matplotlib>=3.8.0` | K线图绘制 |
| `numpy>=1.24.0` | 技术指标计算 |
| `playwright>=1.40.0` | 分时数据采集 |
| `playwright install chromium` | 浏览器驱动（需单独安装） |

---

## ⚠️ 已知限制

1. **资金流向收盘价可能延迟** — 涨跌停日需用新浪/腾讯验证
2. **不支持北交所** — 工具 13、16 不支持北交所股票
3. **腾讯分钟数据是累计值** — 需差分计算

---

## 📄 免责声明

> 数据来源：东方财富、同花顺、新浪财经、腾讯财经等公开市场数据。
>
> **仅供学习研究，不构成投资建议。** 数据可能存在延迟，使用者应自行核实。
