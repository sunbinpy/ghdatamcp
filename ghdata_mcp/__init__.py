# -*- coding: utf-8 -*-
"""
股海罗盘 GH-Data MCP Server
A股全维度数据引擎 — 16个工具覆盖财务/行情/解禁/增减持/研报/调研/持仓/分红/资金流向/两融/高管变动/K线
"""

__version__ = "1.0.0"
__author__ = "gh-data"
__description__ = "A股个股深度分析 MCP 数据引擎"

from .server import mcp

# 导出工具函数
from .server import (
    query_financial_report,
    query_balance_sheet,
    query_cashflow_statement,
    query_income_statement,
    query_realtime_price,
    get_stock_unlock_data,
    get_stock_unlock_holders,
    query_shareholder_trade,
    query_research_report,
    query_institutional_survey,
    query_main_holdings,
    query_dividend_history,
    query_money_flow,
    query_margin_trading,
    query_executive_hold_change,
    generate_kline_chart,
)
