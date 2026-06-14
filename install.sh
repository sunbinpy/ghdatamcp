#!/bin/bash
# ==================================================
#   股海罗盘 GH-Data MCP Server 安装脚本
#   版本: 1.0.0
#   支持: Linux / macOS
#   用法: ./install.sh [local|github|gitee|pypi]
# ==================================================

set -e

echo "=================================================="
echo "  股海罗盘 GH-Data MCP Server 安装脚本"
echo "  版本: 1.0.0"
echo "=================================================="
echo ""

# ----- 安装模式 -----
INSTALL_MODE="${1:-local}"

case "$INSTALL_MODE" in
    local)
        echo "[模式] 本地安装（从当前目录安装）"
        PIP_SOURCE="."
        ;;
    github)
        echo "[模式] GitHub 在线安装"
        echo "  下载地址: https://github.com/sunbinpy/ghdatamcp"
        PIP_SOURCE="git+https://github.com/sunbinpy/ghdatamcp.git@v1.0.0"
        ;;
    gitee)
        echo "[模式] Gitee 国内镜像安装"
        echo "  下载地址: https://gitee.com/sunbinpy/ghdatamcp"
        PIP_SOURCE="git+https://gitee.com/sunbinpy/ghdatamcp.git@v1.0.0"
        ;;
    pypi)
        echo "[模式] PyPI 官方源安装"
        PIP_SOURCE="ghdata-mcp"
        ;;
    *)
        echo "[错误] 未知安装模式: $INSTALL_MODE"
        echo "  支持: local  github  gitee  pypi"
        echo "  示例: ./install.sh github"
        exit 1
        ;;
esac
echo ""

# ----- 检查 Python -----
echo "[1/4] 检查 Python 环境..."
if command -v python3 &> /dev/null; then
    PYTHON=python3
elif command -v python &> /dev/null; then
    PYTHON=python
else
    echo "[失败] 未检测到 Python，请先安装 Python 3.10+"
    exit 1
fi

PY_VER=$($PYTHON --version 2>&1)
echo "[通过] $PY_VER"

$PYTHON -c "import sys; exit(0 if sys.version_info >= (3,10) else 1)"
if [ $? -ne 0 ]; then
    echo "[失败] 需要 Python 3.10+，当前版本过低"
    exit 1
fi
echo ""

# ----- 安装包 -----
echo "[2/4] 安装 ghdata-mcp 及依赖..."
case "$INSTALL_MODE" in
    pypi)
        echo "  正在从 PyPI 下载安装..."
        $PYTHON -m pip install "$PIP_SOURCE"
        ;;
    local)
        echo "  正在从本地目录安装..."
        $PYTHON -m pip install -e "$PIP_SOURCE"
        ;;
    *)
        echo "  正在从 Git 仓库安装..."
        $PYTHON -m pip install "$PIP_SOURCE"
        ;;
esac

if [ $? -ne 0 ]; then
    echo "[失败] 安装失败"
    if [ "$INSTALL_MODE" = "github" ]; then
        echo "  国内用户建议: ./install.sh gitee"
    fi
    if [ "$INSTALL_MODE" = "pypi" ]; then
        echo "  国内用户建议: pip install ghdata-mcp -i https://pypi.tuna.tsinghua.edu.cn/simple"
    fi
    exit 1
fi
echo "[通过] ghdata-mcp 安装完成"
echo ""

# ----- 安装 Playwright -----
echo "[3/4] 安装 Playwright 浏览器驱动..."
if $PYTHON -m playwright install chromium 2>/dev/null; then
    echo "[通过] Playwright 浏览器驱动安装完成"
else
    echo "[警告] Playwright 浏览器驱动安装失败"
    echo "   K线图生成功能可能不可用"
    echo "   可稍后手动运行: python -m playwright install chromium"
fi
echo ""

# ----- 验证安装 -----
echo "[4/4] 验证安装..."
$PYTHON -c "
import ghdata_mcp
print(f'  版本: {ghdata_mcp.__version__}')
tools = [x for x in dir(ghdata_mcp) if x.startswith(('query_','get_','generate_'))]
print(f'  工具数: {len(tools)}')
"
echo "[通过] 安装验证通过"
echo ""

# ----- 注册说明 -----
echo "=================================================="
echo "  安装成功！"
echo "=================================================="
echo ""
echo "  ▸ 注册到 QwenPaw（控制台 -> 智能体 -> MCP -> + 创建）："
echo ""
echo '  {'
echo '    "gh-data": {'
echo '      "command": "'$PYTHON'",'
echo '      "args": ["-m", "ghdata_mcp"],'
echo '      "env": {}'
echo '    }'
echo '  }'
echo ""
echo "  ▸ 验证是否正常运行："
echo "    $PYTHON -m ghdata_mcp"
echo ""
echo "  ▸ 更多信息: README.md"
echo "=================================================="
