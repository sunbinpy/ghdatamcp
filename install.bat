@echo off
chcp 65001 >nul
title 股海罗盘 GH-Data MCP Server 安装程序
setlocal enabledelayedexpansion

echo ==================================================
echo   股海罗盘 GH-Data MCP Server 安装脚本
echo   版本: 1.0.0
echo ==================================================
echo.

:: ----- 安装模式选择 -----
set INSTALL_MODE=local
if not "%~1"=="" set INSTALL_MODE=%~1

if /i "%INSTALL_MODE%"=="github" goto :install_github
if /i "%INSTALL_MODE%"=="gitee" goto :install_gitee
if /i "%INSTALL_MODE%"=="pypi" goto :install_pypi
if /i "%INSTALL_MODE%"=="local" goto :install_local

echo [错误] 未知安装模式: %INSTALL_MODE%
echo   支持: local  github  gitee  pypi
echo   示例: install.bat github
pause
exit /b 1

:: ====== 本地安装 ======
:install_local
echo [模式] 本地安装（从当前目录安装）
set PIP_SOURCE=.
goto :check_python

:: ====== GitHub 安装 ======
:install_github
echo [模式] GitHub 在线安装
echo   下载地址: https://github.com/sunbinpy/ghdatamcp
set PIP_SOURCE=git+https://github.com/sunbinpy/ghdatamcp.git@v1.0.0
goto :check_python

:: ====== Gitee 安装（国内加速） ======
:install_gitee
echo [模式] Gitee 国内镜像安装
echo   下载地址: https://gitee.com/sunbinpy/ghdatamcp
set PIP_SOURCE=git+https://gitee.com/sunbinpy/ghdatamcp.git@v1.0.0
goto :check_python

:: ====== PyPI 安装 ======
:install_pypi
echo [模式] PyPI 官方源安装
set PIP_SOURCE=ghdata-mcp
goto :check_python

:: ====== 检查 Python ======
:check_python
echo.
echo [1/4] 检查 Python 环境...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [失败] 未检测到 Python，请先安装 Python 3.10+
    echo   下载: https://www.python.org/downloads/
    pause
    exit /b 1
)
python -c "import sys; exit(0 if sys.version_info >= (3,10) else 1)"
if %errorlevel% neq 0 (
    echo [失败] 需要 Python 3.10+，当前版本过低
    pause
    exit /b 1
)
for /f "usebackq tokens=*" %%a in (`python --version 2^>nul`) do echo [通过] %%a
echo.

:: ====== 安装包 ======
echo [2/4] 安装 ghdata-mcp 及依赖...
if /i "%INSTALL_MODE%"=="pypi" (
    echo   正在从 PyPI 下载安装...
    pip install %PIP_SOURCE%
) else if /i "%INSTALL_MODE%"=="local" (
    echo   正在从本地目录安装...
    pip install -e %PIP_SOURCE%
) else (
    echo   正在从 Git 仓库安装...
    pip install %PIP_SOURCE%
)

if %errorlevel% neq 0 (
    echo [失败] 安装失败
    echo   可能原因：网络连接问题
    if /i "%INSTALL_MODE%"=="github" (
        echo   国内用户建议: install.bat gitee
    )
    if /i "%INSTALL_MODE%"=="pypi" (
        echo   国内用户建议: pip install ghdata-mcp -i https://pypi.tuna.tsinghua.edu.cn/simple
    )
    pause
    exit /b 1
)
echo [通过] ghdata-mcp 安装完成
echo.

:: ====== 安装 Playwright ======
echo [3/4] 安装 Playwright 浏览器驱动（用于K线图生成）...
python -m playwright install chromium 2>nul
if %errorlevel% neq 0 (
    echo [警告] Playwright 浏览器驱动安装失败
    echo   K线图生成功能可能不可用
    echo   可稍后手动运行: python -m playwright install chromium
) else (
    echo [通过] Playwright 浏览器驱动安装完成
)
echo.

:: ====== 验证安装 ======
echo [4/4] 验证安装...
python -c "import ghdata_mcp; print(f'  版本: {ghdata_mcp.__version__}'); tools=[x for x in dir(ghdata_mcp) if x.startswith(('query_','get_','generate_'))]; print(f'  工具数: {len(tools)}'); print(f'  工具列表: {tools}')"
if %errorlevel% neq 0 (
    echo [失败] 模块导入失败
    pause
    exit /b 1
)
echo [通过] 安装验证通过
echo.

:: ====== 注册说明 ======
echo ==================================================
echo   安装成功！
echo ==================================================
echo.
echo  ▸ 注册到 QwenPaw（控制台 → 智能体 → MCP → + 创建）：
echo.
echo    {
echo      "gh-data": {
echo        "command": "python",
echo        "args": ["-m", "ghdata_mcp"],
echo        "env": {}
echo      }
echo    }
echo.
echo  ▸ 验证是否正常运行：
echo    python -m ghdata_mcp
echo.
echo  ▸ 更多信息: README.md
echo ==================================================
pause
