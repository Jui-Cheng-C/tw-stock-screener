$ErrorActionPreference = "Stop"

$Workspace = "C:\Users\user\Documents\Jui-001"
$Python = "C:\Users\user\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"

Set-Location $Workspace
& $Python "tw_stock_screener.py"
