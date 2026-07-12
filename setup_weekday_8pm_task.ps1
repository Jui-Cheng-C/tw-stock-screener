$ErrorActionPreference = "Stop"

$TaskName = "TWStockScreener"
$Workspace = "C:\Users\user\Documents\Jui-001"
$ScriptPath = Join-Path $Workspace "run_screener.ps1"
$PowerShell = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$Action = New-ScheduledTaskAction -Execute $PowerShell -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`"" -WorkingDirectory $Workspace
$Trigger = New-ScheduledTaskTrigger -Daily -At 8:00PM
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 3)

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Description "Run Taiwan stock screener every day at 20:00 and email the report or status notice." -Force | Out-Null
Write-Host "Scheduled task '$TaskName' is set for every day at 20:00."
