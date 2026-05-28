# IG/X Crawler Worker — 自动重启 (maxpage 由入队时指定，worker 不用传)
# 用法: 在任意目录运行 deploy\run_worker.ps1 即可
#       .\deploy\run_worker.ps1                        (默认: ig_crawler.py --mode all)
#       .\deploy\run_worker.ps1 -Mode full              (仅全量)
#       .\deploy\run_worker.ps1 -Mode incr              (仅增量)
#       .\deploy\run_worker.ps1 -Script x_crawler       (X 平台)
#       .\deploy\run_worker.ps1 -MaxTasks 0             不限任务数

param(
    [string]$Script = "ig_crawler",
    [string]$Mode = "all",
    [int]$MaxTasks = 20
)

# 切换到项目根目录 (deploy 的上级)
Set-Location (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
$ProjectRoot = (Get-Location).Path
Write-Host "Project root: $ProjectRoot"

$Python = "venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    Write-Host "ERROR: $Python not found, run install.sh first"
    exit 1
}

$Args = @("-u", "$Script.py", "--mode", $Mode)

$count = 0
while ($true) {
    $count++
    Write-Host "========================================"
    Write-Host "[$(Get-Date)] Worker #$count starting: $Python $Args (MAX_TASKS=$MaxTasks)"
    Write-Host "========================================"

    $env:MAX_TASKS_PER_WORKER = "$MaxTasks"

    $proc = Start-Process -FilePath $Python -ArgumentList $Args -NoNewWindow -Wait -PassThru

    Write-Host "[$(Get-Date)] Worker #$count exited (code: $($proc.ExitCode)), restarting in 3s..."
    Start-Sleep 3
}
