$ErrorActionPreference = "Stop"

$RepoUrl = "https://github.com/Jui-Cheng-C/tw-stock-screener.git"
$Branch = "main"
$CommitMessage = "Optimize short-term screener strategy"
$FilesToPublish = @(
    ".gitignore",
    ".env.example",
    "requirements.txt",
    "tw_stock_screener.py",
    "run_screener.ps1",
    "setup_weekday_8pm_task.ps1",
    "push_to_github.ps1",
    ".github/workflows/run_screener.yml"
)

Write-Host "Checking Git..."
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw "Git is not installed or not in PATH. Please install Git for Windows first."
}

Write-Host "Initializing Git repository if needed..."
if (-not (Test-Path ".git")) {
    git init
}

Write-Host "Setting branch to main..."
git branch -M $Branch

Write-Host "Setting GitHub remote..."
$remote = git remote get-url origin 2>$null
if ($LASTEXITCODE -ne 0) {
    git remote add origin $RepoUrl
} elseif ($remote -ne $RepoUrl) {
    git remote set-url origin $RepoUrl
}

Write-Host "Checking .gitignore protects .env..."
$gitignore = Get-Content ".gitignore" -ErrorAction Stop
if ($gitignore -notcontains ".env") {
    throw ".gitignore must contain '.env' before pushing. Stop to protect secrets."
}

Write-Host "Clearing Git cached index..."
git rm -r --cached . 2>$null

Write-Host "Adding only project files needed by GitHub Actions..."
foreach ($file in $FilesToPublish) {
    if (Test-Path $file) {
        git add $file
    } else {
        throw "Required file missing: $file"
    }
}

Write-Host "Committing latest screener version if there are staged changes..."
git diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "No staged changes to commit."
} else {
    git commit -m $CommitMessage
}

Write-Host "Force pushing clean local project to GitHub..."
git push -u origin $Branch --force

Write-Host "Done. GitHub repository has been force-updated from local files."
