$ErrorActionPreference = "Stop"

$RepoUrl = "https://github.com/Jui-Cheng-C/tw-stock-screener.git"
$Branch = "main"

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

Write-Host "Clearing Git cached index..."
git rm -r --cached .

Write-Host "Re-adding all local files..."
git add .

Write-Host "Committing force workflow path fix..."
git commit -m "Force fix workflow path"

Write-Host "Force pushing clean local project to GitHub..."
git push -u origin $Branch --force

Write-Host "Done. GitHub repository has been force-updated from local files."
