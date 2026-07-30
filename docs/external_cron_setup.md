# 外部叫醒服務設定：cron-job.org

這份設定用來補強 GitHub Actions 內建排程偶爾漏觸發的問題。  
目標是每天台灣時間 19:00，由 cron-job.org 主動呼叫 GitHub API，叫醒 `Daily Taiwan Stock Screener` 工作流。

## 1. 建立 GitHub Token

到 GitHub 建立 Fine-grained personal access token，建議權限如下：

- Repository access：只選 `Jui-Cheng-C/tw-stock-screener`
- Permissions：
  - Contents：Read and write
  - Actions：Read and write

建立後請只在 cron-job.org 裡保存，不要寫進 `.env`、程式碼或 GitHub 檔案。

## 2. cron-job.org 設定

建立一個新的 Cronjob：

- Title：`Wake Taiwan Stock Screener`
- Schedule：每天 `19:00`
- Timezone：`Asia/Taipei`
- URL：

```text
https://api.github.com/repos/Jui-Cheng-C/tw-stock-screener/dispatches
```

- Method：`POST`
- Request body：

```json
{"event_type":"daily_screener","client_payload":{"source":"cron-job-org"}}
```

- Headers：

```text
Accept: application/vnd.github+json
Authorization: Bearer 你的_GITHUB_TOKEN
X-GitHub-Api-Version: 2022-11-28
User-Agent: cron-job-org
Content-Type: application/json
```

## 3. 成功判斷

cron-job.org 若顯示 HTTP `204`，代表 GitHub 已接受叫醒請求。  
接著到 GitHub repo 的 Actions 頁面，應該會看到一筆 `repository_dispatch` 觸發的 run。

工作流本身仍有台灣時間 18:00-22:59 的防呆閘門；如果外部服務延遲到半夜才叫醒，程式會跳過，避免寄出錯誤日期的報告。

## 4. 為什麼還保留 GitHub 原本排程

目前是雙保險：

- GitHub Actions schedule：內建排程，免費但偶爾漏觸發。
- cron-job.org：外部叫醒線，專門補 GitHub schedule 不穩的問題。

兩邊都觸發也沒關係，`tw_stock_screener.py --skip-if-sent` 會避免同一天重複寄正式報告。
