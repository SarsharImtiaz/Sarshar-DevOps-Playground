# New Relic APM Insights

A full-stack reporting tool that retrieves `ERROR` logs from New Relic, turns noisy log data into actionable insights, and exports the results as polished Excel and Word reports.

The application groups repeated errors into stable patterns, categorizes failures, highlights affected tenants and endpoints, and surfaces operational findings such as dominant error types, traffic peaks, and attribution gaps.

## Features

- Query New Relic logs by account, service, and time range
- Analyze the last 1, 3, 7, 14, or 30 days
- Export an Excel workbook with detailed data and charts
- Export a Word report with an executive-friendly summary
- Deduplicate repeated log records before analysis
- Group variable messages into normalized error patterns
- Categorize authentication, database, validation, HTTP, and application errors
- Break down errors by tenant, endpoint, day, and hour
- Split high-volume NRQL time windows automatically to reduce data loss at New Relic's row limit
- Reuse fetched data for 15 minutes when generating both report formats

## Tech Stack

| Layer | Technology |
| --- | --- |
| Frontend | Next.js 16, React 19, TypeScript, Tailwind CSS |
| Backend | FastAPI, Python 3 |
| Data analysis | pandas |
| Excel reports | openpyxl |
| Word reports | python-docx |
| Observability API | New Relic NerdGraph and NRQL |

## How It Works

1. The user enters a New Relic user API key, account ID, service name, and reporting period.
2. The FastAPI backend queries `Log` events through New Relic NerdGraph.
3. Large result windows are recursively divided when they reach the 5,000-row NRQL limit.
4. The analysis pipeline deduplicates logs, categorizes errors, normalizes recurring patterns, and resolves tenant attribution.
5. The requested Excel or Word report is generated in memory and downloaded by the browser.

## Prerequisites

- Python 3.10 or newer
- Node.js 20.9 or newer
- npm
- A New Relic account ID
- A New Relic user API key with permission to query logs for that account

## Local Setup

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd NewRelic-APM-Insights
```

### 2. Start the backend

From the repository root:

```bash
cd backend
python -m venv venv
```

Activate the virtual environment:

```powershell
# Windows PowerShell
.\venv\Scripts\Activate.ps1
```

```bash
# macOS/Linux
source venv/bin/activate
```

Install the Python dependencies and run the API:

```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

The backend will be available at [http://localhost:8000](http://localhost:8000). Interactive API documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs).

### 3. Start the frontend

In a second terminal:

```bash
cd frontend
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

## Usage

Enter the following values in the web interface:

- **API Key**: your New Relic user API key
- **Account ID**: the New Relic account containing the logs
- **Service Name**: the exact value stored in `entity.name`
- **Period**: a range from 1 to 30 days

Choose **Download Excel Workbook** or **Download Word Report**. The initial request can take some time for a high-volume service. A second report generated with the same inputs will reuse the cached analysis for up to 15 minutes.

## API

### Generate an Excel report

```http
POST /api/report/excel
Content-Type: application/json
```

### Generate a Word report

```http
POST /api/report/word
Content-Type: application/json
```

Both endpoints accept the same body:

```json
{
  "apiKey": "YOUR_NEW_RELIC_USER_API_KEY",
  "accountId": "YOUR_ACCOUNT_ID",
  "serviceName": "YOUR_SERVICE_NAME",
  "days": 7
}
```

`days` must be between `1` and `30`. The legacy `POST /api/fetch-logs` endpoint returns the same Excel report as `/api/report/excel`.

Example with cURL:

```bash
curl -X POST http://localhost:8000/api/report/excel \
  -H "Content-Type: application/json" \
  -d '{"apiKey":"YOUR_API_KEY","accountId":"YOUR_ACCOUNT_ID","serviceName":"YOUR_SERVICE","days":7}' \
  --output newrelic-errors.xlsx
```

## Report Contents

Generated reports include insights such as:

- Total errors, distinct patterns, affected tenants, and affected endpoints
- Category and error-pattern rankings
- Daily and hourly trends
- Peak periods and concentration of recurring failures
- Tenant attribution and top errors per tenant
- Most affected API endpoints
- Unresolved tenant identities and unattributed events
- Deduplication and query-completeness statistics
- Raw, deduplicated log details for further investigation

## Project Structure

```text
NewRelic-APM-Insights/
|-- backend/
|   |-- main.py               # FastAPI routes, caching, and downloads
|   |-- newrelic_client.py    # NerdGraph and NRQL queries
|   |-- analysis.py           # Classification, normalization, and aggregation
|   |-- excel_report.py       # Excel workbook generation
|   |-- word_report.py        # Word document generation
|   |-- tenants.py            # Tenant and domain mappings
|   `-- requirements.txt
|-- frontend/
|   |-- app/                  # Next.js application
|   `-- package.json
`-- README.md
```

## Configuration Notes

- The frontend currently expects the backend at `http://localhost:8000`. Update `API_URL` in `frontend/app/page.tsx` when deploying to another host.
- The backend allows all CORS origins for local development. Restrict `allow_origins` in `backend/main.py` before exposing the API publicly.
- NRQL queries filter for `level = 'ERROR'` and an exact `entity.name` match.
- Tenant display names and fallback domain mappings are maintained in `backend/tenants.py`.

## Security

- Never commit New Relic API keys or include them in screenshots, logs, or generated examples.
- Use a key with the minimum permissions needed to query the target account.
- The API key is sent to the local backend for each request and is included only in the in-memory cache key as a SHA-256 digest; the raw key is not persisted by the application.
- Treat generated reports as sensitive because they may contain tenant information, endpoints, and raw error messages.
- Before production deployment, use HTTPS, restrict CORS, add authentication, and store secrets in an approved secret manager.

## Limitations

- Only log events recorded at `ERROR` level are analyzed.
- Tenant attribution depends on the fields and message formats expected by the current analysis rules.
- If a one-minute query window still reaches New Relic's 5,000-row limit, the report flags possible truncation.
- The report cache is local to a single backend process and is cleared when that process restarts.

## Development Commands

Frontend:

```bash
npm run dev
npm run lint
npm run build
```

Backend:

```bash
uvicorn main:app --reload --port 8000
```

## License

No license has been added yet. Add a `LICENSE` file before distributing or accepting external contributions.
