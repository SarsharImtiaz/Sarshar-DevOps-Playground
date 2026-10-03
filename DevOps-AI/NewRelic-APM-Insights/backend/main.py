import hashlib
import re
import time
from threading import Lock

from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from analysis import analyze_logs
from excel_report import build_excel
from newrelic_client import query_newrelic
from word_report import build_word

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


class RequestBody(BaseModel):
    apiKey: str
    accountId: str
    serviceName: str
    days: int = Field(default=7, ge=1, le=30)


XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

# Downloading the Excel and then the Word report re-uses one New Relic fetch
CACHE_TTL_SECONDS = 15 * 60
_cache = {}
_cache_lock = Lock()


def _cache_key(body):
    raw = f"{body.apiKey}|{body.accountId}|{body.serviceName}|{body.days}"
    return hashlib.sha256(raw.encode()).hexdigest()


def get_analysis(body):
    key = _cache_key(body)
    now = time.time()
    with _cache_lock:
        for k in [k for k, (t, _) in _cache.items() if now - t > CACHE_TTL_SECONDS]:
            del _cache[k]
        if key in _cache:
            return _cache[key][1]

    logs, stats = query_newrelic(body.apiKey, body.accountId, body.serviceName, body.days)
    if not logs:
        raise HTTPException(status_code=404, detail="No ERROR logs found for this service and period")

    analysis = analyze_logs(logs, stats, body.serviceName, body.accountId, body.days)
    with _cache_lock:
        _cache[key] = (now, analysis)
    return analysis


def _filename(a, ext):
    service = re.sub(r"[^A-Za-z0-9_-]+", "_", a.service).strip("_") or "service"
    return f"{service}_errors_{a.period_start:%Y%m%d}-{a.period_end:%Y%m%d}.{ext}"


def _download(body, builder, ext, media_type):
    try:
        analysis = get_analysis(body)
        content = builder(analysis).read()
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{_filename(analysis, ext)}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        print("ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/report/excel")
def excel_report(body: RequestBody):
    return _download(body, build_excel, "xlsx", XLSX)


@app.post("/api/report/word")
def word_report(body: RequestBody):
    return _download(body, build_word, "docx", DOCX)


# Kept for existing callers - same as /api/report/excel
@app.post("/api/fetch-logs")
def fetch_logs(body: RequestBody):
    return excel_report(body)


@app.get("/")
def home():
    return {"message": "Backend is running"}
