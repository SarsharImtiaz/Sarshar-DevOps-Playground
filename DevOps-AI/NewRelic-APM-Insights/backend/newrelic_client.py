"""Fetches ERROR logs from New Relic via NerdGraph (NRQL)."""
import time

import requests

NEW_RELIC_URL = "https://api.newrelic.com/graphql"

# NRQL caps a single query at 5000 rows. A window that returns this many rows is
# split in half and re-queried so no logs are silently dropped.
NRQL_ROW_LIMIT = 5000
MIN_WINDOW_MS = 60 * 1000  # stop splitting below 1 minute

GRAPHQL_QUERY = """
query($accountId: Int!, $nrql: Nrql!) {
  actor {
    account(id: $accountId) {
      nrql(query: $nrql, timeout: 90) {
        results
      }
    }
  }
}
"""

FIELDS = "timestamp, message, level, entity.name, context.RequestPath, context.TenantId"


def _run_nrql(api_key, account_id, nrql):
    response = requests.post(
        NEW_RELIC_URL,
        json={"query": GRAPHQL_QUERY, "variables": {"accountId": int(account_id), "nrql": nrql}},
        headers={"Content-Type": "application/json", "API-Key": api_key},
        timeout=120,
    )
    response.raise_for_status()
    data = response.json()

    if data.get("errors"):
        raise Exception(data["errors"])

    return data["data"]["actor"]["account"]["nrql"]["results"]


def query_newrelic(api_key, account_id, service_name, days=7):
    """Returns (logs, stats). stats records how many windows were queried and whether any were still truncated."""
    service = service_name.replace("\\", "\\\\").replace("'", "\\'")
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 60 * 60 * 1000

    stats = {"windows_queried": 0, "windows_split": 0, "windows_truncated": 0,
             "start_ms": start_ms, "end_ms": end_ms}
    all_logs = []

    # Start with one window per day, splitting any that hit the row limit
    day_ms = 24 * 60 * 60 * 1000
    pending = [(s, min(s + day_ms, end_ms)) for s in range(start_ms, end_ms, day_ms)]

    while pending:
        since, until = pending.pop()
        nrql = (
            f"SELECT {FIELDS} FROM Log "
            f"WHERE entity.name = '{service}' AND level = 'ERROR' "
            f"SINCE {since} UNTIL {until} LIMIT {NRQL_ROW_LIMIT}"
        )
        results = _run_nrql(api_key, account_id, nrql)
        stats["windows_queried"] += 1

        if len(results) >= NRQL_ROW_LIMIT:
            if until - since > MIN_WINDOW_MS:
                mid = since + (until - since) // 2
                pending.extend([(since, mid), (mid, until)])
                stats["windows_split"] += 1
                continue
            stats["windows_truncated"] += 1

        all_logs.extend(results)

    return all_logs, stats
