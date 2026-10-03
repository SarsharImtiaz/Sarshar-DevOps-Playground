"""Turns raw New Relic log rows into the tables used by the Excel and Word reports.

Key steps:
  1. De-duplicate rows that were ingested / fetched more than once.
  2. Classify each message into a category and collapse it into a normalized
     "pattern" (GUIDs, timestamps, e-mails, numbers replaced by placeholders), so
     12,000 "unique" messages become a few hundred real error types.
  3. Resolve the tenant for each row from the log context, or from the identity
     tenant embedded in the message.
"""
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from tenants import DOMAIN_MAP, TENANT_MAP, map_tenant_name

GUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

UNATTRIBUTED = "Unattributed"

# (category, regex matched against the message, description) - first match wins
CATEGORIES = [
    ("HTTP 5xx Response", r'^HTTP "\w+" "[^"]*" responded 5\d\d',
     "Request finished with a server error status (500-599). Logged by the request pipeline after the failure."),
    ("HTTP 4xx Response", r'^HTTP "\w+" "[^"]*" responded 4\d\d',
     "Request finished with a client error status (400-499) but was logged at ERROR level."),
    ("Request Context Log", r"^User:.*Tenant:",
     "'User: ... Tenant: ...' context line written alongside a failing request. Carries no error detail itself."),
    ("Authentication", r"^IDX\d+|Microsoft\.IdentityModel|SecurityToken",
     "Token validation failures, e.g. IDX10223 - the caller's access token had already expired."),
    ("Database", r"database|SqlException|DbCommand|DbUpdateException|EntityFramework",
     "Errors raised by SQL Server / Entity Framework: duplicate keys, timeouts, connection failures."),
    ("Unhandled Exception", r"unhandled exception",
     "Exceptions that escaped application code and were caught by the ASP.NET Core host."),
    ("Validation", r"not valid|invalid|validation",
     "Requests rejected because an input value was malformed."),
    ("Application Error", r".",
     "Other application errors that do not fit a more specific category."),
]
CATEGORY_DESCRIPTIONS = {c: d for c, _, d in CATEGORIES}
CATEGORY_ORDER = [c for c, _, _ in CATEGORIES]
_CATEGORY_RES = [(c, re.compile(p, re.IGNORECASE)) for c, p, _ in CATEGORIES]

_HTTP_RE = re.compile(r'^HTTP "(\w+)" "([^"]*)" responded (\d+)')
_CONTEXT_RE = re.compile(r"^User:\s*(\S*)\s+Tenant:\s*(" + GUID + r")?", re.IGNORECASE)
_INNER_RE = re.compile(r"---> ([\w.]+(?:Exception|Error))(?: \(0x[0-9A-Fa-f]+\))?: ([^\n]+)")
_EXC_TYPE_RE = re.compile(r"^[\s\"]*([\w.]+(?:Exception|Error))(?: \(0x[0-9A-Fa-f]+\))?: ([^\n]+)", re.MULTILINE)
_CONN_PREFIX_RE = re.compile(r"^Connection id \S+, Request id \S+: ")
_SQL_RE = re.compile(
    r"\b(EXEC)\s+([\w.\[\]]+)|\b(INSERT INTO|UPDATE|DELETE FROM|MERGE|FROM)\s+(?:\[?\w+\]?\.)?\[?(\w+)\]?",
    re.IGNORECASE)
_TENANT_IN_MSG_RE = re.compile(r"TenantId:\s*\"?(" + GUID + ")", re.IGNORECASE)

_NORMALIZERS = [
    (re.compile(GUID), "{id}"),
    (re.compile(r"\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}"), "{time}"),
    (re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?Z?"), "{time}"),
    (re.compile(r"[\w.+-]+@[\w-]+(\.[\w-]+)+"), "{email}"),
    (re.compile(r"\b0H[A-Z0-9]{11}(:[0-9A-F]{8})?\b"), "{conn}"),
    (re.compile(r"The duplicate key value is \([^)]*\)\.?"), ""),
    (re.compile(r"An exception occurred in the database while saving changes for context type '[^']*'\.?"),
     "Database save failed."),
    (re.compile(r"An exception occurred while iterating over the results of a query for context type '[^']*'\.?"),
     "Database query failed."),
    (re.compile(r"Text: .*?, Mode: \w+"), "Text: {text}, Mode: {mode}"),
    (re.compile(r"search condition '[^']*'"), "search condition '{text}'"),
    (re.compile(r"\b0x[0-9A-Fa-f]+\b"), "{hex}"),
    (re.compile(r"\b\d+(\.\d+)?\b"), "{n}"),
]


def normalize_text(text):
    text = _CONN_PREFIX_RE.sub("", text.replace('"', ""))
    for regex, repl in _NORMALIZERS:
        text = regex.sub(repl, text)
    return re.sub(r"\s+", " ", text).strip().rstrip("'")


def normalize_path(path):
    if not path or not isinstance(path, str):
        return ""
    path = re.sub(GUID, "{id}", path)
    path = re.sub(r"/\d+(?=/|$)", "/{n}", path)
    return path.split("?")[0]


def classify(message):
    for category, regex in _CATEGORY_RES:
        if regex.search(message):
            return category
    return "Application Error"


def pattern_for(message, category):
    """Stable, human-readable signature for a message."""
    first_line = message.split("\n", 1)[0]

    if category.startswith("HTTP"):
        m = _HTTP_RE.match(message)
        if m:
            return f"{m.group(1)} {normalize_path(m.group(2))} -> {m.group(3)}"

    if category == "Request Context Log":
        m = _CONTEXT_RE.match(message)
        user = "user" if m and m.group(1) else "no user"
        tenant = "tenant" if m and m.group(2) else "no tenant"
        return f"User/Tenant context line ({user}, {tenant})"

    if first_line.startswith("IDX10223"):
        return "IDX10223: Token expired - lifetime validation failed"

    if first_line.startswith("Failed executing DbCommand"):
        sql = _SQL_RE.search(message.split("\n", 1)[-1])
        if sql:
            verb = (sql.group(1) or sql.group(3)).upper()
            target = (sql.group(2) or sql.group(4)).replace("[", "").replace("]", "")
            verb = "SELECT ... FROM" if verb == "FROM" else verb
            return f"Failed executing DbCommand: {verb} {target}"
        return "Failed executing DbCommand"

    head = normalize_text(first_line)
    inner = _INNER_RE.search(message) or _EXC_TYPE_RE.search(message)
    if inner:
        exc_type = inner.group(1).rsplit(".", 1)[-1]
        detail = normalize_text(inner.group(2))
        if detail and detail not in head:
            head = f"{head} | {exc_type}: {detail}"

    return head[:300]


def _clean_message(value):
    return "" if value is None or (isinstance(value, float) and pd.isna(value)) else str(value)


@dataclass
class Analysis:
    service: str
    account_id: str
    days: int
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    stats: dict
    raw: pd.DataFrame
    patterns: pd.DataFrame
    categories: pd.DataFrame
    daily: pd.DataFrame
    hourly: pd.DataFrame
    heatmap: pd.DataFrame
    tenants: pd.DataFrame
    tenant_daily: pd.DataFrame
    tenant_top_patterns: pd.DataFrame
    endpoints: pd.DataFrame
    unresolved_identities: pd.DataFrame
    kpis: dict = field(default_factory=dict)
    findings: list = field(default_factory=list)


def resolve_tenants(df):
    """Adds TenantId / TenantName / TenantSource columns."""
    context_id = df["context.TenantId"].fillna("").astype(str).str.strip().str.upper()

    ctx = df["message"].str.extract(_CONTEXT_RE)
    user = ctx[0].fillna("")
    identity_id = ctx[1].fillna("").str.upper()
    msg_tenant = df["message"].str.extract(_TENANT_IN_MSG_RE)[0].fillna("").str.upper()

    # Learn identity tenant -> CoreRelate tenant from rows that carry both IDs
    both = pd.DataFrame({"identity": identity_id, "ctx": context_id})
    both = both[(both.identity != "") & (both.ctx != "")]
    learned = (
        both.groupby(["identity", "ctx"]).size().reset_index(name="n")
        .sort_values("n", ascending=False).drop_duplicates("identity")
        .set_index("identity")["ctx"].to_dict()
    )

    domains = user.str.split("@").str[-1].str.lower().where(user.str.contains("@"), "")

    # Learn e-mail domain -> CoreRelate tenant, for identity tenants seen without a context tenant
    resolved_ctx = context_id.where(context_id != "", identity_id.map(learned).fillna(""))
    dom = pd.DataFrame({"domain": domains, "ctx": resolved_ctx})
    dom = dom[(dom.domain != "") & (dom.ctx != "")]
    domain_map = (
        dom.groupby(["domain", "ctx"]).size().reset_index(name="n")
        .sort_values("n", ascending=False).drop_duplicates("domain")
        .set_index("domain")["ctx"].to_dict()
    )
    for domain, tid in DOMAIN_MAP.items():
        domain_map.setdefault(domain.lower(), tid.upper())
    for tid, name in TENANT_MAP.items():
        if "." in name and " " not in name:
            domain_map.setdefault(name.lower(), tid)

    tenant_ids, names, sources = [], [], []
    for cid, iid, mid, msg, dom in zip(context_id, identity_id, msg_tenant, df["message"], domains):
        if cid:
            tid, src = cid, "Log context"
        elif mid:
            tid, src = mid, "Message text"
        elif iid and iid in learned:
            tid, src = learned[iid], "Identity tenant (learned)"
        elif iid and dom in domain_map:
            tid, src = domain_map[dom], "User e-mail domain"
        elif iid:
            tenant_ids.append(iid)
            names.append(f"Unresolved identity tenant ({dom})" if dom else "Unresolved identity tenant")
            sources.append("Identity tenant (unresolved)")
            continue
        else:
            tenant_ids.append("")
            names.append(UNATTRIBUTED)
            sources.append("None")
            continue

        tenant_ids.append(tid)
        names.append(map_tenant_name(tid) or f"Unmapped tenant ({tid[:8]})")
        sources.append(src)

    df["TenantId"] = tenant_ids
    df["TenantName"] = names
    df["TenantSource"] = sources
    df["UserDomain"] = domains
    df["IdentityTenantId"] = identity_id
    return df


def _top_pattern(df, key):
    """Most frequent pattern per `key`, preferring real errors over Request Context Log lines."""
    counts = df.groupby([key, "PatternId", "Pattern", "Category"]).size().reset_index(name="n")
    counts["is_context"] = counts["Category"] == "Request Context Log"
    top = counts.sort_values(["is_context", "n"], ascending=[True, False]).drop_duplicates(key)
    top["TopPattern"] = top["PatternId"] + " - " + top["Pattern"]
    return top[[key, "TopPattern"]]


def _share(part, total):
    return part / total if total else 0.0


def analyze_logs(logs, stats, service, account_id, days):
    df = pd.DataFrame(logs)
    for col in ["timestamp", "message", "level", "entity.name", "context.RequestPath", "context.TenantId"]:
        if col not in df.columns:
            df[col] = None

    fetched = len(df)
    df["message"] = df["message"].map(_clean_message)
    df = df.drop_duplicates(subset=["timestamp", "message", "context.TenantId", "context.RequestPath", "entity.name"])
    duplicates = fetched - len(df)

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    df["Date"] = df["timestamp"].dt.normalize()
    df["Hour"] = df["timestamp"].dt.floor("h")

    df["Category"] = df["message"].map(classify)
    df["Pattern"] = [pattern_for(m, c) for m, c in zip(df["message"], df["Category"])]
    df["Endpoint"] = df["context.RequestPath"].map(normalize_path)
    http = df["message"].str.extract(_HTTP_RE)
    df["Method"] = http[0].fillna("")
    df["StatusCode"] = pd.to_numeric(http[2], errors="coerce")
    df = resolve_tenants(df)

    total = len(df)
    attributed = df["TenantName"] != UNATTRIBUTED

    # ---- Patterns --------------------------------------------------------
    grp = df.groupby(["Category", "Pattern"])
    patterns = grp.agg(
        Errors=("message", "size"),
        Tenants=("TenantId", lambda s: s[s != ""].nunique()),
        FirstSeen=("timestamp", "min"),
        LastSeen=("timestamp", "max"),
        Sample=("message", "first"),
    ).reset_index()
    top_tenant = (
        df[attributed].groupby(["Category", "Pattern", "TenantName"]).size()
        .reset_index(name="n").sort_values("n", ascending=False)
        .drop_duplicates(["Category", "Pattern"])
    )
    top_endpoint = (
        df[df.Endpoint != ""].groupby(["Category", "Pattern", "Endpoint"]).size()
        .reset_index(name="n").sort_values("n", ascending=False)
        .drop_duplicates(["Category", "Pattern"])
    )
    patterns = (
        patterns.merge(top_tenant[["Category", "Pattern", "TenantName"]], how="left", on=["Category", "Pattern"])
        .merge(top_endpoint[["Category", "Pattern", "Endpoint"]], how="left", on=["Category", "Pattern"])
        .rename(columns={"TenantName": "TopTenant", "Endpoint": "TopEndpoint"})
        .sort_values("Errors", ascending=False).reset_index(drop=True)
    )
    patterns["Share"] = patterns["Errors"] / total
    patterns.insert(0, "PatternId", [f"P{i + 1:03d}" for i in range(len(patterns))])
    patterns[["TopTenant", "TopEndpoint"]] = patterns[["TopTenant", "TopEndpoint"]].fillna("")
    pattern_ids = patterns.set_index(["Category", "Pattern"])["PatternId"]
    df["PatternId"] = pattern_ids.reindex(pd.MultiIndex.from_arrays([df.Category, df.Pattern])).values

    # ---- Categories ------------------------------------------------------
    categories = df.groupby("Category").agg(
        Errors=("message", "size"),
        Patterns=("Pattern", "nunique"),
        Tenants=("TenantId", lambda s: s[s != ""].nunique()),
    ).reset_index()
    categories["Share"] = categories["Errors"] / total
    categories["Description"] = categories["Category"].map(CATEGORY_DESCRIPTIONS)
    categories = categories.sort_values("Errors", ascending=False).reset_index(drop=True)
    cat_order = categories["Category"].tolist()

    # ---- Time ------------------------------------------------------------
    daily = df.pivot_table(index="Date", columns="Category", values="message", aggfunc="size", fill_value=0)
    daily = daily.reindex(columns=cat_order, fill_value=0)
    daily["Total"] = daily.sum(axis=1)
    daily = daily.sort_index().reset_index()

    hourly = df.groupby("Hour").size().reindex(
        pd.date_range(df["Hour"].min().normalize(), df["Hour"].max(), freq="h"), fill_value=0
    ).rename_axis("Hour").reset_index(name="Errors")

    heatmap = df.pivot_table(index="Date", columns=df["timestamp"].dt.hour, values="message",
                             aggfunc="size", fill_value=0)
    heatmap = heatmap.reindex(columns=range(24), fill_value=0).sort_index()
    heatmap.columns = [f"{h:02d}:00" for h in heatmap.columns]
    heatmap["Total"] = heatmap.sum(axis=1)
    heatmap = heatmap.reset_index()

    # ---- Tenants ---------------------------------------------------------
    tdf = df[attributed]
    tenants = tdf.groupby(["TenantName", "TenantId"]).agg(
        Errors=("message", "size"),
        Patterns=("Pattern", "nunique"),
        Source=("TenantSource", lambda s: s.value_counts().index[0]),
        FirstSeen=("timestamp", "min"),
        LastSeen=("timestamp", "max"),
    ).reset_index()
    tp = _top_pattern(tdf, "TenantId")
    te = (tdf[tdf.Endpoint != ""].groupby(["TenantId", "Endpoint"]).size().reset_index(name="n")
          .sort_values("n", ascending=False).drop_duplicates("TenantId"))
    tenants = (tenants.merge(tp, how="left", on="TenantId")
               .merge(te[["TenantId", "Endpoint"]], how="left", on="TenantId")
               .rename(columns={"Endpoint": "TopEndpoint"}).fillna({"TopPattern": "", "TopEndpoint": ""}))
    tenants["Share"] = tenants["Errors"] / total
    tenants = tenants.sort_values("Errors", ascending=False).reset_index(drop=True)
    tenants.insert(0, "Rank", range(1, len(tenants) + 1))

    tenant_daily = tdf.pivot_table(index="TenantName", columns="Date", values="message",
                                   aggfunc="size", fill_value=0)
    tenant_daily = tenant_daily.reindex(columns=sorted(df["Date"].unique()), fill_value=0)
    tenant_daily.columns = [pd.Timestamp(c).strftime("%a %d %b") for c in tenant_daily.columns]
    tenant_daily["Total"] = tenant_daily.sum(axis=1)
    tenant_daily = tenant_daily.sort_values("Total", ascending=False).reset_index()

    ttp = tdf.groupby(["TenantName", "PatternId", "Category", "Pattern"]).size().reset_index(name="Errors")
    ttp["TenantTotal"] = ttp.groupby("TenantName")["Errors"].transform("sum")
    ttp["ShareOfTenant"] = ttp["Errors"] / ttp["TenantTotal"]
    ttp = ttp.sort_values(["TenantTotal", "TenantName", "Errors"], ascending=[False, True, False])
    ttp["Rank"] = ttp.groupby("TenantName").cumcount() + 1
    tenant_top_patterns = ttp[ttp["Rank"] <= 5].drop(columns="TenantTotal").reset_index(drop=True)

    # ---- Endpoints -------------------------------------------------------
    edf = df[df.Endpoint != ""]
    endpoints = edf.groupby("Endpoint").agg(
        Errors=("message", "size"),
        Tenants=("TenantId", lambda s: s[s != ""].nunique()),
        Patterns=("Pattern", "nunique"),
    ).reset_index()
    endpoints = endpoints.merge(_top_pattern(edf, "Endpoint"), how="left", on="Endpoint")
    endpoints["Share"] = endpoints["Errors"] / total
    endpoints = endpoints.sort_values("Errors", ascending=False).reset_index(drop=True)
    endpoints.insert(0, "Rank", range(1, len(endpoints) + 1))

    # ---- Unresolved identity tenants -------------------------------------
    unresolved = df[df.TenantSource == "Identity tenant (unresolved)"]
    unresolved_identities = unresolved.groupby("IdentityTenantId").agg(
        Errors=("message", "size"),
        UserDomains=("UserDomain", lambda s: ", ".join(sorted(set(d for d in s if d))) or "(none)"),
        LastSeen=("timestamp", "max"),
    ).reset_index().sort_values("Errors", ascending=False).reset_index(drop=True)

    # ---- Raw logs (deduplicated, readable column order) -------------------
    raw = df[[
        "timestamp", "TenantName", "TenantId", "TenantSource", "Category", "PatternId",
        "Method", "StatusCode", "context.RequestPath", "entity.name", "level", "message",
    ]].rename(columns={
        "timestamp": "Timestamp (UTC)", "TenantName": "Tenant", "TenantId": "Tenant ID",
        "TenantSource": "Tenant Source", "PatternId": "Pattern ID", "StatusCode": "Status",
        "context.RequestPath": "Request Path", "entity.name": "Service", "level": "Level",
        "message": "Message",
    })

    period_start = datetime.fromtimestamp(stats["start_ms"] / 1000, tz=timezone.utc).replace(tzinfo=None)
    period_end = datetime.fromtimestamp(stats["end_ms"] / 1000, tz=timezone.utc).replace(tzinfo=None)

    stats = {
        **stats,
        "rows_fetched": fetched,
        "duplicates_removed": duplicates,
        "rows_analyzed": total,
        "tenant_from_context": int((df.TenantSource == "Log context").sum()),
        "tenant_from_message": int((df.TenantSource == "Message text").sum()),
        "tenant_from_identity": int((df.TenantSource == "Identity tenant (learned)").sum()),
        "tenant_from_domain": int((df.TenantSource == "User e-mail domain").sum()),
        "tenant_unresolved_identity": int(len(unresolved)),
        "tenant_unattributed": int((df.TenantName == UNATTRIBUTED).sum()),
        "unmapped_tenant_ids": sorted(df.loc[df.TenantName.str.startswith("Unmapped tenant"), "TenantId"].unique()),
        "identity_mappings_learned": int(df.loc[df.TenantSource == "Identity tenant (learned)", "IdentityTenantId"].nunique()),
    }

    peak_day = daily.loc[daily["Total"].idxmax()]
    peak_hour = hourly.loc[hourly["Errors"].idxmax()]
    kpis = {
        "Total errors": total,
        "Duplicates removed": duplicates,
        "Distinct error patterns": len(patterns),
        "Tenants affected": int(tenants.shape[0]),
        "Endpoints affected": int(endpoints.shape[0]),
        "Daily average": round(total / max(len(daily), 1)),
        "Peak day": f"{peak_day['Date']:%a %d %b} ({int(peak_day['Total']):,})",
        "Peak hour (UTC)": f"{peak_hour['Hour']:%d %b %H:00} ({int(peak_hour['Errors']):,})",
    }

    analysis = Analysis(
        service=service, account_id=str(account_id), days=days,
        generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        period_start=period_start, period_end=period_end, stats=stats, raw=raw,
        patterns=patterns, categories=categories, daily=daily, hourly=hourly, heatmap=heatmap,
        tenants=tenants, tenant_daily=tenant_daily, tenant_top_patterns=tenant_top_patterns,
        endpoints=endpoints, unresolved_identities=unresolved_identities, kpis=kpis,
    )
    analysis.findings = build_findings(analysis)
    return analysis


def build_findings(a):
    """Short, data-driven observations shown on the Overview sheet and in the Word report."""
    total = a.stats["rows_analyzed"]
    findings = []

    top_cat = a.categories.iloc[0]
    findings.append(("Dominant category",
                     f"{top_cat['Category']} accounts for {top_cat['Errors']:,} errors ({top_cat['Share']:.0%})."))

    top = a.patterns.iloc[0]
    findings.append(("Most frequent error",
                     f"{top['PatternId']} '{top['Pattern'][:120]}' occurred {top['Errors']:,} times ({top['Share']:.0%})."))

    top5_share = a.patterns.head(5)["Errors"].sum() / total if total else 0
    findings.append(("Concentration",
                     f"The top 5 of {len(a.patterns)} patterns make up {top5_share:.0%} of all errors."))

    ctx = a.categories[a.categories.Category == "Request Context Log"]
    if not ctx.empty:
        findings.append(("Noise",
                         f"{int(ctx.Errors.iloc[0]):,} rows ({ctx.Share.iloc[0]:.0%}) are User/Tenant context lines "
                         "logged at ERROR level. They carry no error detail; consider logging them at a lower level."))

    auth = a.categories[a.categories.Category == "Authentication"]
    if not auth.empty:
        findings.append(("Authentication",
                         f"{int(auth.Errors.iloc[0]):,} token validation failures, mostly expired access tokens "
                         "(IDX10223). This suggests clients are not refreshing tokens before calling the API."))

    db = a.patterns[a.patterns.Category == "Database"]
    if not db.empty:
        d = db.iloc[0]
        findings.append(("Database",
                         f"Top database error {d['PatternId']} ({d['Errors']:,}x): {d['Pattern'][:300]}"))

    if not a.tenants.empty:
        t = a.tenants.iloc[0]
        findings.append(("Most affected tenant",
                         f"{t['TenantName']} with {t['Errors']:,} errors ({t['Share']:.0%} of total)."))

    if not a.endpoints.empty:
        e = a.endpoints.iloc[0]
        findings.append(("Most affected endpoint", f"{e['Endpoint']} with {e['Errors']:,} errors."))

    findings.append(("Peak", f"Busiest day was {a.kpis['Peak day']}; busiest hour was {a.kpis['Peak hour (UTC)']}."))

    unattributed = a.stats["tenant_unattributed"]
    findings.append(("Attribution",
                     f"{unattributed:,} errors ({_share(unattributed, total):.0%}) could not be linked to a tenant "
                     "(no tenant in log context or message)."))

    if a.stats["duplicates_removed"]:
        findings.append(("Duplicates",
                         f"{a.stats['duplicates_removed']:,} duplicate log rows were removed before analysis."))

    if a.stats["windows_truncated"]:
        findings.append(("Data completeness",
                         f"{a.stats['windows_truncated']} query window(s) still hit the 5,000-row NRQL limit; "
                         "some logs may be missing."))
    return findings
