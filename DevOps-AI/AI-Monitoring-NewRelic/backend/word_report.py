"""Builds the Word (.docx) report: every section is a heading followed by a table."""
from io import BytesIO

from docx import Document
from docx.enum.section import WD_ORIENT
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

from analysis import CATEGORY_DESCRIPTIONS, UNATTRIBUTED
from excel_report import CATEGORY_COLORS, SHEET_GUIDE

NAVY = RGBColor(0x1F, 0x38, 0x64)
MUTED = RGBColor(0x52, 0x51, 0x4E)
HEADER_HEX = "1F3864"
BAND_HEX = "F3F6FA"
RULE_HEX = "D9DEE7"

GLOSSARY = [
    ("Error", "One log row at level ERROR from the selected New Relic service, after duplicates are removed."),
    ("Pattern", "A group of errors with the same message once variable parts (IDs, timestamps, e-mails, "
                "numbers) are replaced by placeholders such as {id} and {n}."),
    ("Pattern ID", "Short reference (P001, P002 ...) ranked by frequency. Use it to filter the Raw Logs sheet."),
    ("Category", "Broad type of error assigned from the message text, e.g. HTTP 5xx Response or Database."),
    ("Tenant", "The CoreRelate customer the request belonged to."),
    ("Tenant ID", "CoreRelate tenant GUID, taken from context.TenantId in the log."),
    ("Identity tenant", "The identity-provider (Entra) tenant GUID printed in 'User: ... Tenant: ...' lines. "
                        "It is different from the CoreRelate Tenant ID and is translated using rows that carry both."),
    ("Unattributed", "Errors with no tenant information in either the log context or the message."),
    ("Endpoint", "API route (context.RequestPath) with IDs replaced by {id}, so all calls to a route group together."),
    ("Duplicate", "A row identical to another in timestamp, message, tenant, path and service - the same event "
                  "ingested or fetched twice."),
    ("Query window", "One NRQL request. New Relic returns at most 5,000 rows per request, so busy periods are "
                     "split into smaller windows."),
    ("UTC", "All times in this report are Coordinated Universal Time."),
]


# ---- Low-level helpers ---------------------------------------------------
def _shade(cell, hex_color):
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tc_pr.append(shd)


def _cell_margins(table, top=40, bottom=40, left=80, right=80):
    tbl_pr = table._tbl.tblPr
    mar = OxmlElement("w:tblCellMar")
    for side, val in (("top", top), ("bottom", bottom), ("left", left), ("right", right)):
        el = OxmlElement(f"w:{side}")
        el.set(qn("w:w"), str(val))
        el.set(qn("w:type"), "dxa")
        mar.append(el)
    tbl_pr.append(mar)


def _borders(table):
    tbl_pr = table._tbl.tblPr
    borders = OxmlElement("w:tblBorders")
    for side in ("top", "bottom", "insideH"):
        el = OxmlElement(f"w:{side}")
        el.set(qn("w:val"), "single")
        el.set(qn("w:sz"), "4")
        el.set(qn("w:color"), RULE_HEX)
        borders.append(el)
    for side in ("left", "right", "insideV"):
        el = OxmlElement(f"w:{side}")
        el.set(qn("w:val"), "nil")
        borders.append(el)
    tbl_pr.append(borders)


def _repeat_header(row):
    tr_pr = row._tr.get_or_add_trPr()
    el = OxmlElement("w:tblHeader")
    el.set(qn("w:val"), "true")
    tr_pr.append(el)


def _no_split(row):
    tr_pr = row._tr.get_or_add_trPr()
    tr_pr.append(OxmlElement("w:cantSplit"))


def _write(cell, text, bold=False, color=None, size=9, align=None, italic=False):
    cell.text = ""
    p = cell.paragraphs[0]
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.space_before = Pt(0)
    if align:
        p.alignment = align
    run = p.add_run("" if text is None else str(text))
    run.font.size = Pt(size)
    run.bold = bold
    run.italic = italic
    if color is not None:
        run.font.color.rgb = color
    return run


def _field(paragraph, instr):
    """Inserts a Word field such as PAGE or NUMPAGES."""
    run = paragraph.add_run()
    begin = OxmlElement("w:fldChar")
    begin.set(qn("w:fldCharType"), "begin")
    text = OxmlElement("w:instrText")
    text.set(qn("xml:space"), "preserve")
    text.text = instr
    end = OxmlElement("w:fldChar")
    end.set(qn("w:fldCharType"), "end")
    run._r.append(begin)
    run._r.append(text)
    run._r.append(end)
    run.font.size = Pt(8)
    run.font.color.rgb = MUTED


def _fmt(v, kind):
    if v is None:
        return ""
    if kind == "int":
        return f"{int(v):,}"
    if kind == "pct":
        return f"{float(v):.1%}"
    if kind == "dt":
        return f"{v:%Y-%m-%d %H:%M}"
    if kind == "date":
        return f"{v:%a %d %b %Y}"
    return str(v)


def table(doc, headers, rows, widths, kinds=None, swatch_col=None):
    """Styled table: navy header row repeated on each page, banded rows, right-aligned numbers.

    swatch_col - index of a column whose values are category names; its cell is shaded in the category colour.
    """
    kinds = kinds or {}
    t = doc.add_table(rows=1, cols=len(headers))
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    t.autofit = False
    _borders(t)
    _cell_margins(t)

    hdr = t.rows[0]
    _repeat_header(hdr)
    for i, h in enumerate(headers):
        c = hdr.cells[i]
        _shade(c, HEADER_HEX)
        numeric = kinds.get(i) in ("int", "pct")
        _write(c, h, bold=True, color=RGBColor(0xFF, 0xFF, 0xFF), size=9,
               align=WD_ALIGN_PARAGRAPH.RIGHT if numeric else None)

    for r, row in enumerate(rows):
        cells = t.add_row().cells
        _no_split(t.rows[-1])
        for i, v in enumerate(row):
            kind = kinds.get(i)
            numeric = kind in ("int", "pct")
            if kind == "swatch":
                _shade(cells[i], CATEGORY_COLORS.get(v, "2A78D6"))
                _write(cells[i], "")
                continue
            _write(cells[i], _fmt(v, kind), bold=(kind == "bold"),
                   align=WD_ALIGN_PARAGRAPH.RIGHT if numeric else None)
            if r % 2:
                _shade(cells[i], BAND_HEX)

    for row in t.rows:
        for i, w in enumerate(widths):
            row.cells[i].width = Inches(w)
    doc.add_paragraph().paragraph_format.space_after = Pt(2)
    return t


def heading(doc, text, level=1):
    h = doc.add_heading(text, level=level)
    h.paragraph_format.keep_with_next = True
    return h


def note(doc, text):
    """One-line caption under a heading (kept short - the tables carry the content)."""
    p = doc.add_paragraph()
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.keep_with_next = True
    run = p.add_run(text)
    run.italic = True
    run.font.size = Pt(8.5)
    run.font.color.rgb = MUTED


def _setup(doc, a):
    section = doc.sections[0]
    section.orientation = WD_ORIENT.PORTRAIT
    section.page_width, section.page_height = Inches(8.5), Inches(11)
    for side in ("left_margin", "right_margin"):
        setattr(section, side, Inches(0.7))
    section.top_margin = section.bottom_margin = Inches(0.7)

    styles = doc.styles
    styles["Normal"].font.name = "Calibri"
    styles["Normal"].font.size = Pt(10)
    for name, size in (("Title", 24), ("Heading 1", 15), ("Heading 2", 12)):
        st = styles[name]
        st.font.name = "Calibri"
        st.font.size = Pt(size)
        st.font.color.rgb = NAVY
        st.font.bold = True
    styles["Heading 1"].paragraph_format.space_before = Pt(16)
    styles["Heading 1"].paragraph_format.space_after = Pt(6)
    styles["Heading 2"].paragraph_format.space_before = Pt(10)
    styles["Heading 2"].paragraph_format.space_after = Pt(4)

    header = section.header.paragraphs[0]
    header.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    run = header.add_run(f"New Relic Error Report  |  {a.service}  |  "
                         f"{a.period_start:%d %b} - {a.period_end:%d %b %Y}")
    run.font.size = Pt(8)
    run.font.color.rgb = MUTED

    footer = section.footer.paragraphs[0]
    footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = footer.add_run("Page ")
    r.font.size, r.font.color.rgb = Pt(8), MUTED
    _field(footer, "PAGE")
    r = footer.add_run(" of ")
    r.font.size, r.font.color.rgb = Pt(8), MUTED
    _field(footer, "NUMPAGES")


# ---- Sections ------------------------------------------------------------
SECTIONS = [
    ("1. Executive Summary", "Headline numbers for the period."),
    ("2. Key Findings", "Observations generated from the data."),
    ("3. Suggested Actions", "Follow-ups the data points to, with the evidence for each."),
    ("4. Errors by Category", "How errors split across broad types."),
    ("5. Top Error Patterns", "The 20 most frequent distinct errors."),
    ("6. Daily Trend", "Errors per day by category."),
    ("7. Peak Hours", "The busiest hours and what drove them."),
    ("8. Tenant Impact", "The most affected tenants."),
    ("9. Top Errors per Tenant", "The three most frequent patterns for the ten most affected tenants."),
    ("10. Endpoint Impact", "The API routes with the most errors."),
    ("11. Data Quality", "Completeness, de-duplication and tenant attribution."),
    ("12. Workbook Guide", "What each sheet of the Excel workbook contains."),
    ("13. Glossary", "Terms used in this report."),
]


def _suggested_actions(a):
    total = a.stats["rows_analyzed"] or 1
    cats = a.categories.set_index("Category")
    actions = []

    if "Request Context Log" in cats.index:
        n = int(cats.loc["Request Context Log", "Errors"])
        actions.append(("High", "Log 'User: ... Tenant: ...' context lines at Information/Debug level, or attach "
                                "them as attributes of the real error.",
                        f"{n:,} rows ({n / total:.0%}) carry no error detail but are logged as ERROR."))
    if a.stats["tenant_unattributed"] / total > 0.25:
        actions.append(("High", "Add context.TenantId to the request logging middleware (the 'HTTP ... responded "
                                "500' line) so every error can be tied to a customer.",
                        f"{a.stats['tenant_unattributed']:,} errors ({a.stats['tenant_unattributed'] / total:.0%}) "
                        "have no tenant."))
    if "Authentication" in cats.index:
        n = int(cats.loc["Authentication", "Errors"])
        actions.append(("Medium", "Check that clients refresh access tokens before they expire; consider logging "
                                  "expired tokens as a warning with a 401 response.",
                        f"{n:,} token validation failures (IDX10223 token expired)."))
    dup = a.patterns[a.patterns.Pattern.str.contains("Cannot insert duplicate key", na=False)]
    if not dup.empty:
        d = dup.iloc[0]
        actions.append(("Medium", "Make the failing save idempotent (upsert or check-before-insert) for the table "
                                  "named in the pattern.",
                        f"{d['PatternId']} occurred {d['Errors']:,} times across {d['Tenants']} tenant(s)."))
    http = a.patterns[a.patterns.Category == "HTTP 5xx Response"]
    if not http.empty:
        h = http.iloc[0]
        actions.append(("Medium", f"Investigate the most failing route {h['Pattern']}.",
                        f"{h['Errors']:,} server errors ({h['Share']:.1%} of all errors)."))
    if a.stats["windows_truncated"]:
        actions.append(("Info", "Re-run for a shorter period; some query windows still hit the 5,000-row limit.",
                        f"{a.stats['windows_truncated']} window(s) truncated."))
    if len(a.unresolved_identities):
        actions.append(("Info", "Add the listed e-mail domains to DOMAIN_MAP in tenants.py.",
                        f"{len(a.unresolved_identities)} identity tenant(s) could not be linked to a tenant."))
    return actions


def build_word(a):
    doc = Document()
    _setup(doc, a)
    s = a.stats
    total = s["rows_analyzed"] or 1

    # Title block
    title = doc.add_paragraph(style="Title")
    title.add_run("New Relic Error Report")
    sub = doc.add_paragraph()
    run = sub.add_run(a.service)
    run.font.size, run.bold, run.font.color.rgb = Pt(14), True, MUTED

    heading(doc, "Report Details")
    table(doc, ["Item", "Value"], [
        ("Service", a.service),
        ("New Relic account", a.account_id),
        ("Period (UTC)", f"{a.period_start:%a %d %b %Y %H:%M} - {a.period_end:%a %d %b %Y %H:%M}"),
        ("Length", f"{a.days} day(s)"),
        ("Log level", "ERROR"),
        ("Generated", f"{a.generated_at:%d %b %Y %H:%M} UTC"),
        ("Companion file", "Excel workbook with the same data, charts and full raw logs"),
    ], widths=[1.8, 5.3], kinds={0: "bold"})

    heading(doc, "Contents")
    table(doc, ["Section", "What it covers"], SECTIONS, widths=[2.2, 4.9], kinds={0: "bold"})

    # 1. Executive summary
    heading(doc, SECTIONS[0][0])
    meaning = {
        "Total errors": "ERROR rows analysed after removing duplicates.",
        "Duplicates removed": "Identical rows dropped before analysis.",
        "Distinct error patterns": "Different error types once variable values are ignored.",
        "Tenants affected": "Customers with at least one attributed error.",
        "Endpoints affected": "Distinct API routes that logged errors.",
        "Daily average": "Total errors divided by the number of days with data.",
        "Peak day": "Day with the most errors.",
        "Peak hour (UTC)": "Hour with the most errors.",
    }
    table(doc, ["Metric", "Value", "What it means"],
          [(k, f"{v:,}" if isinstance(v, int) else v, meaning.get(k, "")) for k, v in a.kpis.items()],
          widths=[1.9, 1.7, 3.5], kinds={0: "bold"})

    # 2. Key findings
    heading(doc, SECTIONS[1][0])
    table(doc, ["#", "Area", "Finding"],
          [(i + 1, area, text) for i, (area, text) in enumerate(a.findings)],
          widths=[0.35, 1.55, 5.2], kinds={1: "bold"})

    # 3. Suggested actions
    heading(doc, SECTIONS[2][0])
    note(doc, "Suggestions are derived automatically from the patterns above; validate them with the owning team.")
    table(doc, ["Priority", "Action", "Evidence"], _suggested_actions(a),
          widths=[0.8, 3.6, 2.7], kinds={0: "bold"})

    # 4. Categories
    heading(doc, SECTIONS[3][0])
    table(doc, ["", "Category", "Errors", "Share", "Patterns", "Tenants", "What it means"],
          [(c.Category, c.Category, c.Errors, c.Share, c.Patterns, c.Tenants, c.Description)
           for c in a.categories.itertuples()],
          widths=[0.12, 1.45, 0.7, 0.6, 0.7, 0.65, 2.9],
          kinds={0: "swatch", 1: "bold", 2: "int", 3: "pct", 4: "int", 5: "int"})

    # 5. Top patterns
    heading(doc, SECTIONS[4][0])
    note(doc, "Variable values are replaced by {id}, {time}, {email}, {n}. Full list: 'Error Patterns' sheet.")
    table(doc, ["ID", "Category", "Pattern", "Errors", "Share", "Tenants", "Last seen (UTC)"],
          [(p.PatternId, p.Category, p.Pattern, p.Errors, p.Share, p.Tenants, p.LastSeen)
           for p in a.patterns.head(20).itertuples()],
          widths=[0.45, 1.2, 2.55, 0.6, 0.55, 0.6, 1.15],
          kinds={0: "bold", 3: "int", 4: "pct", 5: "int", 6: "dt"})

    # 6. Daily trend (top 4 categories + other)
    heading(doc, SECTIONS[5][0])
    cats = [c for c in a.daily.columns if c not in ("Date", "Total")]
    shown = cats[:4]
    headers = ["Date"] + shown + (["Other"] if len(cats) > 4 else []) + ["Total"]
    rows = []
    for d in a.daily.itertuples(index=False):
        row = dict(zip(a.daily.columns, d))
        vals = [row["Date"]] + [row[c] for c in shown]
        if len(cats) > 4:
            vals.append(sum(row[c] for c in cats[4:]))
        vals.append(row["Total"])
        rows.append(vals)
    n = len(headers)
    kinds = {0: "date", **{i: "int" for i in range(1, n)}}
    table(doc, headers, rows, widths=[1.4] + [(7.1 - 1.4) / (n - 1)] * (n - 1), kinds=kinds)

    # 7. Peak hours
    heading(doc, SECTIONS[6][0])
    raw = a.raw
    hours = raw.assign(Hour=raw["Timestamp (UTC)"].dt.floor("h"))
    top_hours = hours.groupby("Hour").size().sort_values(ascending=False).head(10)
    rows = []
    for hour, count in top_hours.items():
        sub = hours[hours.Hour == hour]
        real = sub[sub.Category != "Request Context Log"]
        counts = (real if len(real) else sub)["Pattern ID"].value_counts()
        pid = counts.index[0]
        pat = a.patterns.loc[a.patterns.PatternId == pid, "Pattern"].iloc[0]
        rows.append((f"{hour:%a %d %b %H:00}", count, f"{pid} - {pat}", counts.iloc[0]))
    note(doc, "Main pattern ignores Request Context Log lines where a real error is present.")
    table(doc, ["Hour (UTC)", "Errors", "Main pattern in that hour", "Its count"], rows,
          widths=[1.4, 0.7, 4.2, 0.8], kinds={0: "bold", 1: "int", 3: "int"})

    # 8. Tenants
    heading(doc, SECTIONS[7][0])
    note(doc, f"{s['tenant_unattributed']:,} errors ({s['tenant_unattributed'] / total:.0%}) have no tenant and are "
              "not included. Full list: 'Tenants' sheet.")
    table(doc, ["#", "Tenant", "Errors", "Share", "Patterns", "Most frequent pattern", "Last seen (UTC)"],
          [(t.Rank, t.TenantName, t.Errors, t.Share, t.Patterns, t.TopPattern, t.LastSeen)
           for t in a.tenants.head(20).itertuples()],
          widths=[0.3, 1.75, 0.6, 0.55, 0.65, 2.1, 1.15],
          kinds={1: "bold", 2: "int", 3: "pct", 4: "int", 6: "dt"})

    # 9. Top errors per tenant
    heading(doc, SECTIONS[8][0])
    top_tenants = a.tenants.head(10)["TenantName"].tolist()
    ttp = a.tenant_top_patterns
    rows = []
    for name in top_tenants:
        for p in ttp[(ttp.TenantName == name) & (ttp.Rank <= 3)].itertuples():
            rows.append((name if p.Rank == 1 else "", p.Rank, p.PatternId, p.Pattern, p.Errors, p.ShareOfTenant))
    table(doc, ["Tenant", "#", "ID", "Pattern", "Errors", "% of tenant"], rows,
          widths=[1.7, 0.3, 0.5, 3.3, 0.6, 0.7], kinds={0: "bold", 4: "int", 5: "pct"})

    # 10. Endpoints
    heading(doc, SECTIONS[9][0])
    table(doc, ["#", "Endpoint", "Errors", "Share", "Tenants", "Most frequent pattern"],
          [(e.Rank, e.Endpoint, e.Errors, e.Share, e.Tenants, e.TopPattern) for e in a.endpoints.head(15).itertuples()],
          widths=[0.3, 2.2, 0.6, 0.55, 0.6, 2.85], kinds={1: "bold", 2: "int", 3: "pct", 4: "int"})

    # 11. Data quality
    heading(doc, SECTIONS[10][0])
    heading(doc, "Collection and cleaning", level=2)
    table(doc, ["Check", "Value", "Notes"], [
        ("Query windows run", f"{s['windows_queried']:,}", "Each NRQL request returns at most 5,000 rows."),
        ("Windows split for hitting the limit", f"{s['windows_split']:,}", "Busy windows are halved and re-queried."),
        ("Windows still at the limit", f"{s['windows_truncated']:,}",
         "Complete - no rows dropped." if not s["windows_truncated"] else "Some logs may be missing."),
        ("Rows fetched", f"{s['rows_fetched']:,}", ""),
        ("Duplicate rows removed", f"{s['duplicates_removed']:,}", "Same timestamp, message, tenant, path, service."),
        ("Rows analysed", f"{s['rows_analyzed']:,}", "Basis for every figure in this report."),
    ], widths=[2.5, 1.0, 3.6], kinds={0: "bold"})
    heading(doc, "Tenant attribution", level=2)
    table(doc, ["Source", "Rows", "Share"], [
        ("Log context (context.TenantId)", s["tenant_from_context"], s["tenant_from_context"] / total),
        ("TenantId in message text", s["tenant_from_message"], s["tenant_from_message"] / total),
        ("Identity tenant (learned from data)", s["tenant_from_identity"], s["tenant_from_identity"] / total),
        ("User e-mail domain", s["tenant_from_domain"], s["tenant_from_domain"] / total),
        ("Identity tenant not resolved", s["tenant_unresolved_identity"], s["tenant_unresolved_identity"] / total),
        (UNATTRIBUTED + " (no tenant information)", s["tenant_unattributed"], s["tenant_unattributed"] / total),
    ], widths=[4.1, 1.5, 1.5], kinds={0: "bold", 1: "int", 2: "pct"})
    if len(a.unresolved_identities):
        heading(doc, "Unresolved identity tenants", level=2)
        table(doc, ["Identity tenant ID", "User e-mail domains", "Errors", "Last seen (UTC)"],
              [(u.IdentityTenantId, u.UserDomains, u.Errors, u.LastSeen)
               for u in a.unresolved_identities.itertuples()],
              widths=[2.9, 2.0, 0.8, 1.4], kinds={2: "int", 3: "dt"})
    if s["unmapped_tenant_ids"]:
        heading(doc, "Tenant IDs missing from TENANT_MAP", level=2)
        table(doc, ["Tenant ID"], [(t,) for t in s["unmapped_tenant_ids"]], widths=[7.1])

    # 12. Workbook guide
    heading(doc, SECTIONS[11][0])
    table(doc, ["Sheet", "What it shows", "How to use it"], SHEET_GUIDE, widths=[1.4, 2.9, 2.8], kinds={0: "bold"})
    heading(doc, "Error categories", level=2)
    table(doc, ["", "Category", "Meaning"], [(c, c, d) for c, d in CATEGORY_DESCRIPTIONS.items()],
          widths=[0.12, 1.8, 5.2], kinds={0: "swatch", 1: "bold"})

    # 13. Glossary
    heading(doc, SECTIONS[12][0])
    table(doc, ["Term", "Definition"], GLOSSARY, widths=[1.6, 5.5], kinds={0: "bold"})

    out = BytesIO()
    doc.save(out)
    out.seek(0)
    return out
