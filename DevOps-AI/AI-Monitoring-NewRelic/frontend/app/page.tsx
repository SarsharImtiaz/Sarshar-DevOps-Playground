"use client";
import React, { useState } from "react";
import { FileSpreadsheet, FileText, Loader2 } from "lucide-react";

const API_URL = "http://localhost:8000";

type ReportKind = "excel" | "word";

const REPORTS: Record<ReportKind, { label: string; fallbackName: string }> = {
  excel: { label: "Excel Workbook", fallbackName: "newrelic_errors.xlsx" },
  word: { label: "Word Report", fallbackName: "newrelic_errors.docx" },
};

function filenameFrom(response: Response, fallback: string) {
  const header = response.headers.get("Content-Disposition") || "";
  const match = header.match(/filename="?([^";]+)"?/);
  return match ? match[1] : fallback;
}

export default function Home() {
  const [apiKey, setApiKey] = useState("");
  const [accountId, setAccountId] = useState("");
  const [serviceName, setServiceName] = useState("");
  const [days, setDays] = useState(7);
  const [loading, setLoading] = useState<ReportKind | null>(null);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");

  const canSubmit = apiKey && accountId && serviceName && !loading;

  const download = async (kind: ReportKind) => {
    setLoading(kind);
    setError("");
    setStatus("");
    try {
      const response = await fetch(`${API_URL}/api/report/${kind}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ apiKey, accountId, serviceName, days }),
      });

      if (!response.ok) {
        const body = await response.json().catch(() => null);
        throw new Error(body?.detail || `Request failed (${response.status})`);
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filenameFrom(response, REPORTS[kind].fallbackName);
      a.click();
      window.URL.revokeObjectURL(url);
      setStatus(`${REPORTS[kind].label} downloaded.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error fetching logs");
    }
    setLoading(null);
  };

  const button = (kind: ReportKind, Icon: typeof FileText, primary: boolean) => (
    <button
      onClick={() => download(kind)}
      disabled={!canSubmit}
      style={{
        ...buttonStyle,
        background: primary ? "#2563eb" : "transparent",
        border: primary ? "none" : "1px solid #2563eb",
        opacity: canSubmit || loading === kind ? 1 : 0.5,
        cursor: canSubmit ? "pointer" : "not-allowed",
      }}
    >
      <span style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: "8px" }}>
        {loading === kind ? <Loader2 className="animate-spin" size={16} /> : <Icon size={16} />}
        {loading === kind ? "Generating..." : `Download ${REPORTS[kind].label}`}
      </span>
    </button>
  );

  return (
    <div style={{
      minHeight: "100vh",
      background: "linear-gradient(135deg, #0f172a, #020617)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      fontFamily: "sans-serif",
      padding: "16px"
    }}>
      <div style={{
        width: "100%",
        maxWidth: "420px",
        background: "#0f172a",
        padding: "30px",
        borderRadius: "16px",
        boxShadow: "0 10px 30px rgba(0,0,0,0.5)",
        color: "white"
      }}>
        <h2 style={{ marginBottom: "10px" }}>New Relic Exporter</h2>
        <p style={{ color: "#94a3b8", marginBottom: "20px" }}>
          Generate an error report for a service: an Excel workbook with charts, and a Word summary.
        </p>

        <input
          type="password"
          placeholder="API Key"
          value={apiKey}
          onChange={(e) => setApiKey(e.target.value)}
          style={inputStyle}
        />

        <input
          placeholder="Account ID"
          value={accountId}
          onChange={(e) => setAccountId(e.target.value)}
          style={inputStyle}
        />

        <input
          placeholder="Service Name (e.g. BEC-API-PROD)"
          value={serviceName}
          onChange={(e) => setServiceName(e.target.value)}
          style={inputStyle}
        />

        <label style={{ display: "block", color: "#94a3b8", fontSize: "13px", marginBottom: "6px" }}>
          Period
        </label>
        <select value={days} onChange={(e) => setDays(Number(e.target.value))} style={inputStyle}>
          <option value={1}>Last 24 hours</option>
          <option value={3}>Last 3 days</option>
          <option value={7}>Last 7 days</option>
          <option value={14}>Last 14 days</option>
          <option value={30}>Last 30 days</option>
        </select>

        <div style={{ display: "grid", gap: "10px", marginTop: "8px" }}>
          {button("excel", FileSpreadsheet, true)}
          {button("word", FileText, false)}
        </div>

        <p style={{ color: "#64748b", fontSize: "12px", marginTop: "12px" }}>
          The first download fetches logs from New Relic and can take a minute. The second report re-uses
          the same data for 15 minutes.
        </p>

        {status && <p style={{ color: "#4ade80", fontSize: "13px", marginTop: "8px" }}>{status}</p>}
        {error && <p style={{ color: "#f87171", fontSize: "13px", marginTop: "8px" }}>{error}</p>}
      </div>
    </div>
  );
}

const inputStyle = {
  width: "100%",
  padding: "10px",
  marginBottom: "12px",
  borderRadius: "8px",
  border: "1px solid #1e293b",
  background: "#020617",
  color: "white"
};

const buttonStyle = {
  width: "100%",
  padding: "12px",
  borderRadius: "10px",
  color: "white",
  fontWeight: "bold",
};
