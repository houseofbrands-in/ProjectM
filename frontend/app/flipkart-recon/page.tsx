"use client";
export const dynamic = "force-dynamic";

import * as React from "react";
import { useState, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useWorkspace } from "@/lib/workspace-context";
import {
  uploadFkSkuPnl,
  uploadFkOrderPnl,
  uploadFkPaymentReport,
  uploadCostPrices,
  getFkReconSummary,
  getFkSkuPnl,
  getTruePnl,
} from "@/lib/api";
import { toast } from "sonner";

function fmt(n: number | null | undefined) {
  if (n == null) return "—";
  const abs = Math.abs(n);
  const str = "₹" + abs.toLocaleString("en-IN", { maximumFractionDigits: 0 });
  return n < 0 ? "-" + str : str;
}
function fmtPct(n: number | null | undefined) { return n == null ? "—" : n.toFixed(1) + "%"; }
function cn(...c: (string | false | undefined)[]) { return c.filter(Boolean).join(" "); }

function UploadSlot({ label, description, onUpload, accept }: {
  label: string; description: string; onUpload: (file: File) => Promise<any>; accept?: string;
}) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const handleChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]; if (!file) return;
    setLoading(true); setResult(null);
    try {
      const res = await onUpload(file);
      setResult(`✅ ${res.inserted ?? 0} rows ingested`);
      toast.success(`${label}: ${res.inserted ?? 0} rows ingested`);
    } catch (err: any) { setResult(`❌ ${err.message}`); toast.error(`${label}: ${err.message}`); }
    finally { setLoading(false); e.target.value = ""; }
  };
  return (
    <div className="rounded-xl border p-4 space-y-2">
      <div className="flex items-center justify-between">
        <div><div className="font-medium text-sm">{label}</div>
          <div className="text-xs text-muted-foreground">{description}</div></div>
        <label className="cursor-pointer">
          <input type="file" accept={accept || ".xlsx,.xls,.csv"} className="hidden" onChange={handleChange} disabled={loading} />
          <span className={cn("inline-flex items-center rounded-lg px-3 py-1.5 text-xs font-medium border",
            loading ? "bg-muted text-muted-foreground" : "bg-primary text-primary-foreground hover:bg-primary/90")}>
            {loading ? "Uploading..." : "Upload"}
          </span>
        </label>
      </div>
      {result && <div className="text-xs">{result}</div>}
    </div>
  );
}

function KpiCard({ title, value, sub, negative }: { title: string; value: string; sub?: string; negative?: boolean }) {
  return (<Card><CardContent className="pt-4 pb-3 px-4">
    <div className="text-xs text-muted-foreground mb-1">{title}</div>
    <div className={cn("text-xl font-bold", negative ? "text-red-600" : "text-foreground")}>{value}</div>
    {sub && <div className="text-xs text-muted-foreground mt-1">{sub}</div>}
  </CardContent></Card>);
}

function SortHeader({ label, field, sortBy, sortDir, onSort, align }: {
  label: string; field: string; sortBy: string; sortDir: string; onSort: (f: string) => void; align?: "right";
}) {
  const active = sortBy === field;
  return (
    <th className={cn("pb-2 pr-2 cursor-pointer hover:text-foreground select-none",
      align === "right" && "text-right", active ? "text-foreground" : "text-muted-foreground")}
      onClick={() => onSort(field)}>
      {label}{active && <span className="ml-1">{sortDir === "asc" ? "↑" : "↓"}</span>}
    </th>
  );
}

type Tab = "overview" | "upload" | "sku-pnl" | "true-pnl";
const TABS: { key: Tab; label: string }[] = [
  { key: "overview", label: "Overview" },
  { key: "upload", label: "Upload Data" },
  { key: "sku-pnl", label: "SKU P&L" },
  { key: "true-pnl", label: "True P&L" },
];

export default function FlipkartReconPage() {
  const { workspaceSlug } = useWorkspace();
  const [tab, setTab] = useState<Tab>("overview");
  const [summary, setSummary] = useState<any>(null);
  const [skuPnl, setSkuPnl] = useState<any>(null);
  const [truePnl, setTruePnl] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const [sum, pnl, tp] = await Promise.all([
        getFkReconSummary({ workspace_slug: workspaceSlug }).catch(() => null),
        getFkSkuPnl({ workspace_slug: workspaceSlug, top_n: 200 }).catch(() => null),
        getTruePnl({ workspace_slug: workspaceSlug, platform: "flipkart" }).catch(() => null),
      ]);
      setSummary(sum); setSkuPnl(pnl); setTruePnl(tp);
    } catch (e: any) { setError(e.message); }
    finally { setLoading(false); }
  }, [workspaceSlug]);

  useEffect(() => { loadData(); }, [loadData]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div><h1 className="text-xl font-bold">Flipkart Reconciliation</h1>
          <p className="text-sm text-muted-foreground">Payment settlement &amp; P&amp;L analysis</p></div>
        <Button variant="outline" size="sm" onClick={loadData} disabled={loading}>{loading ? "Loading..." : "Refresh"}</Button>
      </div>
      {error && <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">{error}</div>}
      <div className="flex gap-1 border-b pb-1">
        {TABS.map((t) => (
          <button key={t.key} onClick={() => setTab(t.key)}
            className={cn("px-3 py-1.5 rounded-t-lg text-sm font-medium transition-colors",
              tab === t.key ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted")}>
            {t.label}
          </button>
        ))}
      </div>
      {tab === "overview" && <OverviewTab summary={summary} />}
      {tab === "upload" && <UploadTab workspaceSlug={workspaceSlug} onDone={loadData} />}
      {tab === "sku-pnl" && <SkuPnlTab data={skuPnl} workspaceSlug={workspaceSlug} />}
      {tab === "true-pnl" && <TruePnlTab data={truePnl} workspaceSlug={workspaceSlug} onSetup={() => setTab("upload")} />}
    </div>
  );
}

// ═══ Overview ═══
function OverviewTab({ summary }: { summary: any }) {
  if (!summary || !summary.sku_count) return <div className="text-muted-foreground text-sm py-8 text-center">No data yet. Upload the PNL Report first.</div>;
  const e = summary.expenses || {}; const s = summary.settlement || {};
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard title="Net Sales" value={fmt(summary.net_sales)} sub={`${summary.units?.gross || 0} gross units`} />
        <KpiCard title="Total Expenses" value={fmt(e.total)} sub={`${summary.units?.returned || 0} returns`} negative />
        <KpiCard title="Net Earnings" value={fmt(summary.net_earnings)} sub={`${summary.sku_count} SKUs`} />
        <KpiCard title="Settled" value={fmt(s.settled)} sub={s.pending > 0 ? `₹${s.pending} pending` : "Fully settled"} />
      </div>
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-base">Expense Breakdown</CardTitle></CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[
              { label: "Net Sales", value: summary.net_sales, color: "bg-green-500" },
              { label: "Commission", value: e.commission, color: "bg-red-400" },
              { label: "Collection Fee", value: e.collection_fee, color: "bg-red-300" },
              { label: "Fixed Fee", value: e.fixed_fee, color: "bg-orange-400" },
              { label: "Forward Shipping", value: e.forward_shipping, color: "bg-orange-300" },
              { label: "Reverse Shipping", value: e.reverse_shipping, color: "bg-yellow-400" },
              { label: "Pick & Pack", value: e.pick_and_pack, color: "bg-yellow-300" },
              { label: "Offer Adjustments", value: e.offer_adjustments, color: "bg-purple-300" },
              { label: "GST on Fees", value: e.gst, color: "bg-blue-300" },
              { label: "TCS", value: e.tcs, color: "bg-blue-400" },
              { label: "TDS", value: e.tds, color: "bg-blue-500" },
            ].filter(i => (i.value || 0) > 0).map((item) => {
              const maxVal = summary.net_sales || 1;
              const pct = Math.min((item.value || 0) / maxVal * 100, 100);
              return (
                <div key={item.label} className="flex items-center gap-3">
                  <div className="w-40 text-xs text-right text-muted-foreground shrink-0">{item.label}</div>
                  <div className="flex-1 h-5 bg-muted rounded overflow-hidden">
                    <div className={cn("h-full rounded", item.color)} style={{ width: `${pct}%` }} />
                  </div>
                  <div className="w-20 text-xs font-medium text-right text-red-600">{fmt(item.value)}</div>
                </div>
              );
            })}
          </div>
          <div className="mt-4 pt-3 border-t flex justify-between items-center">
            <span className="text-sm font-medium">Net Earnings</span>
            <span className="text-lg font-bold text-green-600">{fmt(summary.net_earnings)}</span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

// ═══ Upload ═══
function UploadTab({ workspaceSlug, onDone }: { workspaceSlug: string; onDone: () => void }) {
  return (
    <div className="space-y-4">
      <div className="text-sm text-muted-foreground">Upload Flipkart reports for <Badge variant="outline">{workspaceSlug}</Badge></div>
      <div className="grid md:grid-cols-2 gap-3">
        <UploadSlot label="PNL Report (SKU-level)" description="Upload pnl_report.xlsx — reads SKU-level P&L sheet"
          onUpload={(f) => uploadFkSkuPnl(f, { workspace_slug: workspaceSlug })} />
        <UploadSlot label="PNL Report (Order-level)" description="Upload pnl_report.xlsx — reads Orders P&L sheet"
          onUpload={(f) => uploadFkOrderPnl(f, { workspace_slug: workspaceSlug })} />
        <UploadSlot label="Payment Report" description="Upload payment_report.xlsx — bank settlement details"
          onUpload={(f) => uploadFkPaymentReport(f, { workspace_slug: workspaceSlug })} />
      </div>

      {/* Cost Price Section */}
      <div className="pt-4 border-t">
        <div className="text-sm font-medium mb-2">Cost Price (for True P&amp;L)</div>
        <div className="text-xs text-muted-foreground mb-3">Download template with your SKU codes pre-filled, add cost prices, upload back.</div>
        <div className="grid md:grid-cols-2 gap-3">
          <div className="rounded-xl border p-4">
            <div className="font-medium text-sm mb-1">1. Download Template</div>
            <div className="text-xs text-muted-foreground mb-2">CSV with seller SKU codes pre-filled</div>
            <Button variant="outline" size="sm" onClick={() => {
              window.open(`/api/db/recon/cost-price/template?workspace_slug=${workspaceSlug}&platform=flipkart`, "_blank");
            }}>⬇ Download Template</Button>
          </div>
          <UploadSlot label="2. Upload Cost Prices" description="Upload filled CSV with cost_price column"
            accept=".csv,.xlsx"
            onUpload={(f) => uploadCostPrices(f, { workspace_slug: workspaceSlug })} />
        </div>
      </div>

      <Button variant="outline" size="sm" onClick={onDone}>Refresh Data</Button>
    </div>
  );
}

// ═══ SKU P&L ═══
function SkuPnlTab({ data, workspaceSlug }: { data: any; workspaceSlug: string }) {
  const [sortBy, setSortBy] = useState("net_earnings");
  const [sortDir, setSortDir] = useState("desc");
  if (!data?.rows?.length) return <div className="text-muted-foreground text-sm py-8 text-center">No data available.</div>;
  const handleSort = (field: string) => {
    if (sortBy === field) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else { setSortBy(field); setSortDir("desc"); }
  };
  const sortedRows = [...(data.rows || [])].sort((a: any, b: any) => {
    const av = a[sortBy] ?? 0, bv = b[sortBy] ?? 0;
    return sortDir === "asc" ? av - bv : bv - av;
  });
  const t = data.totals || {};
  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">Flipkart SKU P&amp;L</CardTitle>
          <Button variant="outline" size="sm" onClick={() => window.open(`/api/db/recon/flipkart/sku-pnl/download?workspace_slug=${workspaceSlug}&sort_by=${sortBy}&sort_dir=${sortDir}`, "_blank")}>⬇ CSV</Button>
        </div>
      </CardHeader>
      <CardContent className="overflow-x-auto">
        {t.net_sales > 0 && (
          <div className="flex gap-4 mb-3 text-xs bg-muted/50 rounded-lg p-3 flex-wrap">
            <span>Gross: <strong>{t.gross_units}</strong></span>
            <span>Returns: <strong>{t.returned_units}</strong></span>
            <span>Sales: <strong>{fmt(t.net_sales)}</strong></span>
            <span>Expenses: <strong className="text-red-600">{fmt(t.total_expenses)}</strong></span>
            <span>Earnings: <strong className={t.net_earnings >= 0 ? "text-green-600" : "text-red-600"}>{fmt(t.net_earnings)}</strong></span>
          </div>
        )}
        <table className="w-full text-xs">
          <thead><tr className="border-b text-xs">
            <SortHeader label="SKU" field="sku_id" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
            <SortHeader label="Gross" field="gross_units" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Returns" field="returned_units" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Net" field="net_units" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Ret%" field="return_pct" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Net Sales" field="net_sales" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Expenses" field="total_expenses" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Commission" field="commission" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Rev Ship" field="reverse_shipping" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Earnings" field="net_earnings" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            <SortHeader label="Margin" field="margin_pct" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
          </tr></thead>
          <tbody>
            {sortedRows.map((r: any, i: number) => (
              <tr key={i} className="border-b last:border-0 hover:bg-muted/50">
                <td className="py-1.5 pr-2"><div className="font-medium">{r.sku_id}</div>
                  {r.sku_name && <div className="text-[10px] text-muted-foreground truncate max-w-[180px]">{r.sku_name}</div>}</td>
                <td className="py-1.5 text-right pr-2">{r.gross_units}</td>
                <td className="py-1.5 text-right pr-2">{r.returned_units}</td>
                <td className="py-1.5 text-right pr-2">{r.net_units}</td>
                <td className="py-1.5 text-right pr-2">{fmtPct(r.return_pct)}</td>
                <td className="py-1.5 text-right pr-2">{fmt(r.net_sales)}</td>
                <td className="py-1.5 text-right pr-2 text-red-600">{fmt(r.total_expenses)}</td>
                <td className="py-1.5 text-right pr-2 text-red-600">{fmt(r.commission)}</td>
                <td className="py-1.5 text-right pr-2 text-red-600">{fmt(r.reverse_shipping)}</td>
                <td className={cn("py-1.5 text-right pr-2 font-medium", r.net_earnings >= 0 ? "text-green-600" : "text-red-600")}>{fmt(r.net_earnings)}</td>
                <td className={cn("py-1.5 text-right font-medium", r.margin_pct >= 0 ? "text-green-600" : "text-red-600")}>{fmtPct(r.margin_pct)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  );
}

// ═══ True P&L ═══
function TruePnlTab({ data, workspaceSlug, onSetup }: { data: any; workspaceSlug: string; onSetup: () => void }) {
  const [sortBy, setSortBy] = useState("true_profit");
  const [sortDir, setSortDir] = useState("desc");
  const rows = data?.rows || [];
  const totals = data?.totals || {};
  const sorted = [...rows].sort((a: any, b: any) => {
    const av = a[sortBy] ?? -999999, bv = b[sortBy] ?? -999999;
    return sortDir === "asc" ? av - bv : bv - av;
  });
  const handleSort = (field: string) => {
    if (sortBy === field) setSortDir(sortDir === "asc" ? "desc" : "asc");
    else { setSortBy(field); setSortDir("desc"); }
  };

  if (!rows.length) return <div className="text-muted-foreground text-sm py-8 text-center">No data. Upload PNL report first.</div>;
  const hasAnyCost = rows.some((r: any) => r.cost_price != null);

  return (
    <div className="space-y-4">
      {!hasAnyCost && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          ⚠️ No cost prices uploaded yet. Go to <button className="underline font-medium" onClick={onSetup}>Upload Data</button> → download template, fill cost prices, and upload.
        </div>
      )}
      {hasAnyCost && totals.uncosted_skus > 0 && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
          ⚠️ <strong>{totals.uncosted_skus} SKUs</strong> missing cost prices.
        </div>
      )}
      {hasAnyCost && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <KpiCard title="MP Earnings" value={fmt(totals.marketplace_earnings)} sub="After deductions" />
          <KpiCard title="Total COGS" value={fmt(totals.total_cogs)} sub={`${totals.costed_skus} SKUs`} negative />
          <KpiCard title="True Profit" value={fmt(totals.total_true_profit)} negative={(totals.total_true_profit || 0) < 0} />
          <KpiCard title="True Margin" value={totals.net_sales ? fmtPct((totals.total_true_profit || 0) / totals.net_sales * 100) : "—"} />
        </div>
      )}
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">True P&amp;L (with Cost Price)</CardTitle>
            <Button variant="outline" size="sm" onClick={() =>
              window.open(`/api/db/recon/cost-price/true-pnl/download?workspace_slug=${workspaceSlug}&platform=flipkart`, "_blank")
            }>⬇ CSV</Button>
          </div>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead><tr className="border-b text-xs">
              <SortHeader label="SKU" field="seller_sku_code" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
              <SortHeader label="Gross" field="gross_units" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="Net" field="net_units" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="Ret%" field="return_pct" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="Net Sales" field="net_sales" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="MP Earnings" field="marketplace_earnings" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="Cost/Unit" field="cost_price" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="COGS" field="cogs" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="True Profit" field="true_profit" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="True Margin" field="true_margin_pct" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
            </tr></thead>
            <tbody>
              {sorted.map((r: any, i: number) => (
                <tr key={i} className={cn("border-b last:border-0 hover:bg-muted/50",
                  r.true_profit != null && r.true_profit < 0 && "bg-red-50")}>
                  <td className="py-1.5 pr-2"><div className="font-medium">{r.seller_sku_code}</div>
                    {r.sku_name && <div className="text-[10px] text-muted-foreground truncate max-w-[180px]">{r.sku_name}</div>}</td>
                  <td className="py-1.5 text-right pr-2">{r.gross_units}</td>
                  <td className="py-1.5 text-right pr-2">{r.net_units}</td>
                  <td className="py-1.5 text-right pr-2">{fmtPct(r.return_pct)}</td>
                  <td className="py-1.5 text-right pr-2">{fmt(r.net_sales)}</td>
                  <td className="py-1.5 text-right pr-2 text-green-600">{fmt(r.marketplace_earnings)}</td>
                  <td className="py-1.5 text-right pr-2">{r.cost_price ? fmt(r.cost_price) : <span className="text-amber-500">—</span>}</td>
                  <td className="py-1.5 text-right pr-2 text-red-600">{r.cogs != null ? fmt(r.cogs) : "—"}</td>
                  <td className={cn("py-1.5 text-right pr-2 font-bold",
                    r.true_profit == null ? "" : r.true_profit >= 0 ? "text-green-600" : "text-red-600")}>
                    {r.true_profit != null ? fmt(r.true_profit) : "—"}</td>
                  <td className={cn("py-1.5 text-right font-medium",
                    r.true_margin_pct == null ? "" : r.true_margin_pct >= 0 ? "text-green-600" : "text-red-600")}>
                    {r.true_margin_pct != null ? fmtPct(r.true_margin_pct) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  );
}