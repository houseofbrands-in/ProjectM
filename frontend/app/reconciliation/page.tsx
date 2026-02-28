"use client";
export const dynamic = "force-dynamic";

import * as React from "react";
import { useState, useEffect, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useWorkspace } from "@/lib/workspace-context";
import {
  uploadReconPgForward,
  uploadReconPgReverse,
  uploadReconNonOrder,
  uploadReconOrderFlow,
  uploadReconSkuMap,
  getReconSummary,
  getReconCommissionAudit,
  getReconSkuPnl,
  getReconSettlementTracker,
  getReconPenaltyAudit,
} from "@/lib/api";
import { toast } from "sonner";

// ─── Helpers ──────────────────────────────────────────────────────────────
function fmt(n: number | null | undefined) {
  if (n == null) return "—";
  const abs = Math.abs(n);
  const str = "₹" + abs.toLocaleString("en-IN", { maximumFractionDigits: 0 });
  return n < 0 ? "-" + str : str;
}

function fmtPct(n: number | null | undefined) {
  if (n == null) return "—";
  return n.toFixed(1) + "%";
}

function cn(...classes: (string | false | undefined)[]) {
  return classes.filter(Boolean).join(" ");
}

// ─── Upload Slot Component ───────────────────────────────────────────────
function UploadSlot({
  label,
  description,
  onUpload,
}: {
  label: string;
  description: string;
  onUpload: (file: File) => Promise<any>;
}) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setLoading(true);
    setResult(null);
    try {
      const res = await onUpload(file);
      setResult(`✅ ${res.inserted ?? 0} rows ingested`);
      toast.success(`${label}: ${res.inserted ?? 0} rows ingested`);
    } catch (err: any) {
      setResult(`❌ ${err.message}`);
      toast.error(`${label}: ${err.message}`);
    } finally {
      setLoading(false);
      e.target.value = "";
    }
  };

  return (
    <div className="rounded-xl border p-4 space-y-2">
      <div className="flex items-center justify-between">
        <div>
          <div className="font-medium text-sm">{label}</div>
          <div className="text-xs text-muted-foreground">{description}</div>
        </div>
        <label className="cursor-pointer">
          <input type="file" accept=".csv" className="hidden" onChange={handleChange} disabled={loading} />
          <span className={cn(
            "inline-flex items-center rounded-lg px-3 py-1.5 text-xs font-medium border",
            loading ? "bg-muted text-muted-foreground" : "bg-primary text-primary-foreground hover:bg-primary/90"
          )}>
            {loading ? "Uploading..." : "Upload CSV"}
          </span>
        </label>
      </div>
      {result && <div className="text-xs">{result}</div>}
    </div>
  );
}

// ─── KPI Card ─────────────────────────────────────────────────────────────
function KpiCard({ title, value, sub, negative }: { title: string; value: string; sub?: string; negative?: boolean }) {
  return (
    <Card>
      <CardContent className="pt-4 pb-3 px-4">
        <div className="text-xs text-muted-foreground mb-1">{title}</div>
        <div className={cn("text-xl font-bold", negative ? "text-red-600" : "text-foreground")}>{value}</div>
        {sub && <div className="text-xs text-muted-foreground mt-1">{sub}</div>}
      </CardContent>
    </Card>
  );
}

// ─── Sortable Header ──────────────────────────────────────────────────────
function SortHeader({
  label,
  field,
  sortBy,
  sortDir,
  onSort,
  align,
}: {
  label: string;
  field: string;
  sortBy: string;
  sortDir: string;
  onSort: (field: string) => void;
  align?: "left" | "right";
}) {
  const active = sortBy === field;
  return (
    <th
      className={cn(
        "pb-2 pr-2 cursor-pointer hover:text-foreground select-none",
        align === "right" && "text-right",
        active ? "text-foreground" : "text-muted-foreground"
      )}
      onClick={() => onSort(field)}
    >
      {label}
      {active && <span className="ml-1">{sortDir === "asc" ? "↑" : "↓"}</span>}
    </th>
  );
}

// ─── Tabs ─────────────────────────────────────────────────────────────────
type Tab = "overview" | "upload" | "commission" | "sku-pnl" | "penalties";

const TABS: { key: Tab; label: string }[] = [
  { key: "overview", label: "Overview" },
  { key: "upload", label: "Upload Data" },
  { key: "commission", label: "Commission Audit" },
  { key: "sku-pnl", label: "SKU P&L" },
  { key: "penalties", label: "Penalties" },
];

// ═════════════════════════════════════════════════════════════════════════
export default function ReconciliationPage() {
  const { workspaceSlug } = useWorkspace();
  const [tab, setTab] = useState<Tab>("overview");
  const [summary, setSummary] = useState<any>(null);
  const [commission, setCommission] = useState<any>(null);
  const [skuPnl, setSkuPnl] = useState<any>(null);
  const [penalties, setPenalties] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [sum, comm, pnl, pen] = await Promise.all([
        getReconSummary({ workspace_slug: workspaceSlug }).catch(() => null),
        getReconCommissionAudit({ workspace_slug: workspaceSlug }).catch(() => null),
        getReconSkuPnl({ workspace_slug: workspaceSlug, top_n: 200 }).catch(() => null),
        getReconPenaltyAudit({ workspace_slug: workspaceSlug }).catch(() => null),
      ]);
      setSummary(sum);
      setCommission(comm);
      setSkuPnl(pnl);
      setPenalties(pen);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [workspaceSlug]);

  useEffect(() => { loadData(); }, [loadData]);

  const fw = summary?.forward;
  const rv = summary?.reverse;
  const no = summary?.non_order;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold">Payment Reconciliation</h1>
          <p className="text-sm text-muted-foreground">Myntra settlement tracking &amp; P&amp;L analysis</p>
        </div>
        <Button variant="outline" size="sm" onClick={loadData} disabled={loading}>
          {loading ? "Loading..." : "Refresh"}
        </Button>
      </div>

      {error && <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">{error}</div>}

      <div className="flex gap-1 border-b pb-1">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cn(
              "px-3 py-1.5 rounded-t-lg text-sm font-medium transition-colors",
              tab === t.key ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted"
            )}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "overview" && <OverviewTab fw={fw} rv={rv} no={no} summary={summary} />}
      {tab === "upload" && <UploadTab workspaceSlug={workspaceSlug} onDone={loadData} />}
      {tab === "commission" && <CommissionTab data={commission} />}
      {tab === "sku-pnl" && <SkuPnlTab data={skuPnl} workspaceSlug={workspaceSlug} />}
      {tab === "penalties" && <PenaltyTab data={penalties} />}
    </div>
  );
}

// ═══ Overview Tab ══════════════════════════════════════════════════════════
function OverviewTab({ fw, rv, no, summary }: any) {
  if (!fw) return <div className="text-muted-foreground text-sm py-8 text-center">No data yet. Upload settlement files first.</div>;

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard title="Forward Sales" value={fmt(fw?.total_seller_amount)} sub={`${fw?.orders || 0} orders`} />
        <KpiCard title="Return Deductions" value={fmt(rv?.total_seller_amount)} sub={`${rv?.orders || 0} returns`} negative />
        <KpiCard title="Net Settlement" value={fmt(summary?.net_settlement)} sub="Received in bank" />
        <KpiCard title="Pending Settlement" value={fmt(summary?.total_pending)} sub="Yet to receive" negative={summary?.total_pending > 0} />
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Deductions Breakdown (Forward)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {[
              { label: "Total Seller Amount", value: fw?.total_seller_amount, color: "bg-green-500" },
              { label: "Commission (incl. Platform Fees)", value: -(fw?.deductions?.commission || 0), color: "bg-red-400" },
              { label: "Logistics (incl. Shipping, Pick&Pack)", value: -(fw?.deductions?.logistics || 0), color: "bg-orange-400" },
              { label: "TCS", value: -(fw?.deductions?.tcs || 0), color: "bg-blue-300" },
              { label: "TDS", value: -(fw?.deductions?.tds || 0), color: "bg-blue-400" },
            ].map((item) => {
              const maxVal = fw?.total_seller_amount || 1;
              const pct = Math.min(Math.abs(item.value || 0) / maxVal * 100, 100);
              return (
                <div key={item.label} className="flex items-center gap-3">
                  <div className="w-56 text-xs text-right text-muted-foreground shrink-0">{item.label}</div>
                  <div className="flex-1 h-5 bg-muted rounded overflow-hidden">
                    <div className={cn("h-full rounded", item.color)} style={{ width: `${pct}%` }} />
                  </div>
                  <div className={cn("w-24 text-xs font-medium text-right", (item.value || 0) < 0 && "text-red-600")}>
                    {fmt(item.value)}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Logistics breakdown */}
          {fw?.deductions?.logistics_breakdown && (
            <div className="mt-3 pt-3 border-t space-y-1">
              <div className="text-xs font-medium text-muted-foreground mb-1">Logistics Breakdown:</div>
              {Object.entries(fw.deductions.logistics_breakdown).map(([k, v]: [string, any]) => (
                <div key={k} className="flex justify-between text-xs text-muted-foreground">
                  <span className="pl-4">{k.replace(/_/g, " ")}</span>
                  <span>{fmt(v)}</span>
                </div>
              ))}
            </div>
          )}

          <div className="mt-4 pt-3 border-t flex justify-between items-center">
            <span className="text-sm font-medium">Total Deductions</span>
            <span className="text-lg font-bold text-red-600">{fmt(fw?.deductions?.total)}</span>
          </div>
          <div className="mt-2 flex justify-between items-center">
            <span className="text-sm font-medium">Settled Amount</span>
            <span className="text-lg font-bold text-green-600">{fmt(fw?.settled)}</span>
          </div>
        </CardContent>
      </Card>

      {(no?.count || 0) > 0 && (
        <Card>
          <CardContent className="pt-4">
            <div className="flex justify-between items-center">
              <div>
                <div className="font-medium text-sm">Non-Order Deductions</div>
                <div className="text-xs text-muted-foreground">{no?.count} entries (penalties, SPF claims, adjustments)</div>
              </div>
              <div className="text-lg font-bold text-red-600">{fmt(no?.total_amount)}</div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// ═══ Upload Tab ═══════════════════════════════════════════════════════════
function UploadTab({ workspaceSlug, onDone }: { workspaceSlug: string; onDone: () => void }) {
  return (
    <div className="space-y-4">
      <div className="text-sm text-muted-foreground">
        Upload Myntra settlement reports for <Badge variant="outline">{workspaceSlug}</Badge>
      </div>
      <div className="grid md:grid-cols-2 gap-3">
        <UploadSlot label="PG Forward Settled" description="Forward sales that Myntra has settled"
          onUpload={(f) => uploadReconPgForward(f, { workspace_slug: workspaceSlug, status: "settled" })} />
        <UploadSlot label="PG Forward Unsettled" description="Forward sales pending settlement"
          onUpload={(f) => uploadReconPgForward(f, { workspace_slug: workspaceSlug, status: "unsettled" })} />
        <UploadSlot label="PG Reverse Settled" description="Returns/RTO that Myntra has settled"
          onUpload={(f) => uploadReconPgReverse(f, { workspace_slug: workspaceSlug, status: "settled" })} />
        <UploadSlot label="PG Reverse Unsettled" description="Returns/RTO pending settlement"
          onUpload={(f) => uploadReconPgReverse(f, { workspace_slug: workspaceSlug, status: "unsettled" })} />
        <UploadSlot label="Non-Order Settlement" description="Penalties, SPF claims, adjustments"
          onUpload={(f) => uploadReconNonOrder(f, { workspace_slug: workspaceSlug })} />
        <UploadSlot label="Order Flow" description="Master order lifecycle (forward + reverse)"
          onUpload={(f) => uploadReconOrderFlow(f, { workspace_slug: workspaceSlug })} />
        <UploadSlot label="Listings Report (SKU Map)" description="Maps Myntra SKU codes to your seller SKU codes"
          onUpload={(f) => uploadReconSkuMap(f, { workspace_slug: workspaceSlug })} />
      </div>
      <Button variant="outline" size="sm" onClick={onDone}>Refresh Data</Button>
    </div>
  );
}

// ═══ Commission Audit Tab ═════════════════════════════════════════════════
function CommissionTab({ data }: { data: any }) {
  if (!data) return <div className="text-muted-foreground text-sm py-8 text-center">No data available.</div>;
  const dist = data.rate_distribution || {};
  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-base">Commission Rate Distribution</CardTitle></CardHeader>
        <CardContent>
          <div className="text-xs text-muted-foreground mb-3">{data.total_orders} orders analyzed</div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-muted-foreground">
                <th className="pb-2">Rate %</th>
                <th className="pb-2 text-right">Orders</th>
                <th className="pb-2 text-right">Total Commission</th>
                <th className="pb-2 text-right">Avg/Order</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(dist).map(([rate, info]: [string, any]) => (
                <tr key={rate} className="border-b last:border-0">
                  <td className="py-2 font-medium">{rate}%</td>
                  <td className="py-2 text-right">{info.count}</td>
                  <td className="py-2 text-right">{fmt(info.total_commission)}</td>
                  <td className="py-2 text-right">{fmt(info.avg_commission)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  );
}

// ═══ SKU P&L Tab ══════════════════════════════════════════════════════════
function SkuPnlTab({ data, workspaceSlug }: { data: any; workspaceSlug: string }) {
  const [sortBy, setSortBy] = useState("net_profit");
  const [sortDir, setSortDir] = useState("desc");

  if (!data?.rows?.length) return <div className="text-muted-foreground text-sm py-8 text-center">No data available. Upload PG Forward and PG Reverse files first.</div>;

  const handleSort = (field: string) => {
    if (sortBy === field) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortBy(field);
      setSortDir("desc");
    }
  };

  // Client-side sort
  const sortedRows = [...(data.rows || [])].sort((a: any, b: any) => {
    const av = a[sortBy] ?? 0;
    const bv = b[sortBy] ?? 0;
    return sortDir === "asc" ? av - bv : bv - av;
  });

  const handleDownload = () => {
    window.open(`/api/db/recon/sku-pnl/download?workspace_slug=${workspaceSlug}&sort_by=${sortBy}&sort_dir=${sortDir}`, "_blank");
  };

  const totals = data.totals || {};

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">SKU-Level Profit &amp; Loss</CardTitle>
            <Button variant="outline" size="sm" onClick={handleDownload}>
              ⬇ Download CSV
            </Button>
          </div>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          {/* Totals row */}
          {totals.gross_revenue > 0 && (
            <div className="flex gap-4 mb-3 text-xs bg-muted/50 rounded-lg p-3">
              <span>Orders: <strong>{totals.forward_orders}</strong></span>
              <span>Returns: <strong>{totals.return_orders}</strong></span>
              <span>Revenue: <strong>{fmt(totals.gross_revenue)}</strong></span>
              <span>Returns: <strong className="text-red-600">{fmt(totals.return_deduction)}</strong></span>
              <span>Commission: <strong className="text-red-600">{fmt(totals.commission)}</strong></span>
              <span>Logistics: <strong className="text-red-600">{fmt(totals.logistics)}</strong></span>
              <span>Net Profit: <strong className={totals.net_profit >= 0 ? "text-green-600" : "text-red-600"}>{fmt(totals.net_profit)}</strong></span>
            </div>
          )}

          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-xs">
                <SortHeader label="Seller SKU" field="seller_sku_code" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
                <SortHeader label="Brand" field="brand" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
                <SortHeader label="Orders" field="forward_orders" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Returns" field="return_orders" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Ret%" field="return_pct" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Revenue" field="gross_revenue" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Returns ₹" field="return_deduction" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Commission" field="commission" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Logistics" field="logistics" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Tax" field="tcs" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Net Profit" field="net_profit" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="Margin" field="margin_pct" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
                <SortHeader label="ASP" field="asp" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} align="right" />
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((r: any, i: number) => (
                <tr key={i} className="border-b last:border-0 hover:bg-muted/50">
                  <td className="py-1.5 pr-2">
                    <div className="font-medium">{r.seller_sku_code || r.sku_code}</div>
                    {r.seller_sku_code && (
                      <div className="text-[10px] text-muted-foreground">{r.sku_code}</div>
                    )}
                    {r.style_name && (
                      <div className="text-[10px] text-muted-foreground truncate max-w-[200px]">{r.style_name}</div>
                    )}
                  </td>
                  <td className="py-1.5 pr-2">{r.brand}</td>
                  <td className="py-1.5 text-right pr-2">{r.forward_orders}</td>
                  <td className="py-1.5 text-right pr-2">{r.return_orders}</td>
                  <td className="py-1.5 text-right pr-2">{fmtPct(r.return_pct)}</td>
                  <td className="py-1.5 text-right pr-2">{fmt(r.gross_revenue)}</td>
                  <td className="py-1.5 text-right pr-2 text-red-600">{fmt(r.return_deduction)}</td>
                  <td className="py-1.5 text-right pr-2 text-red-600">{fmt(r.commission)}</td>
                  <td className="py-1.5 text-right pr-2 text-red-600">{fmt(r.logistics)}</td>
                  <td className="py-1.5 text-right pr-2 text-red-600">{fmt((r.tcs || 0) + (r.tds || 0))}</td>
                  <td className={cn("py-1.5 text-right pr-2 font-medium", r.net_profit >= 0 ? "text-green-600" : "text-red-600")}>
                    {fmt(r.net_profit)}
                  </td>
                  <td className={cn("py-1.5 text-right pr-2 font-medium", r.margin_pct >= 0 ? "text-green-600" : "text-red-600")}>
                    {fmtPct(r.margin_pct)}
                  </td>
                  <td className="py-1.5 text-right">{fmt(r.asp)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  );
}

// ═══ Penalty Tab ══════════════════════════════════════════════════════════
function PenaltyTab({ data }: { data: any }) {
  if (!data?.rows?.length) return <div className="text-muted-foreground text-sm py-8 text-center">No penalty/non-order deductions found.</div>;
  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-base">Non-Order Deductions &amp; Penalties</CardTitle></CardHeader>
        <CardContent>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-muted-foreground">
                <th className="pb-2">Description</th>
                <th className="pb-2">Type</th>
                <th className="pb-2 text-right">Count</th>
                <th className="pb-2 text-right">Total Amount</th>
              </tr>
            </thead>
            <tbody>
              {data.rows.map((r: any, i: number) => (
                <tr key={i} className="border-b last:border-0">
                  <td className="py-2">{r.description || "—"}</td>
                  <td className="py-2"><Badge variant={r.type === "credit" ? "default" : "destructive"}>{r.type}</Badge></td>
                  <td className="py-2 text-right">{r.count}</td>
                  <td className={cn("py-2 text-right font-medium", r.total_amount < 0 ? "text-red-600" : "text-green-600")}>
                    {fmt(r.total_amount)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  );
}