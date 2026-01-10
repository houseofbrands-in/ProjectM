"use client";

import * as React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useWorkspace } from "@/lib/workspace-context";
import { Button } from "@/components/ui/button";

type SortDir = "asc" | "desc";
type ReturnMode = "overall" | "same_month";
type ReturnType = "all" | "customer";

type SortKey = "gmv" | "orders" | "asp" | "returns" | "return_pct" | "return_amount";

type Row = {
  style_key: string;
  brand: string;
  orders: number;
  gmv: number;
  asp: number;

  // Backward compat
  returns: number;
  return_pct: number;

  // Preferred split fields (available from backend now)
  returns_total_units?: number;
  return_units?: number; // customer returns only (return_type == "RETURN")
  rto_units?: number; // RTO only (return_type == "RTO")
  return_only_pct?: number;
  rto_pct?: number;

  // Only present when return_mode = same_month
  return_amount?: number;
};

type StyleResp = { workspace_slug: string; count: number; rows: Row[]; return_mode?: ReturnMode };

type SummaryResp = {
  orders: number;
  returns_total_units?: number;
  return_units?: number;
  rto_units?: number;
  returns?: number; // backward compat
  rto?: number; // backward compat
  return_pct?: number;
  return_only_pct?: number;
  rto_pct?: number;
};

type GmvAspResp = {
  gmv: number;
  asp: number;
  prev_gmv?: number;
  prev_asp?: number;
  gmv_change_pct?: number | null;
  asp_change_pct?: number | null;
};

function fmtInr(n: number) {
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 0,
  }).format(n);
}

function fmtPct(n: number) {
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(2)}%`;
}

function sortIcon(active: boolean, dir: SortDir) {
  if (!active) return "↕";
  return dir === "asc" ? "↑" : "↓";
}

async function fetchBrands(workspace_slug: string, portal?: string) {
  const qs = new URLSearchParams();
  qs.set("workspace_slug", workspace_slug);
  if (portal) qs.set("portal", portal);

  const res = await fetch(`/api/db/brands?${qs.toString()}`, { cache: "no-store" });
  if (!res.ok) return [];
  const j = await res.json().catch(() => null);
  return (j?.brands ?? []) as string[];
}


async function fetchStyleTable(params: {
  workspace_slug: string;
  portal?: string;
  start: string;
  end: string;
  brand?: string;
  return_mode?: ReturnMode;
  sort?: string;
  dir?: string;
  limit?: number;
  row_dim: string;
}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/kpi/style-gmv-asp?${qs.toString()}`, { cache: "no-store" });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`style-gmv-asp failed: ${res.status} ${t}`.trim());
  }
  return (await res.json()) as StyleResp;
}

function rowLabel(portal: string | undefined, rowDim: "style" | "sku", r: any) {
  if (portal === "flipkart" || rowDim === "sku") return r.seller_sku_code ?? r.style_key;
  return r.style_key;
}

async function fetchSummary(params: {
  workspace_slug: string;
  start: string;
  end: string;
  portal: string;
  brand?: string;
  return_mode?: ReturnMode;
}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/kpi/summary?${qs.toString()}`, { cache: "no-store" });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`summary failed: ${res.status} ${t}`.trim());
  }
  return (await res.json()) as SummaryResp;
}

async function fetchGmvAsp(params: { workspace_slug: string; portal: string; start: string; end: string; brand?: string }) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/kpi/gmv-asp?${qs.toString()}`, { cache: "no-store" });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`gmv-asp failed: ${res.status} ${t}`.trim());
  }
  return (await res.json()) as GmvAspResp;
}

export default function OrdersClient() {
  const { workspaceSlug, portal, start, end } = useWorkspace();

  const [brands, setBrands] = React.useState<string[]>([]);
  const [brand, setBrand] = React.useState<string>("");

  const [returnMode, setReturnMode] = React.useState<ReturnMode>("overall");
  const [returnType, setReturnType] = React.useState<ReturnType>("all");

  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);

  const [styleData, setStyleData] = React.useState<StyleResp | null>(null);
  const [summary, setSummary] = React.useState<SummaryResp | null>(null);
  const [gmvAsp, setGmvAsp] = React.useState<GmvAspResp | null>(null);

  const [sort, setSort] = React.useState<{ key: SortKey; dir: SortDir }>({
    key: "gmv",
    dir: "desc",
  });


  type RowDim = "style" | "sku";
const [rowDim, setRowDim] = React.useState<RowDim>("style");

// Force sku mode for flipkart
React.useEffect(() => {
  if (portal === "flipkart") setRowDim("sku");
  else setRowDim("style");
}, [portal]);

  // If user switches to overall, ensure we’re not sorting by return_amount
  React.useEffect(() => {
    if (returnMode === "overall" && sort.key === "return_amount") {
      setSort({ key: "gmv", dir: "desc" });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [returnMode]);

  React.useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const b = await fetchBrands(workspaceSlug, portal);
        if (!alive) return;
        setBrands(b);
      } catch {
        if (!alive) return;
        setBrands([]);
      }
    })();
    return () => {
      alive = false;
    };
  }, [workspaceSlug, portal]);

  const load = React.useCallback(async () => {
    setLoading(true);
    setErr(null);
    try {
      const [st, sm, ga] = await Promise.all([
        fetchStyleTable({
          workspace_slug: workspaceSlug,
          portal,
          start,
          end,
          brand: brand || undefined,
          return_mode: returnMode,
          sort: sort.key,
          dir: sort.dir,
          limit: 200,
          row_dim: portal === "flipkart" ? "sku" : "style",

        }),
        fetchSummary({
          workspace_slug: workspaceSlug,
          start,
          portal,
          end,
          brand: brand || undefined,
          return_mode: returnMode,
        }),
        fetchGmvAsp({
          workspace_slug: workspaceSlug,
          portal,
          start,
          end,
          brand: brand || undefined,
        }),
      ]);

      setStyleData(st);
      setSummary(sm);
      setGmvAsp(ga);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setStyleData(null);
      setSummary(null);
      setGmvAsp(null);
    } finally {
      setLoading(false);
    }
  }, [workspaceSlug, portal, start, end, brand, returnMode, sort.key, sort.dir]);

  React.useEffect(() => {
    load();
  }, [load]);

  function toggleSort(key: SortKey) {
    // Block sorting by return_amount in overall mode
    if (key === "return_amount" && returnMode === "overall") return;

    setSort((p) => {
      if (p.key !== key) return { key, dir: "desc" };
      return { key, dir: p.dir === "desc" ? "asc" : "desc" };
    });
  }

  const rows = styleData?.rows ?? [];
  const showReturnAmount = returnMode === "same_month";

  // Return Amount total still comes from style table (only exists in same_month mode)
  const returnAmountTotal = React.useMemo(() => {
    if (!showReturnAmount) return 0;
    let s = 0;
    for (const r of rows) s += Number((r as any).return_amount ?? 0);
    return s;
  }, [rows, showReturnAmount]);

  // KPI values come from SUMMARY + GMV-ASP (to match dashboard)
  const kpiOrders = Number(summary?.orders ?? 0);

  const kpiReturnsTotal = Number(
    (summary as any)?.returns_total_units ?? (summary as any)?.returns ?? 0
  );
  const kpiCustomerReturns = Number((summary as any)?.return_units ?? 0);
  const kpiRto = Number((summary as any)?.rto_units ?? (summary as any)?.rto ?? 0);

  const kpiReturnPct = Number((summary as any)?.return_pct ?? 0);
  const kpiCustomerReturnPct = Number((summary as any)?.return_only_pct ?? 0);
  const kpiRtoPct = Number((summary as any)?.rto_pct ?? 0);

  return (
    <div className="space-y-4">
      <Card className="rounded-2xl">
        <CardHeader className="flex flex-row items-center justify-between gap-2">
          <CardTitle className="text-base">
  Orders • {portal === "flipkart" || rowDim === "sku" ? "SKU-wise" : "Style-wise"}
</CardTitle>


          <div className="flex flex-wrap items-center gap-2">
            {/* Return mode toggle */}
            <div className="flex items-center rounded-xl border p-1">
              <button
                type="button"
                className={[
                  "h-8 rounded-lg px-3 text-sm transition",
                  returnMode === "overall" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                ].join(" ")}
                onClick={() => setReturnMode("overall")}
              >
                Overall
              </button>
              <button
                type="button"
                className={[
                  "h-8 rounded-lg px-3 text-sm transition",
                  returnMode === "same_month" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                ].join(" ")}
                onClick={() => setReturnMode("same_month")}
              >
                Same month
              </button>
            </div>

            {/* Return type toggle */}
            <div className="flex items-center rounded-xl border p-1">
              <button
                type="button"
                className={[
                  "h-8 rounded-lg px-3 text-sm transition",
                  returnType === "all" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                ].join(" ")}
                onClick={() => setReturnType("all")}
              >
                All returns
              </button>
              <button
                type="button"
                className={[
                  "h-8 rounded-lg px-3 text-sm transition",
                  returnType === "customer" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                ].join(" ")}
                onClick={() => setReturnType("customer")}
                title="Customer-only view shows Customer + RTO separately"
              >
                Customer only
              </button>
            </div>

            <select
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              value={brand}
              onChange={(e) => setBrand(e.target.value)}
            >
              <option value="">All Brands</option>
              {brands.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </select>

            <Button variant="outline" className="rounded-xl" onClick={load} disabled={loading}>
              {loading ? "Refreshing…" : "Refresh"}
            </Button>
          </div>
        </CardHeader>

        <CardContent>
          {err ? <div className="text-sm text-red-600">{err}</div> : null}

          <div
            className={`mb-3 grid gap-3 ${
              showReturnAmount
                ? returnType === "customer"
                  ? "md:grid-cols-5"
                  : "md:grid-cols-4"
                : returnType === "customer"
                  ? "md:grid-cols-4"
                  : "md:grid-cols-3"
            }`}
          >
            <div className="rounded-xl border p-3">
              <div className="text-xs text-muted-foreground">Orders</div>
              <div className="text-lg font-semibold">{kpiOrders.toLocaleString()}</div>
            </div>

            <div className="rounded-xl border p-3">
              <div className="text-xs text-muted-foreground">GMV</div>
              <div className="text-lg font-semibold">{fmtInr(Number(gmvAsp?.gmv ?? 0))}</div>
            </div>

            {returnType === "customer" ? (
              <>
                <div className="rounded-xl border p-3">
                  <div className="text-xs text-muted-foreground">Customer Returns (units)</div>
                  <div className="text-lg font-semibold">{kpiCustomerReturns.toLocaleString()}</div>
                </div>
                <div className="rounded-xl border p-3">
                  <div className="text-xs text-muted-foreground">RTO (units)</div>
                  <div className="text-lg font-semibold">{kpiRto.toLocaleString()}</div>
                </div>
              </>
            ) : (
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground">Returns (units)</div>
                <div className="text-lg font-semibold">{kpiReturnsTotal.toLocaleString()}</div>
              </div>
            )}

            {showReturnAmount ? (
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground">Return Amount</div>
                <div className="text-lg font-semibold">{fmtInr(returnAmountTotal)}</div>
              </div>
            ) : null}
          </div>

          {/* Optional: show % row (matches dashboard) */}
          <div className="mb-4 flex flex-wrap gap-3 text-sm text-muted-foreground">
            {returnType === "customer" ? (
              <>
                <span>Customer Return %: <span className="text-foreground font-medium">{fmtPct(kpiCustomerReturnPct)}</span></span>
                <span>RTO %: <span className="text-foreground font-medium">{fmtPct(kpiRtoPct)}</span></span>
              </>
            ) : (
              <span>Return %: <span className="text-foreground font-medium">{fmtPct(kpiReturnPct)}</span></span>
            )}
            <span>ASP: <span className="text-foreground font-medium">{fmtInr(Number(gmvAsp?.asp ?? 0))}</span></span>
          </div>

          {rows.length === 0 ? (
            <div className="text-sm text-muted-foreground">No rows.</div>
          ) : (
            <div className="overflow-x-auto rounded-xl border">
              <table className="w-full text-sm">
                <thead className="bg-muted/50">
                  <tr className="text-left">
                    <th className="p-3 select-none">Brand</th>

                    <th className="p-3 text-right cursor-pointer select-none" onClick={() => toggleSort("orders")}>
                      Orders <span className="text-xs opacity-70">{sortIcon(sort.key === "orders", sort.dir)}</span>
                    </th>
                    <th className="p-3 text-right cursor-pointer select-none" onClick={() => toggleSort("gmv")}>
                      GMV <span className="text-xs opacity-70">{sortIcon(sort.key === "gmv", sort.dir)}</span>
                    </th>
                    <th className="p-3 text-right cursor-pointer select-none" onClick={() => toggleSort("asp")}>
                      ASP <span className="text-xs opacity-70">{sortIcon(sort.key === "asp", sort.dir)}</span>
                    </th>

                    {returnType === "customer" ? (
                      <>
                        <th className="p-3 text-right">Customer Returns</th>
                        <th className="p-3 text-right">RTO</th>
                      </>
                    ) : null}

                    <th className="p-3 text-right cursor-pointer select-none" onClick={() => toggleSort("returns")}>
                      Total Returns <span className="text-xs opacity-70">{sortIcon(sort.key === "returns", sort.dir)}</span>
                    </th>

                    <th className="p-3 text-right cursor-pointer select-none" onClick={() => toggleSort("return_pct")}>
                      Return % <span className="text-xs opacity-70">{sortIcon(sort.key === "return_pct", sort.dir)}</span>
                    </th>

                    {showReturnAmount ? (
                      <th
                        className="p-3 text-right cursor-pointer select-none"
                        onClick={() => toggleSort("return_amount")}
                        title="Only available in Same month mode"
                      >
                        Return Amount{" "}
                        <span className="text-xs opacity-70">{sortIcon(sort.key === "return_amount", sort.dir)}</span>
                      </th>
                    ) : null}
                  </tr>
                </thead>

                <tbody>
                  {rows.map((r) => (
                    <tr key={(r as any).seller_sku_code ?? r.style_key} className="border-t">
                      <td className="p-3">
  <div className="font-medium">{r.brand || "(Unknown)"}</div>

  {portal === "flipkart" || rowDim === "sku" ? (
    <>
      <div className="text-xs font-mono">{(r as any).seller_sku_code ?? "—"}</div>
      <div className="text-[11px] text-muted-foreground">{r.style_key || "—"}</div>
    </>
  ) : (
    <div className="text-xs text-muted-foreground">{r.style_key}</div>
  )}
</td>


                      <td className="p-3 text-right font-mono">{Number(r.orders ?? 0).toLocaleString()}</td>
                      <td className="p-3 text-right font-mono">{fmtInr(Number(r.gmv ?? 0))}</td>
                      <td className="p-3 text-right font-mono">{fmtInr(Number(r.asp ?? 0))}</td>

                      {returnType === "customer" ? (
                        <>
                          <td className="p-3 text-right font-mono">
                            {Number((r as any).return_units ?? 0).toLocaleString()}
                          </td>
                          <td className="p-3 text-right font-mono">
                            {Number((r as any).rto_units ?? 0).toLocaleString()}
                          </td>
                        </>
                      ) : null}

                      <td className="p-3 text-right font-mono">
                        {Number((r as any).returns_total_units ?? r.returns ?? 0).toLocaleString()}
                      </td>

                      <td className="p-3 text-right font-mono">{Number(r.return_pct ?? 0).toFixed(1)}%</td>

                      {showReturnAmount ? (
                        <td className="p-3 text-right font-mono">{fmtInr(Number((r as any).return_amount ?? 0))}</td>
                      ) : null}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <div className="mt-2 text-xs text-muted-foreground">
            {showReturnAmount
              ? "Same month: Return Amount = Seller Price of returned order lines."
              : "Overall: Return Amount is not shown because returns file has no amount."}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
