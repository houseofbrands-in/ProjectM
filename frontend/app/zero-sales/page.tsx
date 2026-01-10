// frontend/app/zero-sales/page.tsx
"use client";

import * as React from "react";
import { AppShell } from "@/components/app-shell";
import WorkspaceBar from "@/components/WorkspaceBar";
import { useWorkspace } from "@/lib/workspace-context";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  getZeroSalesSinceLive,
  type ZeroSalesRow,
  getBrands,
} from "@/lib/api";

type SortDir = "desc" | "asc";

function downloadCSV(filename: string, rows: any[]) {
  const headers = [
    "brand",
    "product_name",
    "style_key",
    "seller_sku_code",
    "live_date",
    "days_live",
    "orders",
  ];

  const esc = (v: any) => {
    const s = String(v ?? "");
    const n = s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
    if (n.includes(",") || n.includes('"') || n.includes("\n"))
      return `"${n.replaceAll('"', '""')}"`;
    return n;
  };

  const lines = [
    headers.join(","),
    ...rows.map((r) => headers.map((h) => esc(r[h])).join(",")),
  ];

  const blob = new Blob(["\uFEFF" + lines.join("\n")], {
    type: "text/csv;charset=utf-8",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function TableSkeleton({ rows = 10 }: { rows?: number }) {
  return (
    <>
      {Array.from({ length: rows }).map((_, i) => (
        <tr key={`sk-${i}`} className="border-t">
          {Array.from({ length: 6 }).map((__, j) => (
            <td key={`sk-${i}-${j}`} className="p-3">
              <div className="h-4 w-full animate-pulse rounded-md bg-muted" />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

export default function ZeroSalesPage() {
  const { workspaceSlug } = useWorkspace();

  const [minDays, setMinDays] = React.useState(7);
  const [topN, setTopN] = React.useState(200);

  const [brands, setBrands] = React.useState<string[]>([]);
  const [brand, setBrand] = React.useState<string>(""); // "" = all

  const [query, setQuery] = React.useState("");
  const [sortDir, setSortDir] = React.useState<SortDir>("desc");

  const [lastRefreshed, setLastRefreshed] = React.useState<Date | null>(null);

  const [rows, setRows] = React.useState<ZeroSalesRow[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);

  async function load() {
    setLoading(true);
    setErr(null);

    try {
      const data = await getZeroSalesSinceLive({
        min_days_live: minDays,
        top_n: topN,
        workspace_slug: workspaceSlug,
        brand: brand || undefined,
        sort_dir: sortDir,
      });

      setRows(data);
      setLastRefreshed(new Date());
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  // load brands when workspace changes
  React.useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const res = await getBrands({ workspace_slug: workspaceSlug });
        const list = Array.isArray(res?.brands) ? res.brands : [];
        if (!alive) return;
        setBrands(list);
      } catch {
        if (!alive) return;
        setBrands([]);
      }
    })();
    return () => {
      alive = false;
    };
  }, [workspaceSlug]);

  // initial load + reload when workspace changes
  React.useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceSlug]);

  const filtered = React.useMemo(() => {
    if (!query.trim()) return rows;
    const q = query.toLowerCase();
    return rows.filter((r) => {
      return (
        (r.brand ?? "").toLowerCase().includes(q) ||
        (r.product_name ?? "").toLowerCase().includes(q) ||
        (r.style_key ?? "").toLowerCase().includes(q) ||
        (r.seller_sku_code ?? "").toLowerCase().includes(q)
      );
    });
  }, [rows, query]);

  function toggleDaysLiveSort() {
    const next: SortDir = sortDir === "desc" ? "asc" : "desc";
    setSortDir(next);

    // reload from backend so sorting applies to FULL dataset, not just current rows
    setTimeout(() => {
      load();
    }, 0);
  }

  function onChangeBrand(nextBrand: string) {
    setBrand(nextBrand);
    setTimeout(() => load(), 0);
  }

  return (
    <AppShell>
      <div className="space-y-4">
        <WorkspaceBar />

        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="text-lg font-semibold">Zero Sales Since Live</div>
            <div className="text-sm text-muted-foreground">
              Styles live for N+ days with zero orders (based on catalog live date)
            </div>

            {/* ✅ UPDATED: show brand in header line */}
            <div className="text-xs text-muted-foreground mt-1">
              Workspace: <span className="font-mono">{workspaceSlug}</span> • Brand:{" "}
              <span className="font-mono">{brand ? brand : "All"}</span> • Last
              refreshed:{" "}
              {lastRefreshed ? ` ${lastRefreshed.toLocaleString()}` : " —"}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <select
              className="h-10 rounded-xl border bg-background px-3 text-sm"
              value={brand}
              onChange={(e) => onChangeBrand(e.target.value)}
              disabled={loading}
              title="Brand filter"
            >
              <option value="">All brands</option>
              {brands.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </select>

            <Input
              placeholder="Search brand / product / style / sku…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="max-w-sm rounded-xl"
            />

            <Input
              type="number"
              min={0}
              value={minDays}
              onChange={(e) => setMinDays(Number(e.target.value || 0))}
              className="w-36 rounded-xl"
              placeholder="Min days"
            />

            <Input
              type="number"
              min={1}
              value={topN}
              onChange={(e) => setTopN(Number(e.target.value || 1))}
              className="w-32 rounded-xl"
              placeholder="Top N"
            />

            <Button
              variant="outline"
              className="rounded-xl"
              onClick={load}
              disabled={loading}
            >
              {loading ? "Refreshing…" : "Refresh"}
            </Button>

            <Button
              className="rounded-xl"
              disabled={loading || filtered.length === 0}
              onClick={() =>
                downloadCSV(
                  `zero-sales_${workspaceSlug}_brand${brand || "ALL"}_min${minDays}_top${topN}_dayslive_${sortDir}.csv`,
                  filtered.map((r) => ({
                    brand: r.brand ?? "",
                    product_name: r.product_name ?? "",
                    style_key: r.style_key ?? "",
                    seller_sku_code: r.seller_sku_code ?? "",
                    live_date: r.live_date ?? "",
                    days_live: r.days_live ?? 0,
                    orders: r.orders ?? 0,
                  }))
                )
              }
            >
              Export CSV
            </Button>
          </div>
        </div>

        {err ? <div className="text-sm text-red-600">Error: {err}</div> : null}

        <Card className="rounded-2xl">
          <CardHeader>
            <CardTitle className="text-base">Styles</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-auto rounded-xl border">
              <table className="w-full text-sm">
                <thead className="bg-muted/50">
                  <tr className="text-left">
                    <th className="p-3">Brand</th>
                    <th className="p-3">Product</th>
                    <th className="p-3">Style</th>
                    <th className="p-3">Live Date</th>

                    <th className="p-3 text-right">
                      <button
                        type="button"
                        onClick={toggleDaysLiveSort}
                        className="inline-flex items-center gap-1 font-semibold hover:underline"
                        title="Sort by Days Live (server-side)"
                        disabled={loading}
                      >
                        Days Live{" "}
                        <span className="font-mono">
                          {sortDir === "desc" ? "↓" : "↑"}
                        </span>
                      </button>
                    </th>

                    <th className="p-3 text-right">Orders</th>
                  </tr>
                </thead>

                <tbody>
                  {loading ? (
                    <TableSkeleton rows={10} />
                  ) : filtered.length ? (
                    filtered.map((r, i) => (
                      <tr
                        key={`${r.style_key}-${r.live_date}-${i}`}
                        className="border-t"
                      >
                        <td className="p-3">{r.brand ?? ""}</td>
                        <td className="p-3">{r.product_name ?? ""}</td>
                        <td className="p-3 font-mono text-xs">{r.style_key}</td>
                        <td className="p-3">{r.live_date}</td>
                        <td className="p-3 text-right">{r.days_live}</td>
                        <td className="p-3 text-right">{r.orders}</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td className="p-3 text-muted-foreground" colSpan={6}>
                        No results (try lowering min_days_live or increasing top_n).
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <div className="mt-2 text-xs text-muted-foreground">
              Sorting is server-side. It re-fetches data so it applies to the full dataset.
            </div>
          </CardContent>
        </Card>
      </div>
    </AppShell>
  );
}
