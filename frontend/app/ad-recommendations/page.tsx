"use client";

import * as React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";
import { getAdsRecommendations, type AdsRecoRow, getBrands } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace-context";
import WorkspaceBar from "@/components/WorkspaceBar";

function toISODate(d: Date) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function pct01ToText(v?: number | null) {
  if (v === null || v === undefined || Number.isNaN(v)) return "-";
  return `${Math.round(v * 1000) / 10}%`;
}

function downloadCsv(filename: string, rows: any[]) {
  if (!rows || rows.length === 0) return;

  const headers = Object.keys(rows[0]);
  const esc = (x: any) => {
    const s = x === null || x === undefined ? "" : String(x);
    const needs = /[",\n]/.test(s);
    return needs ? `"${s.replace(/"/g, '""')}"` : s;
  };

  const csv = [
    headers.join(","),
    ...rows.map((r) => headers.map((h) => esc((r as any)[h])).join(",")),
  ].join("\n");

  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

const TAG_ORDER: Record<string, number> = {
  "STOP (High Returns)": 0,
  SCALE: 1,
  "TRENDING PUSH": 2,
  "PUSH (New Discovery)": 3,
  "PUSH (Zero-Sale)": 4,
  WATCH: 5,
};

type SortField =
  | "tag"
  | "orders_30d"
  | "return_pct_30d"
  | "impressions"
  | "clicks"
  | "add_to_carts"
  | "purchases";

type SortDir = "asc" | "desc";

export default function AdRecommendationsPage() {
  // simple, self-contained controls (no dependency on DateRangeBar)
  const { workspaceSlug, portal, start, end, setStart, setEnd } = useWorkspace();

  // ✅ Brand filter
  const [brands, setBrands] = React.useState<string[]>([]);
  const [brandsLoading, setBrandsLoading] = React.useState(false);
  const [brand, setBrand] = React.useState<string>(""); // "" = ALL

  // thresholds (your VBA defaults)
  const [newAgeDays, setNewAgeDays] = React.useState(60);
  const [minOrders, setMinOrders] = React.useState(2);
  const [highReturnPct, setHighReturnPct] = React.useState(0.35); // 0.35 = 35%

  // data
  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);
  const [rows, setRows] = React.useState<AdsRecoRow[]>([]);
  const [snapshotAt, setSnapshotAt] = React.useState<string | null>(null);

  // filters
  const [q, setQ] = React.useState("");
  const [tag, setTag] = React.useState<string>("ALL");
  const [onlyZeroSales, setOnlyZeroSales] = React.useState(false);

  // ✅ sorting (click headers)
  const [sortField, setSortField] = React.useState<SortField>("tag");
  const [sortDir, setSortDir] = React.useState<SortDir>("asc");

  const [limit, setLimit] = React.useState<number>(100);

  const [weeklyFile, setWeeklyFile] = React.useState<File | null>(null);
  const [weeklyReplace, setWeeklyReplace] = React.useState(false);
  const [weeklyUploading, setWeeklyUploading] = React.useState(false);
  const [weeklyLastUpload, setWeeklyLastUpload] = React.useState<any>(null);
  const [inStockOnly, setInStockOnly] = React.useState(false);
  const [stockSnapshotAt, setStockSnapshotAt] = React.useState<string | null>(
    null
  );

  // ✅ load brands whenever workspace changes
  React.useEffect(() => {
    let alive = true;
    (async () => {
      setBrandsLoading(true);
      try {
        const res = await getBrands({ workspace_slug: workspaceSlug });
        const list = Array.isArray(res?.brands) ? res.brands : [];
        if (!alive) return;
        setBrands(list);
      } catch {
        if (!alive) return;
        setBrands([]);
      } finally {
        if (!alive) return;
        setBrandsLoading(false);
      }
    })();

    return () => {
      alive = false;
    };
  }, [workspaceSlug]);

  function toggleSort(nextField: SortField) {
    if (sortField === nextField) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
      return;
    }
    setSortField(nextField);

    // default direction when switching columns
    if (nextField === "tag") setSortDir("asc");
    else setSortDir("desc");
  }

  function SortMark({ field }: { field: SortField }) {
    if (sortField !== field) return null;
    return (
      <span className="text-xs text-muted-foreground">
        {sortDir === "asc" ? "▲" : "▼"}
      </span>
    );
  }

  async function uploadWeeklyReport() {
    if (!weeklyFile) {
      toast.error("Please choose a CSV file first.");
      return;
    }

    setWeeklyUploading(true);
    try {
      const fd = new FormData();
      fd.append("file", weeklyFile);

      const qs = new URLSearchParams();
      qs.set("workspace_slug", workspaceSlug);
      qs.set("replace", String(weeklyReplace));

      // use Next proxy (/api) to avoid CORS
      const res = await fetch(
        `/api/db/ingest/myntra-weekly-perf?${qs.toString()}`,
        {
          method: "POST",
          body: fd,
        }
      );

      const raw = await res.text().catch(() => "");
      let json: any = null;
      try {
        json = raw ? JSON.parse(raw) : null;
      } catch {
        json = null;
      }

      if (!res.ok) {
        const msg =
          (json && (json.detail || json.message)) ||
          raw ||
          `Upload failed: ${res.status}`;
        throw new Error(String(msg));
      }

      setWeeklyLastUpload(json);
      toast.success(`Weekly report uploaded: ${json?.inserted ?? 0} rows`);
      // refresh recommendations after upload
      await load();
    } catch (e: any) {
      toast.error(String(e?.message ?? e));
    } finally {
      setWeeklyUploading(false);
    }
  }

  async function load() {
    setLoading(true);
    setErr(null);
    try {
      const res = await getAdsRecommendations({
        start,
        end,
        workspace_slug: workspaceSlug,
     
        // ✅ FIX 1: pass portal
        portal: portal || undefined,

        // ✅ brand wired into API call
        brand: brand || undefined,
        new_age_days: newAgeDays,
        min_orders: minOrders,
        high_return_pct: highReturnPct,
        in_stock_only: stockSnapshotAt ? inStockOnly : false,
      });

      setRows(Array.isArray(res?.rows) ? res.rows : []);
      setSnapshotAt(res?.params?.latest_snapshot_at ?? null);
      setStockSnapshotAt(res?.params?.latest_stock_snapshot_at ?? null);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setRows([]);
      setSnapshotAt(null);
      setStockSnapshotAt(null);
      toast.error(String(e?.message ?? e));
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    workspaceSlug,

    // ✅ FIX 2: reload when portal changes
    portal,

    start,
    end,
    brand, // ✅ triggers reload
    newAgeDays,
    minOrders,
    highReturnPct,
    inStockOnly,
    stockSnapshotAt, // ✅ (small but correct) if snapshot changes, in_stock_only logic changes
  ]);

  const filtered = React.useMemo(() => {
    const qq = q.trim().toLowerCase();

    let out = rows;

    if (tag !== "ALL") out = out.filter((r) => (r.tag ?? "") === tag);

    if (onlyZeroSales) out = out.filter((r) => Number(r.orders_30d ?? 0) === 0);

    if (qq) {
      out = out.filter((r) => {
        const s1 = String((r as any).seller_sku_code ?? r.style_key ?? "").toLowerCase();
        const s2 = String(r.product_name ?? "").toLowerCase();
        const s3 = String(r.brand ?? "").toLowerCase();
        const s4 = String((r as any).listing_id ?? "").toLowerCase();
        return s1.includes(qq) || s2.includes(qq) || s3.includes(qq) || s4.includes(qq);

      });
    }

    const getNum = (r: AdsRecoRow, k: SortField): number => {
      if (k === "tag") return 0;
      const v = (r as any)[k];
      if (typeof v === "number") return v;
      if (v === null || v === undefined) return Number.NaN;
      const n = Number(v);
      return Number.isFinite(n) ? n : Number.NaN;
    };

    const normalize = (v: number): number => {
      if (Number.isFinite(v)) return v;
      // push null/NaN to bottom
      return sortDir === "asc"
        ? Number.POSITIVE_INFINITY
        : Number.NEGATIVE_INFINITY;
    };

    const sorter = (a: AdsRecoRow, b: AdsRecoRow): number => {
      if (sortField === "tag") {
        const oa = TAG_ORDER[a.tag] ?? 99;
        const ob = TAG_ORDER[b.tag] ?? 99;
        const primary = sortDir === "asc" ? oa - ob : ob - oa;
        if (primary !== 0) return primary;

        // tiebreak: more orders first
        const t1 = (b.orders_30d ?? 0) - (a.orders_30d ?? 0);
        if (t1 !== 0) return t1;

        return String(a.style_key ?? "").localeCompare(
          String(b.style_key ?? "")
        );
      }

      const av = normalize(getNum(a, sortField));
      const bv = normalize(getNum(b, sortField));
      const primary = sortDir === "asc" ? av - bv : bv - av;
      if (primary !== 0) return primary;

      // stable tiebreaks
      const t0 = (TAG_ORDER[a.tag] ?? 99) - (TAG_ORDER[b.tag] ?? 99);
      if (t0 !== 0) return t0;

      const t1 = (b.orders_30d ?? 0) - (a.orders_30d ?? 0);
      if (t1 !== 0) return t1;

      return String(a.style_key ?? "").localeCompare(String(b.style_key ?? ""));
    };

    out = [...out].sort(sorter);

    return out;
  }, [rows, q, tag, onlyZeroSales, sortField, sortDir]);

  const visible = React.useMemo(() => {
    if (limit <= 0) return filtered;
    return filtered.slice(0, limit);
  }, [filtered, limit]);

  const tags = React.useMemo(() => {
    const uniq = Array.from(new Set(rows.map((r) => r.tag))).filter(Boolean);
    uniq.sort((a, b) => (TAG_ORDER[a] ?? 99) - (TAG_ORDER[b] ?? 99));
    return uniq;
  }, [rows]);

  return (
    <div className="space-y-4 p-4">
      <WorkspaceBar />
      <Card>
        <CardHeader className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
          <CardTitle className="text-base">Ad Recommendations</CardTitle>
        </CardHeader>

        <CardContent className="space-y-3">
          <div className="flex flex-wrap items-center gap-2">
            {/* ✅ Brand dropdown */}
            <select
              className="h-9 w-52 rounded-xl border bg-background px-3 text-sm"
              value={brand}
              onChange={(e) => setBrand(e.target.value)}
              disabled={loading || brandsLoading}
              title="Brand filter"
            >
              <option value="">
                {brandsLoading ? "Loading brands…" : "All brands"}
              </option>
              {brands.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </select>

            <input
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              type="file"
              accept=".csv"
              onChange={(e) => setWeeklyFile(e.target.files?.[0] ?? null)}
              title="Upload Myntra weekly CSV"
            />

            <label className="flex items-center gap-2 text-sm text-muted-foreground">
              <input
                type="checkbox"
                checked={weeklyReplace}
                onChange={(e) => setWeeklyReplace(e.target.checked)}
              />
              Replace history
            </label>

            <Button
              variant="outline"
              onClick={uploadWeeklyReport}
              disabled={weeklyUploading || !weeklyFile}
            >
              {weeklyUploading ? "Uploading..." : "Upload weekly report"}
            </Button>

            <input
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              type="date"
              value={start}
              onChange={(e) => setStart(e.target.value)}
              title="start"
            />
            <input
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              type="date"
              value={end}
              onChange={(e) => setEnd(e.target.value)}
              title="end"
            />

            <input
              className="h-9 w-28 rounded-xl border bg-background px-3 text-sm"
              type="number"
              value={newAgeDays}
              onChange={(e) => setNewAgeDays(Number(e.target.value || 0))}
              title="new_age_days"
            />
            <input
              className="h-9 w-28 rounded-xl border bg-background px-3 text-sm"
              type="number"
              value={minOrders}
              onChange={(e) => setMinOrders(Number(e.target.value || 0))}
              title="min_orders"
            />
            <input
              className="h-9 w-32 rounded-xl border bg-background px-3 text-sm"
              type="number"
              step="0.01"
              value={highReturnPct}
              onChange={(e) => setHighReturnPct(Number(e.target.value || 0))}
              title="high_return_pct (0.35 = 35%)"
            />

            <input
              type="checkbox"
              checked={inStockOnly}
              onChange={(e) => setInStockOnly(e.target.checked)}
            />
            {!stockSnapshotAt ? (
              <span className="text-xs text-muted-foreground">
                Upload stock to enable
              </span>
            ) : null}
            {stockSnapshotAt ? (
              <div className="text-xs text-muted-foreground">
                Latest stock snapshot: {stockSnapshotAt}
              </div>
            ) : (
              <div className="text-xs text-muted-foreground">
                Stock snapshot: — (upload stock in Uploads page)
              </div>
            )}

            <Button onClick={load} disabled={loading}>
              {loading ? "Loading..." : "Apply"}
            </Button>

            <Button
              variant="outline"
              disabled={visible.length === 0}
              onClick={() =>
                downloadCsv(
                  // ✅ optional: include portal in filename for clarity
                  `ad_recommendations_${workspaceSlug}_${portal || "ALL"}_brand${
                    brand || "ALL"
                  }_${start}_to_${end}.csv`,
                  visible
                )
              }
            >
              Export CSV
            </Button>
          </div>

          {snapshotAt ? (
            <div className="text-xs text-muted-foreground">
              Latest weekly snapshot:{" "}
              <span className="font-mono">{snapshotAt}</span>
            </div>
          ) : (
            <div className="text-xs text-muted-foreground">
              Latest weekly snapshot: <span className="font-mono">—</span>
            </div>
          )}

          {weeklyLastUpload ? (
            <div className="text-xs text-muted-foreground">
              Last upload: inserted{" "}
              <span className="font-semibold">{weeklyLastUpload.inserted}</span>{" "}
              rows •{" "}
              <span className="font-mono">{weeklyLastUpload.ingested_at ?? ""}</span>
            </div>
          ) : null}

          <div className="flex flex-wrap items-center gap-2">
            <input
              className="h-9 w-72 rounded-xl border bg-background px-3 text-sm"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search style / product / brand..."
            />

            <select
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              value={tag}
              onChange={(e) => setTag(e.target.value)}
              title="Filter tag"
            >
              <option value="ALL">All tags</option>
              {tags.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>

            {/* Sorting is via clicking table headers */}
            <div className="text-xs text-muted-foreground">
              Sort:{" "}
              <span className="font-mono">
                {sortField} {sortDir.toUpperCase()}
              </span>{" "}
              (click headers)
            </div>

            <select
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              value={String(limit)}
              onChange={(e) => setLimit(Number(e.target.value))}
              title="Show rows"
            >
              <option value="50">Show 50</option>
              <option value="100">Show 100</option>
              <option value="200">Show 200</option>
              <option value="500">Show 500</option>
              <option value="0">Show All</option>
            </select>

            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={onlyZeroSales}
                onChange={(e) => setOnlyZeroSales(e.target.checked)}
              />
              Zero sales only
            </label>

            <div className="text-sm text-muted-foreground">
              Showing <span className="font-semibold">{visible.length}</span> of{" "}
              <span className="font-semibold">{filtered.length}</span> (total{" "}
              <span className="font-semibold">{rows.length}</span>)
            </div>
          </div>

          {err ? (
            <div className="rounded-xl border border-red-300 bg-red-50 p-3 text-sm text-red-700">
              {err}
            </div>
          ) : null}

          <div className="overflow-auto rounded-xl border">
            <table className="min-w-[1100px] w-full text-sm">
              <thead className="bg-muted/50">
                <tr className="text-left">
                  <th className="p-2">
                    <button
                      type="button"
                      className="inline-flex w-full items-center gap-1 hover:underline"
                      onClick={() => toggleSort("tag")}
                      title="Sort by Tag (ASC/DESC)"
                    >
                      Tag <SortMark field="tag" />
                    </button>
                  </th>

                  <th className="p-2">SKU</th>


                  <th className="text-right">
                    <span className="p-2 inline-block w-full">Stock</span>
                  </th>

                  <th className="p-2 text-right">
                    <button
                      type="button"
                      className="inline-flex w-full items-center justify-end gap-1 hover:underline"
                      onClick={() => toggleSort("orders_30d")}
                      title="Sort Orders30d (ASC/DESC)"
                    >
                      Orders30d <SortMark field="orders_30d" />
                    </button>
                  </th>

                  <th className="p-2 text-right">Momentum</th>

                  <th className="p-2 text-right">
                    <button
                      type="button"
                      className="inline-flex w-full items-center justify-end gap-1 hover:underline"
                      onClick={() => toggleSort("return_pct_30d")}
                      title="Sort Return% (ASC/DESC)"
                    >
                      Return% <SortMark field="return_pct_30d" />
                    </button>
                  </th>

                  <th className="p-2 text-right">RTO share</th>

                  <th className="p-2 text-right">
                    <button
                      type="button"
                      className="inline-flex w-full items-center justify-end gap-1 hover:underline"
                      onClick={() => toggleSort("impressions")}
                      title="Sort Impressions (ASC/DESC)"
                    >
                      Impr <SortMark field="impressions" />
                    </button>
                  </th>

                  <th className="p-2 text-right">
                    <button
                      type="button"
                      className="inline-flex w-full items-center justify-end gap-1 hover:underline"
                      onClick={() => toggleSort("clicks")}
                      title="Sort Clicks (ASC/DESC)"
                    >
                      Clicks <SortMark field="clicks" />
                    </button>
                  </th>

                  <th className="p-2 text-right">
                    <button
                      type="button"
                      className="inline-flex w-full items-center justify-end gap-1 hover:underline"
                      onClick={() => toggleSort("add_to_carts")}
                      title="Sort ATC (ASC/DESC)"
                    >
                      ATC <SortMark field="add_to_carts" />
                    </button>
                  </th>

                  <th className="p-2 text-right">
                    <button
                      type="button"
                      className="inline-flex w-full items-center justify-end gap-1 hover:underline"
                      onClick={() => toggleSort("purchases")}
                      title="Sort Purchases (ASC/DESC)"
                    >
                      Purchases <SortMark field="purchases" />
                    </button>
                  </th>

                  <th className="p-2">Why</th>
                </tr>
              </thead>

              <tbody>
                {visible.map((r, idx) => (
                  <tr key={`${r.style_key}-${idx}`} className="border-t">
                    <td className="p-2 font-medium">
                      {(() => {
                        const qty =
                          typeof r.style_total_qty === "number"
                            ? r.style_total_qty
                            : null;

                        const ageDays =
                          typeof r.age_days === "number" ? r.age_days : null;
                        const orders30d =
                          typeof r.orders_30d === "number"
                            ? r.orders_30d
                            : r.orders_30d ?? 0;

                        const noStock = qty !== null && qty < 3;
                        const lowStock = qty !== null && qty < 10;

                        const shouldReplenish =
                          noStock &&
                          ageDays !== null &&
                          ageDays <= newAgeDays &&
                          (orders30d ?? 0) > 0;

                        if (shouldReplenish) return "REPLENISH & SCALE";
                        if (noStock) return "NO STOCK (STOP ADS)";
                        if (lowStock) return "LOW STOCK";
                        return r.tag ?? "-";
                      })()}
                    </td>

                    <td className="p-2">
  <div className="font-medium">{r.style_key}</div>
  {r.listing_id ? (
    <div className="text-xs text-muted-foreground">Listing: {r.listing_id}</div>
  ) : null}
</td>



                    <td
                      className={[
                        "text-right font-mono",
                        typeof r.style_total_qty === "number" &&
                        r.style_total_qty > 0
                          ? "font-semibold"
                          : "text-muted-foreground",
                      ].join(" ")}
                    >
                      {typeof r.style_total_qty === "number"
                        ? r.style_total_qty
                        : "—"}
                    </td>

                    <td className="p-2 text-right">{r.orders_30d ?? 0}</td>
                    <td className="p-2 text-right">
                      {r.momentum == null
                        ? "-"
                        : `${Math.round(r.momentum * 1000) / 10}%`}
                    </td>
                    <td className="p-2 text-right">
                      {pct01ToText(r.return_pct_30d)}
                    </td>
                    <td className="p-2 text-right">
                      {pct01ToText(r.rto_share_30d)}
                    </td>
                    <td className="p-2 text-right">{r.impressions ?? 0}</td>
                    <td className="p-2 text-right">{r.clicks ?? 0}</td>
                    <td className="p-2 text-right">{r.add_to_carts ?? 0}</td>
                    <td className="p-2 text-right">{r.purchases ?? 0}</td>
                    <td className="p-2">{r.why}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
