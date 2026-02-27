"use client";

import * as React from "react";
import { AppShell } from "@/components/app-shell";
import WorkspaceBar from "@/components/WorkspaceBar";
import { useWorkspace } from "@/lib/workspace-context";
import { apiGet } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type Level = "brand" | "style" | "sku";
type Portal = "myntra" | "flipkart";

type BrandsResp = {
  workspace_slug: string;
  count: number;
  brands: string[];
};

type AspOptimizerBand = {
  from: number;
  to: number;
  mid: number;
};

type AspOptimizerBucket = {
  band: { from: number; to: number; mid: number };
  sold_units: number;
  gmv: number;
  days: number;
  avg_units_per_day: number;
  returns_total: number;
  returns_rto: number;
  returns_customer: number;
  returns_pct: number;
  net_units: number;
  avg_net_units_per_day: number;
};

type AspOptimizerRow = {
  key: string;
  current_asp: number;
  days_active: number;
  units: number;
  confidence: "high" | "medium" | "low";
  current_avg_units_per_day: number;

  best_volume_band: AspOptimizerBucket | null;
  best_net_band: AspOptimizerBucket | null;

  lift_units_pct: number | null;
};

type AspOptimizerTimeseriesPoint = {
  date: string; // YYYY-MM-DD
  units: number;
  gmv: number;
  asp: number | null;
  returns_units: number;
};

type AspOptimizerResp = {
  portal: string | null;
  level: Level;
  start: string;
  end: string;
  bucket_size: number;
  rows: AspOptimizerRow[];
  deep_dive: null | {
    timeseries: AspOptimizerTimeseriesPoint[];
    bands: AspOptimizerBucket[];
  };
  note?: string;
};

function fmtDateInput(d: Date) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function fmtNum(n: any) {
  const x = Number(n ?? 0);
  if (!Number.isFinite(x)) return "0";
  return (Math.round(x * 100) / 100).toFixed(2).replace(/\.00$/, "");
}

function fmtInt(n: any) {
  const x = Number(n ?? 0);
  if (!Number.isFinite(x)) return "0";
  return String(Math.round(x));
}

function bandLabel(b: any) {
  if (!b) return "—";
  const from = Number(b.from ?? 0);
  const to = Number(b.to ?? 0);
  if (!Number.isFinite(from) || !Number.isFinite(to)) return "—";
  return `₹${from}–₹${to}`;
}

function levelLabel(level: Level, portal: Portal) {
  if (level === "brand") return "Brand";
  if (portal === "flipkart") return "Seller SKU (Style+SKU)";
  return level === "sku" ? "SKU" : "Style";
}

function keyPlaceholder(level: Level, portal: Portal) {
  if (level === "brand") return "Enter Brand name";
  if (portal === "flipkart") return "Enter seller_sku (Flipkart: seller_sku is both Style & SKU)";
  return level === "sku" ? "Enter SKU (seller_sku_code)" : "Enter Style Key";
}

type SortKey =
  | "lift_units_pct"
  | "best_units_per_day"
  | "current_asp"
  | "returns_pct_best"
  | "net_units_per_day_best"
  | "units"
  | "days_active"
  | "confidence";

type SortDir = "desc" | "asc";

function confRank(c: any) {
  const v = String(c ?? "").toLowerCase();
  if (v === "high") return 3;
  if (v === "medium") return 2;
  if (v === "low") return 1;
  return 0;
}

export default function AspOptimizerPage() {
  const { workspaceSlug } = useWorkspace();

  const [portal, setPortal] = React.useState<Portal>("myntra");
  const [level, setLevel] = React.useState<Level>("style");

  const [start, setStart] = React.useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return fmtDateInput(d);
  });
  const [end, setEnd] = React.useState(() => fmtDateInput(new Date()));

  // brand dropdown (like dashboard)
  const [brands, setBrands] = React.useState<string[]>([]);
  const [brandsLoading, setBrandsLoading] = React.useState(false);
  const [brandsError, setBrandsError] = React.useState<string | null>(null);
  const [brand, setBrand] = React.useState<string>(""); // "" = All brands

  const [bucketSize, setBucketSize] = React.useState<number>(50);
  const [topN, setTopN] = React.useState<number>(50);
  const [minDays, setMinDays] = React.useState<number>(7);
  const [minUnits, setMinUnits] = React.useState<number>(10);

  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [rows, setRows] = React.useState<AspOptimizerRow[]>([]);
  const [search, setSearch] = React.useState("");

  // Sorting controls
  const [sortKey, setSortKey] = React.useState<SortKey>("lift_units_pct");
  const [sortDir, setSortDir] = React.useState<SortDir>("desc");

  // Drawer state
  const [drawerOpen, setDrawerOpen] = React.useState(false);
  const [selectedKey, setSelectedKey] = React.useState<string>("");
  const [deepLoading, setDeepLoading] = React.useState(false);
  const [deepError, setDeepError] = React.useState<string | null>(null);
  const [deep, setDeep] = React.useState<AspOptimizerResp | null>(null);

  // ✅ Flipkart rule: only SKU-wise
  React.useEffect(() => {
    if (portal === "flipkart" && level === "style") setLevel("sku");
    // brand-level is allowed (it will work only if catalog mapping exists; safe to keep)
  }, [portal, level]);

  // ✅ Load brands when workspace changes (same behavior as dashboard)
  React.useEffect(() => {
    if (!workspaceSlug) return;
    setBrandsLoading(true);
    setBrandsError(null);
    setBrands([]);
    setBrand(""); // reset to All brands on workspace change

    apiGet<BrandsResp>("/db/brands", { workspace_slug: workspaceSlug })
      .then((r) => setBrands(Array.isArray(r?.brands) ? r.brands : []))
      .catch((e2: any) => setBrandsError(String(e2?.message ?? e2)))
      .finally(() => setBrandsLoading(false));
  }, [workspaceSlug]);

  async function loadList() {
    if (!workspaceSlug) return;

    setLoading(true);
    setError(null);
    setRows([]);
    setDeep(null);
    setSelectedKey("");
    setDrawerOpen(false);

    try {
      const resp = await apiGet<AspOptimizerResp>("/db/kpi/asp-optimizer", {
        workspace_slug: workspaceSlug ?? "default",
        start,
        end,
        portal,
        level,
        brand: brand || undefined,
        bucket_size: bucketSize,
        top_n: topN,
        min_days: minDays,
        min_units: minUnits,
      });
      setRows(resp.rows ?? []);
    } catch (e: any) {
      setError(e?.message ?? "Failed to load ASP Optimizer");
    } finally {
      setLoading(false);
    }
  }

  async function loadDeepDive(k: string) {
    const kk = (k ?? "").trim();
    if (!kk) return;
    if (!workspaceSlug) return;

    setDeepLoading(true);
    setDeepError(null);
    setDeep(null);

    try {
      const resp = await apiGet<AspOptimizerResp>("/db/kpi/asp-optimizer", {
        workspace_slug: workspaceSlug ?? "default",
        start,
        end,
        portal,
        level,
        key: kk,
        brand: brand || undefined,
        bucket_size: bucketSize,
        top_n: topN,
        min_days: minDays,
        min_units: minUnits,
      });
      setDeep(resp);
    } catch (e: any) {
      setDeepError(e?.message ?? "Failed to load deep dive");
    } finally {
      setDeepLoading(false);
    }
  }

  function openDrawerWithKey(k: string) {
    const kk = String(k ?? "").trim();
    if (!kk) return;
    setSelectedKey(kk);
    setDrawerOpen(true);
    loadDeepDive(kk);
  }

  function closeDrawer() {
    setDrawerOpen(false);
    setSelectedKey("");
    setDeep(null);
    setDeepError(null);
    setDeepLoading(false);
  }

  const filteredRows = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return rows;
    return (rows ?? []).filter((r) => String(r.key ?? "").toLowerCase().includes(q));
  }, [rows, search]);

  const sortedRows = React.useMemo(() => {
    const arr = [...(filteredRows ?? [])];

    function value(r: AspOptimizerRow): number {
      const bestVol = r.best_volume_band;
      const bestNet = r.best_net_band;

      switch (sortKey) {
        case "lift_units_pct":
          return Number(r.lift_units_pct ?? -1e18);
        case "best_units_per_day":
          return Number(bestVol?.avg_units_per_day ?? -1e18);
        case "net_units_per_day_best":
          return Number(bestNet?.avg_net_units_per_day ?? -1e18);
        case "current_asp":
          return Number(r.current_asp ?? -1e18);
        case "returns_pct_best":
          return Number(bestVol?.returns_pct ?? -1e18);
        case "units":
          return Number(r.units ?? -1e18);
        case "days_active":
          return Number(r.days_active ?? -1e18);
        case "confidence":
          return confRank(r.confidence);
        default:
          return 0;
      }
    }

    arr.sort((a, b) => {
      const va = value(a);
      const vb = value(b);
      if (sortDir === "asc") return va - vb;
      return vb - va;
    });

    return arr;
  }, [filteredRows, sortKey, sortDir]);

  const selectedSummary = React.useMemo(() => {
    if (!selectedKey) return null;
    return (rows ?? []).find((r) => String(r.key) === String(selectedKey)) ?? null;
  }, [rows, selectedKey]);

  React.useEffect(() => {
    if (!workspaceSlug) return;
    loadList();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceSlug]);

  return (
    <AppShell>
      <WorkspaceBar />

      <div className="mx-auto max-w-7xl p-6 space-y-6">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div>
            <div className="text-2xl font-semibold">ASP Optimizer</div>
            <div className="text-sm text-muted-foreground">
              Find ASP bands that drive higher orders (and compare returns impact).
              {portal === "flipkart" ? (
                <span className="ml-2">
                  Note: Flipkart uses <span className="font-mono">seller_sku</span> as both Style & SKU.
                </span>
              ) : null}
            </div>
          </div>
          <Button onClick={loadList} disabled={loading}>
            {loading ? "Loading…" : "Reload"}
          </Button>
        </div>

        {/* Filters */}
        <Card>
          <CardHeader>
            <CardTitle>Filters</CardTitle>
          </CardHeader>

          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-12 gap-3">
              {/* Portal */}
              <div className="md:col-span-3">
                <label className="text-sm text-muted-foreground">Portal</label>
                <div className="flex items-center rounded-xl border p-1">
                  <button
                    type="button"
                    className={[
                      "h-9 rounded-lg px-3 text-sm transition",
                      portal === "myntra" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                    ].join(" ")}
                    onClick={() => setPortal("myntra")}
                  >
                    Myntra
                  </button>
                  <button
                    type="button"
                    className={[
                      "h-9 rounded-lg px-3 text-sm transition",
                      portal === "flipkart" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                    ].join(" ")}
                    onClick={() => setPortal("flipkart")}
                  >
                    Flipkart
                  </button>
                </div>
              </div>

              {/* Level */}
              <div className="md:col-span-3">
                <label className="text-sm text-muted-foreground">Level</label>
                <div className="flex items-center rounded-xl border p-1">
                  <button
                    type="button"
                    className={[
                      "h-9 rounded-lg px-3 text-sm transition",
                      level === "brand" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                    ].join(" ")}
                    onClick={() => setLevel("brand")}
                  >
                    Brand
                  </button>

                  <button
                    type="button"
                    className={[
                      "h-9 rounded-lg px-3 text-sm transition",
                      level === "style" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                      portal === "flipkart" ? "opacity-40 cursor-not-allowed" : "",
                    ].join(" ")}
                    onClick={() => {
                      if (portal === "flipkart") return;
                      setLevel("style");
                    }}
                    title={portal === "flipkart" ? "Flipkart has no style_id. Use SKU-wise." : "Style-wise"}
                  >
                    Style
                  </button>

                  <button
                    type="button"
                    className={[
                      "h-9 rounded-lg px-3 text-sm transition",
                      level === "sku" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                    ].join(" ")}
                    onClick={() => setLevel("sku")}
                  >
                    SKU
                  </button>
                </div>
              </div>

              {/* Start */}
              <div className="md:col-span-2">
                <label className="text-sm text-muted-foreground">Start</label>
                <Input type="date" value={start} onChange={(e) => setStart(e.target.value)} />
              </div>

              {/* End */}
              <div className="md:col-span-2">
                <label className="text-sm text-muted-foreground">End</label>
                <Input type="date" value={end} onChange={(e) => setEnd(e.target.value)} />
              </div>

              {/* Brand */}
              <div className="md:col-span-2">
                <label className="text-sm text-muted-foreground">Brand</label>
                <select
                  value={brand}
                  onChange={(e) => setBrand(e.target.value)}
                  className="h-10 w-full rounded-xl border bg-background px-3 text-sm shadow-sm"
                  disabled={loading || brandsLoading}
                  title={brandsError ? `Brand load failed: ${brandsError}` : "Brand filter"}
                >
                  <option value="">{brandsLoading ? "Loading brands…" : "All brands"}</option>
                  {brands.map((b) => (
                    <option key={b} value={b}>
                      {b}
                    </option>
                  ))}
                </select>
                {brandsError ? <div className="text-xs text-red-600 mt-1">{brandsError}</div> : null}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-6 gap-3">
              <div>
                <label className="text-sm text-muted-foreground">Bucket (₹)</label>
                <Input
                  type="number"
                  value={bucketSize}
                  min={1}
                  max={1000}
                  onChange={(e) => setBucketSize(Number(e.target.value || 50))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Top N</label>
                <Input
                  type="number"
                  value={topN}
                  min={1}
                  max={500}
                  onChange={(e) => setTopN(Number(e.target.value || 50))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Min Days</label>
                <Input
                  type="number"
                  value={minDays}
                  min={1}
                  max={365}
                  onChange={(e) => setMinDays(Number(e.target.value || 7))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Min Units</label>
                <Input type="number" value={minUnits} min={0} onChange={(e) => setMinUnits(Number(e.target.value || 10))} />
              </div>

              <div className="md:col-span-2 flex items-end gap-3">
                <Button onClick={loadList} disabled={loading}>
                  {loading ? "Loading…" : "Load"}
                </Button>
                <div className="text-xs text-muted-foreground">Tip: 30–90 days = stronger confidence.</div>
              </div>
            </div>

            {error ? <div className="text-sm text-red-600">{error}</div> : null}
          </CardContent>
        </Card>

        {/* Opportunities */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle>Opportunities</CardTitle>
            <div className="text-xs text-muted-foreground">Click any row to open the right panel (deep dive).</div>
          </CardHeader>

          <CardContent className="space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder={`Search ${levelLabel(level, portal)}…`} />

              {/* ✅ Sorting controls */}
              <select
                value={sortKey}
                onChange={(e) => setSortKey(e.target.value as SortKey)}
                className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm"
                title="Sort column"
              >
                <option value="lift_units_pct">Sort: Lift %</option>
                <option value="best_units_per_day">Sort: Best Units/Day</option>
                <option value="net_units_per_day_best">Sort: Best Net Units/Day</option>
                <option value="current_asp">Sort: Current ASP</option>
                <option value="returns_pct_best">Sort: Returns % (Best band)</option>
                <option value="units">Sort: Units</option>
                <option value="days_active">Sort: Days active</option>
                <option value="confidence">Sort: Confidence</option>
              </select>

              <select
                value={sortDir}
                onChange={(e) => setSortDir(e.target.value as SortDir)}
                className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm"
                title="Sort direction"
              >
                <option value="desc">Desc</option>
                <option value="asc">Asc</option>
              </select>
            </div>

            <div className="rounded-xl border overflow-auto" style={{ maxHeight: 520 }}>
              <table className="w-full text-sm">
                <thead className="bg-muted/50">
                  <tr className="text-left">
                    <th className="p-2">{levelLabel(level, portal)}</th>
                    <th className="p-2 text-right">Current ASP</th>
                    <th className="p-2 text-right">Avg Units/Day</th>
                    <th className="p-2 text-right">Best Band (Units)</th>
                    <th className="p-2 text-right">Lift %</th>
                    <th className="p-2 text-right">Returns %</th>
                    <th className="p-2 text-right">Best Band (Net)</th>
                    <th className="p-2 text-right">Conf.</th>
                  </tr>
                </thead>
                <tbody>
                  {loading ? (
                    <tr>
                      <td className="p-3 text-sm text-muted-foreground" colSpan={8}>
                        Loading…
                      </td>
                    </tr>
                  ) : sortedRows?.length ? (
                    sortedRows.map((r, i) => {
                      const bestVol = r.best_volume_band;
                      const bestNet = r.best_net_band;
                      const retPct = bestVol ? bestVol.returns_pct : 0;

                      return (
                        <tr
                          key={`${r.key}-${i}`}
                          className="border-t cursor-pointer hover:bg-muted/30"
                          onClick={() => openDrawerWithKey(String(r.key))}
                        >
                          <td className="p-2 font-mono text-xs">{r.key}</td>
                          <td className="p-2 text-right">{fmtNum(r.current_asp)}</td>
                          <td className="p-2 text-right">{fmtNum(r.current_avg_units_per_day)}</td>
                          <td className="p-2 text-right font-mono">{bandLabel(bestVol?.band)}</td>
                          <td className="p-2 text-right font-mono">
                            {r.lift_units_pct === null || r.lift_units_pct === undefined ? "—" : `${fmtNum(r.lift_units_pct)}%`}
                          </td>
                          <td className="p-2 text-right font-mono">{fmtNum(retPct)}%</td>
                          <td className="p-2 text-right font-mono">{bandLabel(bestNet?.band)}</td>
                          <td className="p-2 text-right font-mono text-xs">{r.confidence}</td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td className="p-3 text-sm text-muted-foreground" colSpan={8}>
                        No rows.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            <div className="text-xs text-muted-foreground">
              “Best Band (Units)” = highest avg units/day. “Best Band (Net)” = highest avg (sold - returns)/day.
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Right Drawer */}
      {drawerOpen ? (
        <div className="fixed inset-0 z-50">
          <div className="absolute inset-0 bg-black/30" onClick={closeDrawer} />
          <div className="absolute right-0 top-0 h-full w-full max-w-md bg-background shadow-2xl overflow-y-auto">
            <div className="flex items-center justify-between border-b p-4">
              <div>
                <div className="text-sm text-muted-foreground">{levelLabel(level, portal)}</div>
                <div className="font-mono text-lg">{selectedKey || "—"}</div>
                <div className="text-xs text-muted-foreground mt-1">
                  Portal: {portal} • Level: {level}
                  {portal === "flipkart" ? <span className="ml-2">• Flipkart: seller_sku is both Style+SKU</span> : null}
                </div>
              </div>
              <Button variant="outline" size="sm" onClick={closeDrawer}>
                Close
              </Button>
            </div>

            <div className="p-4 space-y-3" style={{ maxHeight: "calc(100vh - 73px)" }}>
              {/* Manual key reload */}
              <div className="grid grid-cols-1 gap-2">
                <label className="text-sm text-muted-foreground">Key</label>
                <div className="flex gap-2">
                  <Input value={selectedKey} onChange={(e) => setSelectedKey(e.target.value)} placeholder={keyPlaceholder(level, portal)} />
                  <Button onClick={() => loadDeepDive(selectedKey)} disabled={deepLoading || !selectedKey.trim()}>
                    {deepLoading ? "…" : "Load"}
                  </Button>
                </div>
              </div>

              {deepError ? <div className="text-sm text-red-600">{deepError}</div> : null}

              {/* Summary cards */}
              {selectedSummary ? (
                <div className="grid grid-cols-2 gap-2">
                  <div className="rounded-xl border p-3">
                    <div className="text-xs text-muted-foreground">Current ASP</div>
                    <div className="text-lg font-semibold">{fmtNum(selectedSummary.current_asp)}</div>
                    <div className="text-xs text-muted-foreground mt-1">
                      Days: {fmtInt(selectedSummary.days_active)} • Units: {fmtInt(selectedSummary.units)}
                    </div>
                  </div>

                  <div className="rounded-xl border p-3">
                    <div className="text-xs text-muted-foreground">Avg Units/Day</div>
                    <div className="text-lg font-semibold">{fmtNum(selectedSummary.current_avg_units_per_day)}</div>
                    <div className="text-xs text-muted-foreground mt-1">
                      Conf: <span className="font-mono">{selectedSummary.confidence}</span>
                    </div>
                  </div>

                  <div className="rounded-xl border p-3">
                    <div className="text-xs text-muted-foreground">Best Band (Units)</div>
                    <div className="text-lg font-semibold font-mono">{bandLabel(selectedSummary.best_volume_band?.band)}</div>
                    <div className="text-xs text-muted-foreground mt-1">
                      Lift:{" "}
                      {selectedSummary.lift_units_pct === null || selectedSummary.lift_units_pct === undefined ? "—" : `${fmtNum(selectedSummary.lift_units_pct)}%`}
                    </div>
                  </div>

                  <div className="rounded-xl border p-3">
                    <div className="text-xs text-muted-foreground">Best Band (Net)</div>
                    <div className="text-lg font-semibold font-mono">{bandLabel(selectedSummary.best_net_band?.band)}</div>
                    <div className="text-xs text-muted-foreground mt-1">
                      Returns% (best):{" "}
                      <span className="font-mono">{selectedSummary.best_volume_band ? `${fmtNum(selectedSummary.best_volume_band.returns_pct)}%` : "—"}</span>
                    </div>
                  </div>
                </div>
              ) : null}

              {/* ✅ Price Bands */}
              <div className="rounded-xl border p-3">
                <div className="text-sm font-medium">Price Bands</div>
                <div className="text-xs text-muted-foreground mb-2">Compare Units/Day vs Returns% to pick the best band.</div>

                {deepLoading ? (
                  <div className="text-sm text-muted-foreground">Loading…</div>
                ) : deep?.deep_dive?.bands?.length ? (
                  <div className="rounded-xl border overflow-auto" style={{ maxHeight: 280 }}>
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th className="p-2">Band</th>
                          <th className="p-2 text-right">Sold</th>
                          <th className="p-2 text-right">Days</th>
                          <th className="p-2 text-right">Units/Day</th>
                          <th className="p-2 text-right">Ret%</th>
                          <th className="p-2 text-right">Net</th>
                          <th className="p-2 text-right">Net/Day</th>
                        </tr>
                      </thead>
                      <tbody>
                        {deep.deep_dive.bands.map((b, i) => (
                          <tr key={`${b.band?.from ?? i}-${i}`} className="border-t">
                            <td className="p-2 font-mono">{bandLabel(b.band)}</td>
                            <td className="p-2 text-right">{fmtInt(b.sold_units)}</td>
                            <td className="p-2 text-right">{fmtInt(b.days)}</td>
                            <td className="p-2 text-right font-mono">{fmtNum(b.avg_units_per_day)}</td>
                            <td className="p-2 text-right font-mono">{fmtNum(b.returns_pct)}%</td>
                            <td className="p-2 text-right">{fmtInt(b.net_units)}</td>
                            <td className="p-2 text-right font-mono">{fmtNum(b.avg_net_units_per_day)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No bands available.</div>
                )}
              </div>

              {/* Timeseries */}
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground mb-2">Day-wise Performance</div>

                {deepLoading ? (
                  <div className="text-sm text-muted-foreground">Loading…</div>
                ) : deep?.deep_dive?.timeseries?.length ? (
                  <div className="rounded-xl border overflow-auto" style={{ maxHeight: 420 }}>
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th className="p-2">Date</th>
                          <th className="p-2 text-right">Units</th>
                          <th className="p-2 text-right">ASP</th>
                          <th className="p-2 text-right">GMV</th>
                          <th className="p-2 text-right">Returns</th>
                        </tr>
                      </thead>
                      <tbody>
                        {deep.deep_dive.timeseries.map((t, i) => (
                          <tr key={`${t.date}-${i}`} className="border-t">
                            <td className="p-2 font-mono text-xs">{t.date}</td>
                            <td className="p-2 text-right">{fmtInt(t.units)}</td>
                            <td className="p-2 text-right">{t.asp === null ? "—" : fmtNum(t.asp)}</td>
                            <td className="p-2 text-right">{fmtNum(t.gmv)}</td>
                            <td className="p-2 text-right">{fmtInt(t.returns_units)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No deep dive data yet.</div>
                )}

                {deep?.note ? <div className="text-xs text-muted-foreground mt-2">{deep.note}</div> : null}
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </AppShell>
  );
}
