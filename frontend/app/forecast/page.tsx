"use client";

import * as React from "react";
import { AppShell } from "@/components/app-shell";
import WorkspaceBar from "@/components/WorkspaceBar";
import { useWorkspace } from "@/lib/workspace-context";
import { apiGet } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";


type ForecastMode = "size" | "sku";

type ForecastInputs = {
  forecast_days: number;
  sales_days: number;
  spike_multiplier: number;
  lead_time_days: number;
  target_cover_days: number;
  safety_stock_pct: number;
  exclude_rto: boolean;
};

type ForecastTotals = {
  orders_gross?: number;
  rto_units_subtracted?: number;
  orders_net?: number;
  stock_qty?: number;

  avg_daily?: number;
  forecast_units?: number;
  required_on_hand?: number;
  gap_qty?: number;
};

type SizeForecastRow = {
  size: string | null;
  orders: number;
  share_orders: number; // percent (0..100)
  stock_qty: number;
  avg_daily_orders: number;
  days_cover: number | null;
  risk: string | null;

  required_qty?: number;
  gap_qty?: number;
};

type SkuForecastRow = {
  sku: string | null;
  orders: number;
  share_orders: number; // percent (0..100)
  stock_qty: number;
  avg_daily_orders: number;
  days_cover: number | null;
  risk: string | null;

  required_qty?: number;
  gap_qty?: number;
};

type BaseForecastResp = {
  workspace_slug: string;
  style_key: string;
  window: {
    start: string;
    end: string;
    days: number;
  };
  latest_stock_snapshot_at?: string | null;
  inputs?: Partial<ForecastInputs>;
  totals: ForecastTotals;
};

type SizeForecastResp = BaseForecastResp & { rows: SizeForecastRow[] };
type SkuForecastResp = BaseForecastResp & { rows: SkuForecastRow[] };

type ForecastRowUnified = {
  bucket: string; // Size or SKU
  orders: number;
  share_orders: number;
  stock_qty: number;
  avg_daily_orders: number;
  days_cover: number | null;
  risk: string | null;

  required_qty: number;
  gap_qty: number;
};

type ForecastUnified = {
  workspace_slug: string;
  style_key: string;
  window: { start: string; end: string; days: number };
  latest_stock_snapshot_at?: string | null;

  inputs: ForecastInputs;
  totals: ForecastTotals;

  mode: ForecastMode;
  bucket_label: "Size" | "SKU";
  rows: ForecastRowUnified[];
};

type StyleMonthlyRow = {
  style_key: string;
  orders: number;
  returns: number;
  return_pct: number;
  last_order_date?: string | null;
};

type StyleMonthlyResp = {
  workspace_slug: string;
  filters?: {
    start?: string | null;
    end?: string | null;
    month_start?: string | null;
    top_n?: number | null;
  };
  month_totals?: any[];
  rows: StyleMonthlyRow[];
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

function fmtPct(p: any) {
  const x = Number(p ?? 0);
  if (!Number.isFinite(x)) return "0.0%";
  return `${(Math.round(x * 10) / 10).toFixed(1)}%`;
}

function csvEscape(v: any) {
  const s = String(v ?? "");
  const n = s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (n.includes(",") || n.includes('"') || n.includes("\n")) {
    return `"${n.replaceAll('"', '""')}"`;
  }
  return n;
}

function downloadCsv(filename: string, header: string[], rows: any[][]) {
  const lines: string[] = [];
  lines.push(header.map(csvEscape).join(","));
  for (const r of rows) lines.push(r.map(csvEscape).join(","));
  const blob = new Blob(["\uFEFF" + lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

const DEFAULT_INPUTS: ForecastInputs = {
  forecast_days: 30,
  sales_days: 7,
  spike_multiplier: 2,
  lead_time_days: 10,
  target_cover_days: 20,
  safety_stock_pct: 10,
  exclude_rto: false,
};

function unifyForecast(mode: ForecastMode, resp: SizeForecastResp | SkuForecastResp, fallbackInputs: ForecastInputs): ForecastUnified {
  const bucket_label: "Size" | "SKU" = mode === "size" ? "Size" : "SKU";

  const inputs: ForecastInputs = {
    ...fallbackInputs,
    ...resp.inputs,
  } as ForecastInputs;

  const rows: ForecastRowUnified[] =
    mode === "size"
      ? (resp as SizeForecastResp).rows.map((r) => ({
          bucket: (r.size ?? "—").toString(),
          orders: Number(r.orders ?? 0),
          share_orders: Number(r.share_orders ?? 0),
          stock_qty: Number(r.stock_qty ?? 0),
          avg_daily_orders: Number(r.avg_daily_orders ?? 0),
          days_cover: r.days_cover === null || r.days_cover === undefined ? null : Number(r.days_cover),
          risk: r.risk ?? null,
          required_qty: Number(r.required_qty ?? 0),
          gap_qty: Number(r.gap_qty ?? 0),
        }))
      : (resp as SkuForecastResp).rows.map((r) => ({
          bucket: (r.sku ?? "—").toString(),
          orders: Number(r.orders ?? 0),
          share_orders: Number(r.share_orders ?? 0),
          stock_qty: Number(r.stock_qty ?? 0),
          avg_daily_orders: Number(r.avg_daily_orders ?? 0),
          days_cover: r.days_cover === null || r.days_cover === undefined ? null : Number(r.days_cover),
          risk: r.risk ?? null,
          required_qty: Number(r.required_qty ?? 0),
          gap_qty: Number(r.gap_qty ?? 0),
        }));

  return {
    workspace_slug: resp.workspace_slug,
    style_key: resp.style_key,
    window: resp.window,
    latest_stock_snapshot_at: resp.latest_stock_snapshot_at ?? null,
    inputs,
    totals: resp.totals ?? {},
    mode,
    bucket_label,
    rows,
  };
}

export default function ForecastPage() {
  const { workspaceSlug } = useWorkspace();

  const [start, setStart] = React.useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return fmtDateInput(d);
  });
  const [end, setEnd] = React.useState(() => fmtDateInput(new Date()));

  const [styleKey, setStyleKey] = React.useState("");

  const [mode, setMode] = React.useState<ForecastMode>("size");

  // NEW inputs
  const [fInputs, setFInputs] = React.useState<ForecastInputs>({ ...DEFAULT_INPUTS });

  // forecast data
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [data, setData] = React.useState<ForecastUnified | null>(null);

  // top styles
  const [topLoading, setTopLoading] = React.useState(false);
  const [topError, setTopError] = React.useState<string | null>(null);
  const [topRows, setTopRows] = React.useState<StyleMonthlyRow[]>([]);
  const [topN, setTopN] = React.useState(50);
  const [search, setSearch] = React.useState("");

  const [exportAllLoading, setExportAllLoading] = React.useState(false);

  const monthStart = React.useMemo(() => {
    if (!start || start.length < 7) return "";
    return `${start.slice(0, 7)}-01`;
  }, [start]);

  function buildForecastParams(style_key: string) {
    return {
      workspace_slug: workspaceSlug ?? "default",
      style_key,
      start,
      end,
      forecast_days: fInputs.forecast_days,
      sales_days: fInputs.sales_days,
      spike_multiplier: fInputs.spike_multiplier,
      lead_time_days: fInputs.lead_time_days,
      target_cover_days: fInputs.target_cover_days,
      safety_stock_pct: fInputs.safety_stock_pct,
      exclude_rto: fInputs.exclude_rto,
    };
  }

  async function loadForecast(sk?: string, forcedMode?: ForecastMode) {
    const chosen = (sk ?? styleKey).trim();
    const useMode = forcedMode ?? mode;

    if (!chosen) {
      setError("Enter a style_key first.");
      setData(null);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      if (useMode === "size") {
        const resp = await apiGet<SizeForecastResp>("/db/style/size-forecast", buildForecastParams(chosen));
        setData(unifyForecast("size", resp, fInputs));
      } else {
        const resp = await apiGet<SkuForecastResp>("/db/style/sku-forecast", buildForecastParams(chosen));
        setData(unifyForecast("sku", resp, fInputs));
      }
    } catch (e: any) {
      setData(null);
      setError(e?.message ?? "Failed to load forecast");
    } finally {
      setLoading(false);
    }
  }

  async function loadTopStyles() {
    setTopLoading(true);
    setTopError(null);

    try {
      const resp = await apiGet<StyleMonthlyResp>("/db/style-monthly", {
        workspace_slug: workspaceSlug ?? "default",
        month_start: monthStart,
        start,
        end,
        top_n: topN,
      });
      setTopRows(resp.rows ?? []);
    } catch (e: any) {
      setTopRows([]);
      setTopError(e?.message ?? "Failed to load top styles");
    } finally {
      setTopLoading(false);
    }
  }

  React.useEffect(() => {
    if (!workspaceSlug) return;
    if (!start || !end) return;
    loadTopStyles();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceSlug, monthStart, start, end, topN]);

  const filteredTopRows = React.useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return topRows;
    return topRows.filter((r) => String(r.style_key ?? "").toLowerCase().includes(q));
  }, [topRows, search]);

  function exportForecastCsv() {
    if (!data) return;

    const bucketCol = data.mode === "size" ? "size" : "sku";

    const header = [
      "workspace_slug",
      "style_key",
      "mode",
      "start",
      "end",
      "hist_days",
      "forecast_days",
      "sales_days",
      "spike_multiplier",
      "lead_time_days",
      "target_cover_days",
      "safety_stock_pct",
      "exclude_rto",
      bucketCol,
      "orders",
      "share_orders_pct",
      "ratio_per_100",
      "stock_qty",
      "days_cover",
      "risk",
      "required_qty",
      "gap_qty",
      "style_orders_gross",
      "style_orders_net",
      "style_stock_total",
      "style_forecast_units",
      "style_required_on_hand",
      "style_gap_total",
    ];

    const totals = data.totals ?? {};
    const rows = (data.rows ?? []).map((r) => {
      const share = Number(r.share_orders ?? 0);
      const ratio100 = Math.round(share);
      return [
        data.workspace_slug,
        data.style_key,
        data.mode,
        data.window?.start ?? "",
        data.window?.end ?? "",
        data.window?.days ?? "",
        data.inputs.forecast_days,
        data.inputs.sales_days,
        data.inputs.spike_multiplier,
        data.inputs.lead_time_days,
        data.inputs.target_cover_days,
        data.inputs.safety_stock_pct,
        data.inputs.exclude_rto,
        r.bucket ?? "",
        r.orders ?? 0,
        fmtNum(share),
        ratio100,
        r.stock_qty ?? 0,
        r.days_cover === null ? "" : fmtNum(r.days_cover),
        r.risk ?? "",
        fmtNum(r.required_qty ?? 0),
        fmtNum(r.gap_qty ?? 0),
        fmtInt(totals.orders_gross ?? 0),
        fmtInt(totals.orders_net ?? 0),
        fmtInt(totals.stock_qty ?? 0),
        fmtNum(totals.forecast_units ?? 0),
        fmtNum(totals.required_on_hand ?? 0),
        fmtNum(totals.gap_qty ?? 0),
      ];
    });

    const fname = `forecast_selected_${data.workspace_slug}_${data.style_key}_${data.mode}_${data.window?.start ?? ""}_to_${data.window?.end ?? ""}.csv`;
    downloadCsv(fname, header, rows);
  }

  async function exportAllDetailedCsv() {
    if (!filteredTopRows?.length) return;

    setExportAllLoading(true);
    try {
      const header = [
        "workspace_slug",
        "style_key",
        "mode",
        "start",
        "end",
        "hist_days",
        "forecast_days",
        "sales_days",
        "spike_multiplier",
        "lead_time_days",
        "target_cover_days",
        "safety_stock_pct",
        "exclude_rto",
        "bucket_type",
        "bucket_value",
        "orders",
        "share_orders_pct",
        "ratio_per_100",
        "stock_qty",
        "days_cover",
        "risk",
        "required_qty",
        "gap_qty",
        "style_orders_gross",
        "style_orders_net",
        "style_stock_total",
        "style_forecast_units",
        "style_required_on_hand",
        "style_gap_total",
        "style_return_pct",
      ];

      const outRows: any[][] = [];

      for (const s of filteredTopRows) {
        const sk = String(s.style_key ?? "").trim();
        if (!sk) continue;

        let chosen: ForecastUnified | null = null;

        // Try size first
        try {
          const sizeResp = await apiGet<SizeForecastResp>("/db/style/size-forecast", buildForecastParams(sk));
          const uni = unifyForecast("size", sizeResp, fInputs);

          const onlyNoSize =
            (uni.rows?.length ?? 0) === 0 ||
            ((uni.rows?.length ?? 0) === 1 &&
              String(uni.rows[0]?.bucket ?? "").toUpperCase() === "NO_SIZE" &&
              Number(uni.rows[0]?.orders ?? 0) === Number(uni.totals?.orders_net ?? uni.totals?.orders_gross ?? 0));

          if (!onlyNoSize) chosen = uni;
        } catch {
          // ignore
        }

        if (!chosen) {
          const skuResp = await apiGet<SkuForecastResp>("/db/style/sku-forecast", buildForecastParams(sk));
          chosen = unifyForecast("sku", skuResp, fInputs);
        }

        const totals = chosen.totals ?? {};
        const bucketType = chosen.mode === "size" ? "size" : "sku";
        const styleReturnPct = Number((s as any).return_pct ?? 0);

        for (const r of chosen.rows ?? []) {
          const share = Number(r.share_orders ?? 0);
          const ratio100 = Math.round(share);

          outRows.push([
            chosen.workspace_slug,
            chosen.style_key,
            chosen.mode,
            chosen.window?.start ?? "",
            chosen.window?.end ?? "",
            chosen.window?.days ?? "",
            chosen.inputs.forecast_days,
            chosen.inputs.sales_days,
            chosen.inputs.spike_multiplier,
            chosen.inputs.lead_time_days,
            chosen.inputs.target_cover_days,
            chosen.inputs.safety_stock_pct,
            chosen.inputs.exclude_rto,
            bucketType,
            r.bucket ?? "",
            r.orders ?? 0,
            fmtNum(share),
            ratio100,
            r.stock_qty ?? 0,
            r.days_cover === null ? "" : fmtNum(r.days_cover),
            r.risk ?? "",
            fmtNum(r.required_qty ?? 0),
            fmtNum(r.gap_qty ?? 0),
            fmtInt(totals.orders_gross ?? 0),
            fmtInt(totals.orders_net ?? 0),
            fmtInt(totals.stock_qty ?? 0),
            fmtNum(totals.forecast_units ?? 0),
            fmtNum(totals.required_on_hand ?? 0),
            fmtNum(totals.gap_qty ?? 0),
            fmtNum(styleReturnPct),
          ]);
        }
      }

      const fname = `forecast_ALL_detailed_${workspaceSlug ?? "default"}_${start}_to_${end}.csv`;
      downloadCsv(fname, header, outRows);
    } finally {
      setExportAllLoading(false);
    }
  }

  const titleRight = mode === "size" ? "Size Mix + Recommendation" : "SKU Mix + Recommendation";

  const summary = data?.totals ?? {};
  const ordersNet = Number(summary.orders_net ?? summary.orders_gross ?? 0);
  const stockTotal = Number(summary.stock_qty ?? 0);

  return (
    <AppShell>
      <WorkspaceBar />

      <div className="mx-auto max-w-7xl p-6 space-y-6">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div>
            <div className="text-2xl font-semibold">Forecast</div>
            <div className="text-sm text-muted-foreground">
              Mix (orders share) + required stock recommendation. Use SKU mode for non-size styles (e.g. Saree).
            </div>
          </div>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Inputs</CardTitle>
          </CardHeader>

          <CardContent className="space-y-4">
            {/* Row 1 */}
            <div className="grid grid-cols-1 md:grid-cols-6 gap-3">
              <div className="md:col-span-2">
                <label className="text-sm text-muted-foreground">Style Key</label>
                <Input value={styleKey} onChange={(e) => setStyleKey(e.target.value)} placeholder="e.g. 31720640" />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Start</label>
                <Input type="date" value={start} onChange={(e) => setStart(e.target.value)} />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">End</label>
                <Input type="date" value={end} onChange={(e) => setEnd(e.target.value)} />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Top N</label>
                <Input
                  type="number"
                  value={topN}
                  min={5}
                  max={500}
                  onChange={(e) => setTopN(Number(e.target.value || 50))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Forecast Type</label>
                <div className="flex items-center rounded-xl border p-1">
                  <button
                    type="button"
                    className={[
                      "h-9 rounded-lg px-3 text-sm transition",
                      mode === "size" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                    ].join(" ")}
                    onClick={() => setMode("size")}
                  >
                    Size-wise
                  </button>
                  <button
                    type="button"
                    className={[
                      "h-9 rounded-lg px-3 text-sm transition",
                      mode === "sku" ? "bg-muted font-medium" : "text-muted-foreground hover:text-foreground",
                    ].join(" ")}
                    onClick={() => setMode("sku")}
                  >
                    SKU-wise
                  </button>
                </div>
              </div>
            </div>

            {/* Row 2: Forecast params */}
            <div className="grid grid-cols-1 md:grid-cols-7 gap-3">
              <div>
                <label className="text-sm text-muted-foreground">Forecast Days</label>
                <Input
                  type="number"
                  value={fInputs.forecast_days}
                  min={1}
                  max={120}
                  onChange={(e) => setFInputs((p) => ({ ...p, forecast_days: Number(e.target.value || 30) }))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Sales Days</label>
                <Input
                  type="number"
                  value={fInputs.sales_days}
                  min={0}
                  max={31}
                  onChange={(e) => setFInputs((p) => ({ ...p, sales_days: Number(e.target.value || 0) }))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Spike ×</label>
                <Input
                  type="number"
                  step="0.1"
                  value={fInputs.spike_multiplier}
                  min={0.5}
                  max={20}
                  onChange={(e) => setFInputs((p) => ({ ...p, spike_multiplier: Number(e.target.value || 1) }))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Lead Time (days)</label>
                <Input
                  type="number"
                  value={fInputs.lead_time_days}
                  min={0}
                  max={120}
                  onChange={(e) => setFInputs((p) => ({ ...p, lead_time_days: Number(e.target.value || 0) }))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Target Cover (days)</label>
                <Input
                  type="number"
                  value={fInputs.target_cover_days}
                  min={0}
                  max={180}
                  onChange={(e) => setFInputs((p) => ({ ...p, target_cover_days: Number(e.target.value || 0) }))}
                />
              </div>

              <div>
                <label className="text-sm text-muted-foreground">Safety %</label>
                <Input
                  type="number"
                  step="1"
                  value={fInputs.safety_stock_pct}
                  min={0}
                  max={500}
                  onChange={(e) => setFInputs((p) => ({ ...p, safety_stock_pct: Number(e.target.value || 0) }))}
                />
              </div>

              <div className="flex items-end gap-3">
  <label className="flex items-center gap-3 cursor-pointer select-none">
    <input
      type="checkbox"
      className="h-4 w-4"
      checked={fInputs.exclude_rto}
      onChange={(e) => setFInputs((p) => ({ ...p, exclude_rto: e.target.checked }))}
    />
    <div className="text-sm">
      Exclude RTO
      <div className="text-xs text-muted-foreground">Subtract RTO from demand</div>
    </div>
  </label>
</div>

            </div>

            {/* Buttons */}
            <div className="flex items-center gap-3 flex-wrap">
              <Button onClick={() => loadForecast()} disabled={loading}>
                {loading ? "Loading…" : "Load Forecast"}
              </Button>

              <Button variant="outline" onClick={loadTopStyles} disabled={topLoading}>
                {topLoading ? "Loading…" : "Reload Top Styles"}
              </Button>

              {data ? (
                <Button variant="secondary" onClick={exportForecastCsv}>
                  Export (Selected)
                </Button>
              ) : null}

              <Button
                variant="secondary"
                onClick={exportAllDetailedCsv}
                disabled={exportAllLoading || topLoading || !filteredTopRows?.length}
              >
                {exportAllLoading ? "Exporting…" : "Export ALL (Detailed)"}
              </Button>

              <div className="text-xs text-muted-foreground">
                Month for Top Styles: <span className="font-mono">{monthStart}</span>
              </div>
            </div>

            {error ? <div className="text-sm text-red-600">{error}</div> : null}
          </CardContent>
        </Card>

        {/* Summary KPI */}
        {data ? (
          <div className="grid grid-cols-1 md:grid-cols-6 gap-4">
            <Card>
              <CardContent className="p-4">
                <div className="text-xs text-muted-foreground">Orders (Net)</div>
                <div className="text-2xl font-semibold">{fmtInt(ordersNet)}</div>
                {data.inputs.exclude_rto ? (
                  <div className="text-xs text-muted-foreground mt-1">
                    Gross: {fmtInt(summary.orders_gross ?? 0)} | RTO sub: {fmtInt(summary.rto_units_subtracted ?? 0)}
                  </div>
                ) : (
                  <div className="text-xs text-muted-foreground mt-1">Gross: {fmtInt(summary.orders_gross ?? 0)}</div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="text-xs text-muted-foreground">Stock (Total)</div>
                <div className="text-2xl font-semibold">{fmtInt(stockTotal)}</div>
                <div className="text-xs text-muted-foreground mt-1">
                  {data.latest_stock_snapshot_at ? "Snapshot OK" : "No stock snapshot"}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="text-xs text-muted-foreground">Avg Daily</div>
                <div className="text-2xl font-semibold">{fmtNum(summary.avg_daily ?? 0)}</div>
                <div className="text-xs text-muted-foreground mt-1">Hist days: {fmtInt(data.window.days)}</div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="text-xs text-muted-foreground">Forecast Units</div>
                <div className="text-2xl font-semibold">{fmtNum(summary.forecast_units ?? 0)}</div>
                <div className="text-xs text-muted-foreground mt-1">
                  Horizon: {fmtInt(data.inputs.forecast_days)} days
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="text-xs text-muted-foreground">Required On-hand</div>
                <div className="text-2xl font-semibold">{fmtNum(summary.required_on_hand ?? 0)}</div>
                <div className="text-xs text-muted-foreground mt-1">
                  Lead {fmtInt(data.inputs.lead_time_days)} + Cover {fmtInt(data.inputs.target_cover_days)} days | Safety{" "}
                  {fmtNum(data.inputs.safety_stock_pct)}%
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="text-xs text-muted-foreground">Gap Qty</div>
                <div className="text-2xl font-semibold">{fmtNum(summary.gap_qty ?? 0)}</div>
                <div className="text-xs text-muted-foreground mt-1">Need to arrange (if stock low)</div>
              </CardContent>
            </Card>
          </div>
        ) : null}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Top Styles */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle>Top Styles (Orders)</CardTitle>
              <div className="text-xs text-muted-foreground">Click a style to load forecast in current mode + params.</div>
            </CardHeader>
            <CardContent className="space-y-3">
              <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search style_key…" />

              {topError ? <div className="text-sm text-red-600">{topError}</div> : null}

              <div className="rounded-xl border overflow-auto" style={{ maxHeight: 520 }}>
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="p-2">Style</th>
                      <th className="p-2 text-right">Orders</th>
                      <th className="p-2 text-right">Ret%</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topLoading ? (
                      <tr>
                        <td className="p-3 text-sm text-muted-foreground" colSpan={3}>
                          Loading…
                        </td>
                      </tr>
                    ) : filteredTopRows?.length ? (
                      filteredTopRows.map((r, i) => (
                        <tr
                          key={`${r.style_key}-${i}`}
                          className="border-t hover:bg-muted/30 cursor-pointer"
                          onClick={() => {
                            const sk = String(r.style_key);
                            setStyleKey(sk);
                            loadForecast(sk);
                          }}
                        >
                          <td className="p-2 font-mono text-xs">{r.style_key}</td>
                          <td className="p-2 text-right">{r.orders ?? 0}</td>
                          <td className="p-2 text-right font-mono">{fmtPct(r.return_pct)}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td className="p-3 text-sm text-muted-foreground" colSpan={3}>
                          No rows.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>

          {/* Mix Table */}
          <Card>
            <CardHeader className="flex flex-row items-center justify-between gap-3">
              <CardTitle>{titleRight}</CardTitle>
              {data ? (
                <Button variant="outline" size="sm" onClick={exportForecastCsv}>
                  Export (Selected)
                </Button>
              ) : null}
            </CardHeader>

            <CardContent>
              {data ? (
                <>
                  <div className="overflow-auto rounded-xl border">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th className="p-2">{data.bucket_label}</th>
                          <th className="p-2 text-right">Orders</th>
                          <th className="p-2 text-right">Share %</th>
                          <th className="p-2 text-right">Ratio / 100</th>
                          <th className="p-2 text-right">Stock</th>
                          <th className="p-2 text-right">Req Qty</th>
                          <th className="p-2 text-right">Gap</th>
                          <th className="p-2 text-right">Days Cover</th>
                          <th className="p-2">Risk</th>
                        </tr>
                      </thead>
                      <tbody>
                        {data.rows?.length ? (
                          data.rows.map((r, i) => {
                            const share = Number(r.share_orders ?? 0);
                            const ratio100 = Math.round(share);
                            return (
                              <tr key={`${r.bucket}-${i}`} className="border-t">
                                <td className="p-2 font-mono">{r.bucket ?? "—"}</td>
                                <td className="p-2 text-right">{r.orders ?? 0}</td>
                                <td className="p-2 text-right">{fmtNum(share)}</td>
                                <td className="p-2 text-right font-mono">{ratio100}</td>
                                <td className="p-2 text-right">{r.stock_qty ?? 0}</td>
                                <td className="p-2 text-right font-mono">{fmtNum(r.required_qty ?? 0)}</td>
                                <td className="p-2 text-right font-mono">{fmtNum(r.gap_qty ?? 0)}</td>
                                <td className="p-2 text-right">{r.days_cover === null ? "—" : fmtNum(r.days_cover)}</td>
                                <td className="p-2 font-mono text-xs">{r.risk ?? "—"}</td>
                              </tr>
                            );
                          })
                        ) : (
                          <tr>
                            <td className="p-3 text-sm text-muted-foreground" colSpan={9}>
                              No rows.
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>

                  <div className="text-xs text-muted-foreground mt-3">
                    “Req Qty” is the recommended on-hand qty for this bucket based on forecast & cover-days.
                    “Gap” = max(Req Qty - Current Stock, 0).
                  </div>
                </>
              ) : (
                <div className="text-sm text-muted-foreground">Load a forecast to see mix.</div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </AppShell>
  );
}
