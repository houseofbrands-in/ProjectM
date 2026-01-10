"use client";

import * as React from "react";
import { AppShell } from "@/components/app-shell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useWorkspace } from "@/lib/workspace-context";
import {
  getKpiSummary,
  getReturnsReasons,
  getReturnsStyleWise,
  getReturnsSkuWise,
  getHeatmapStyleReason,
  getHeatmapSkuReason,
  getBrands,
  type BrandsResponse,
  type KpiSummary,
  type ReturnReasonRow,
  type ReturnsStyleRow,
  type ReturnsSkuRow,
  type HeatmapResponseStyle,
  type HeatmapResponseSku,
} from "@/lib/api";
import WorkspaceBar from "@/components/WorkspaceBar";


type View = "style" | "sku" | "heatmap";
type HeatmapKind = "style" | "sku";

function ClientOnly({ children }: { children: React.ReactNode }) {
  const [mounted, setMounted] = React.useState(false);
  React.useEffect(() => setMounted(true), []);
  if (!mounted) return null;
  return <>{children}</>;
}

function fmtPct(n: number) {
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(2)}%`;
}

function downloadCSV(filename: string, rows: any[], headers: string[]) {
  const esc = (v: any) => {
    const s = String(v ?? "");
    const n = s.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
    if (n.includes(",") || n.includes('"') || n.includes("\n")) {
      return `"${n.replaceAll('"', '""')}"`;
    }
    return n;
  };

  const lines = [
    headers.join(","),
    ...rows.map((r) => headers.map((h) => esc(r?.[h])).join(",")),
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

function ViewSwitch({
  view,
  setView,
  disabled,
}: {
  view: View;
  setView: (v: View) => void;
  disabled: boolean;
}) {
  return (
    <div className="inline-flex rounded-xl border bg-background p-1">
      <Button
        type="button"
        size="sm"
        variant={view === "style" ? "default" : "ghost"}
        className="rounded-lg h-9"
        onClick={() => setView("style")}
        disabled={disabled}
      >
        Style
      </Button>
      <Button
        type="button"
        size="sm"
        variant={view === "sku" ? "default" : "ghost"}
        className="rounded-lg h-9"
        onClick={() => setView("sku")}
        disabled={disabled}
      >
        SKU
      </Button>
      <Button
        type="button"
        size="sm"
        variant={view === "heatmap" ? "default" : "ghost"}
        className="rounded-lg h-9"
        onClick={() => setView("heatmap")}
        disabled={disabled}
      >
        Heatmap
      </Button>
    </div>
  );
}

function HeatmapKindSwitch({
  kind,
  setKind,
  disabled,
}: {
  kind: HeatmapKind;
  setKind: (v: HeatmapKind) => void;
  disabled: boolean;
}) {
  return (
    <div className="inline-flex rounded-xl border bg-background p-1">
      <Button
        type="button"
        size="sm"
        variant={kind === "style" ? "default" : "ghost"}
        className="rounded-lg h-9"
        onClick={() => setKind("style")}
        disabled={disabled}
      >
        Style heatmap
      </Button>
      <Button
        type="button"
        size="sm"
        variant={kind === "sku" ? "default" : "ghost"}
        className="rounded-lg h-9"
        onClick={() => setKind("sku")}
        disabled={disabled}
      >
        SKU heatmap
      </Button>
    </div>
  );
}

function HeatmapTable({
  heatmap,
  kind,
  loading,
}: {
  heatmap: HeatmapResponseStyle | HeatmapResponseSku | null;
  kind: HeatmapKind;
  loading: boolean;
}) {
  if (loading) return <div className="text-sm text-muted-foreground">Loading…</div>;
  if (!heatmap) return <div className="text-sm text-muted-foreground">No heatmap data.</div>;

  const hm = heatmap as HeatmapResponseStyle | HeatmapResponseSku;
  const cols = (hm.cols ?? []).map((c) => (c as any).reason);
  const isStyle = kind === "style";
  const keyCol = isStyle ? "style_key" : "seller_sku_code";

  return (
    <div className="overflow-auto rounded-xl border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr className="text-left">
            <th className="p-3 whitespace-nowrap">{isStyle ? "Style" : "SKU"}</th>
            <th className="p-3 whitespace-nowrap">Brand</th>
            <th className="p-3 text-right whitespace-nowrap">Orders</th>
            <th className="p-3 text-right whitespace-nowrap">Returns</th>
            {cols.map((c) => (
              <th key={c} className="p-3 text-right whitespace-nowrap">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {hm.rows?.length ? (
            hm.rows.map((r: any, ri: number) => (
              <tr key={`${String(r?.[keyCol] ?? ri)}-${ri}`} className="border-t">
                <td className="p-3 font-mono text-xs whitespace-nowrap">
                  {String(r?.[keyCol] ?? "")}
                </td>
                <td className="p-3 whitespace-nowrap">{String(r?.brand ?? "")}</td>
                <td className="p-3 text-right whitespace-nowrap">{Number(r?.orders ?? 0)}</td>
                <td className="p-3 text-right whitespace-nowrap">
                  {Number(r?.returns_units ?? 0)}
                </td>

                {cols.map((_, ci) => {
                  const v = (hm as any).matrix_pct?.[ri]?.[ci];
                  return (
                    <td key={ci} className="p-3 text-right whitespace-nowrap">
                      {v == null ? "—" : fmtPct(Number(v))}
                    </td>
                  );
                })}
              </tr>
            ))
          ) : (
            <tr>
              <td className="p-3 text-muted-foreground" colSpan={4 + cols.length}>
                No heatmap data.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

export default function ReturnsInsightsPage() {
  return (
    <ClientOnly>
      <ReturnsInsightsInner />
    </ClientOnly>
  );
}

function ReturnsInsightsInner() {
  const { workspaceSlug, portal, start, end } = useWorkspace();

  const [view, setView] = React.useState<View>("style");

  // Brand filter
  const [brands, setBrands] = React.useState<string[]>([]);
  const [brand, setBrand] = React.useState<string>(""); // "" => All brands

  const [topN, setTopN] = React.useState(20);
  const [minOrders, setMinOrders] = React.useState(10);

  // heatmap controls
  const [heatmapKind, setHeatmapKind] = React.useState<HeatmapKind>("style");
  const [topReasons, setTopReasons] = React.useState(10); // backend max 10
  const [topRows, setTopRows] = React.useState(50);

  const [summary, setSummary] = React.useState<KpiSummary | null>(null);
  const [reasons, setReasons] = React.useState<ReturnReasonRow[]>([]);
  const [topStyleRows, setTopStyleRows] = React.useState<ReturnsStyleRow[]>([]);
  const [topSkuRows, setTopSkuRows] = React.useState<ReturnsSkuRow[]>([]);

  const [heatmap, setHeatmap] = React.useState<HeatmapResponseStyle | HeatmapResponseSku | null>(null);

  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);
  const [lastRefreshed, setLastRefreshed] = React.useState<string>("—");

  // Load brand list per workspace
  React.useEffect(() => {
    setBrands([]);
    setBrand("");
    getBrands({ portal, workspace_slug: workspaceSlug })
      .then((res: BrandsResponse) => setBrands(res?.brands ?? []))
      .catch(() => setBrands([]));
  }, [workspaceSlug, portal]);


  async function loadBase() {
    const brandParam = brand ? brand : undefined;

    const [s, r, sw, sku] = await Promise.all([
      getKpiSummary({ portal, start, end, workspace_slug: workspaceSlug, brand: brandParam }),
      getReturnsReasons({ portal, start, end, workspace_slug: workspaceSlug, top_n: topN, brand: brandParam }),
      getReturnsStyleWise({
        portal,
        start,
        end,
        workspace_slug: workspaceSlug,
        top_n: topN,
        min_orders: minOrders,
        brand: brandParam,
      }),
      getReturnsSkuWise({
        portal,
        start,
        end,
        workspace_slug: workspaceSlug,
        top_n: topN,
        min_orders: minOrders,
        brand: brandParam,
      }),
    ]);

    setSummary(s);
    setReasons(r.rows ?? []);
    setTopStyleRows(sw ?? []);
    setTopSkuRows(sku ?? []);
  }

  async function loadHeatmap() {
    const brandParam = brand ? brand : undefined;

    const tr = Math.max(1, Math.min(10, Number(topReasons || 10)));
    const rows = Math.max(1, Number(topRows || 50));

    if (heatmapKind === "style") {
      const hm = await getHeatmapStyleReason({
        portal,
        start,
        end,
        workspace_slug: workspaceSlug,
        top_reasons: tr,
        top_rows: rows,
        brand: brandParam,
      });
      setHeatmap(hm);
      return;
    }

    const hm = await getHeatmapSkuReason({
      portal,
      start,
      end,
      workspace_slug: workspaceSlug,
      top_reasons: tr,
      top_rows: rows,
      brand: brandParam,
    });
    setHeatmap(hm);
  }

  async function loadAll() {
    setLoading(true);
    setErr(null);
    try {
      await loadBase();
      if (view === "heatmap") {
        await loadHeatmap();
      } else {
        setHeatmap(null);
      }
      setLastRefreshed(new Date().toLocaleString());
    } catch (e: any) {
      setErr(String(e?.message ?? e));
      setSummary(null);
      setReasons([]);
      setTopStyleRows([]);
      setTopSkuRows([]);
      setHeatmap(null);
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    loadAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceSlug, portal, start, end, brand, topN, minOrders]);

  React.useEffect(() => {
    if (view === "heatmap") {
      setLoading(true);
      setErr(null);
      loadHeatmap()
        .catch((e: any) => setErr(String(e?.message ?? e)))
        .finally(() => setLoading(false));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, heatmapKind, portal, workspaceSlug, start, end, brand]);


  function exportStyleCSV() {
    if (!topStyleRows.length) return;
    const b = brand ? `_${brand}` : "";
    downloadCSV(
      `returns_stylewise${b}_${workspaceSlug}_${start}_to_${end}.csv`,
      topStyleRows,
      ["brand", "product_name", "style_key", "orders", "returns_units", "return_pct", "last_order_date"]
    );
  }

  function exportSkuCSV() {
    if (!topSkuRows.length) return;
    const b = brand ? `_${brand}` : "";
    downloadCSV(
      `returns_skuwise${b}_${workspaceSlug}_${start}_to_${end}.csv`,
      topSkuRows,
      ["brand", "product_name", "seller_sku_code", "style_key", "orders", "returns_units", "return_pct", "last_order_date"]
    );
  }

  function exportHeatmapCSV() {
    if (!heatmap) return;
    const b = brand ? `_${brand}` : "";

    const hm = heatmap as HeatmapResponseStyle | HeatmapResponseSku;
    const reasonCols = (hm.cols ?? []).map((c) => (c as any).reason);
    const keyCol = heatmapKind === "style" ? "style_key" : "seller_sku_code";

    const rows = (hm.rows ?? []).map((r: any, ri: number) => {
      const base: any = {
        [keyCol]: r?.[keyCol] ?? "",
        brand: r?.brand ?? "",
        orders: Number(r?.orders ?? 0),
        returns_units: Number(r?.returns_units ?? 0),
      };

      reasonCols.forEach((reason, ci) => {
        const v = (hm as any).matrix_pct?.[ri]?.[ci];
        base[reason] = v == null ? "" : Number(v);
      });

      return base;
    });

    downloadCSV(
      `returns_heatmap_${heatmapKind}${b}_${workspaceSlug}_${start}_to_${end}.csv`,
      rows,
      [keyCol, "brand", "orders", "returns_units", ...reasonCols]
    );
  }

  const sectionTitle =
    view === "style"
      ? "Style-wise Returns (Heatmap Base)"
      : view === "sku"
      ? "SKU-wise Returns (Heatmap Base)"
      : "Heatmap: Return Reasons";

  return (
    <AppShell>
      <div className="space-y-6">
        <WorkspaceBar />
        {err ? (
          <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-3 text-sm">
            {err}
          </div>
        ) : null}

        <div className="text-xs text-muted-foreground">
          Workspace: <span className="font-mono">{workspaceSlug}</span> • Brand:{" "}
          <span className="font-mono">{brand || "All"}</span> • Window:{" "}
          <span className="font-mono">
            {start} → {end}
          </span>{" "}
          • Last refreshed: {lastRefreshed}
        </div>

        <Card className="rounded-2xl">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle className="text-base">Returns Summary</CardTitle>
            <div className="flex items-center gap-2">
              <select
                className="h-10 w-52 rounded-xl border bg-background px-3 text-sm"
                value={brand}
                onChange={(e) => setBrand(e.target.value)}
                disabled={loading}
              >
                <option value="">All brands</option>
                {brands.map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>

              <Input
                className="w-20 rounded-xl"
                value={topN}
                onChange={(e) => setTopN(Math.max(1, Number(e.target.value || 1)))}
                type="number"
                min={1}
              />
              <Input
                className="w-24 rounded-xl"
                value={minOrders}
                onChange={(e) => setMinOrders(Math.max(0, Number(e.target.value || 0)))}
                type="number"
                min={0}
              />
              <Button className="rounded-xl" onClick={() => loadAll()} disabled={loading}>
                Reload
              </Button>
            </div>
          </CardHeader>

          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground">Orders</div>
                <div className="text-lg font-semibold">
                  {summary ? Number(summary.orders ?? 0).toLocaleString() : "—"}
                </div>
              </div>
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground">Returns (Units)</div>
                <div className="text-lg font-semibold">
                  {summary ? Number((summary as any).returns_total_units ?? 0).toLocaleString() : "—"}
                </div>
              </div>
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground">Return %</div>
                <div className="text-lg font-semibold">
                  {summary ? fmtPct(Number((summary as any).return_pct ?? 0)) : "—"}
                </div>
              </div>
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground">RTO %</div>
                <div className="text-lg font-semibold">
                  {summary ? fmtPct(Number((summary as any).rto_pct ?? 0)) : "—"}
                </div>
              </div>
              <div className="rounded-xl border p-3">
                <div className="text-xs text-muted-foreground">Return-only %</div>
                <div className="text-lg font-semibold">
                  {summary ? fmtPct(Number((summary as any).return_only_pct ?? 0)) : "—"}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-2xl">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle className="text-base">Top Return Reasons</CardTitle>
            <div className="text-xs text-muted-foreground">
              Source: returns_raw.raw_json → <span className="font-mono">return_reason</span> • (bucketed)
            </div>
          </CardHeader>
          <CardContent>
            <div className="overflow-auto rounded-xl border">
              <table className="w-full text-sm">
                <thead className="bg-muted/50">
                  <tr className="text-left">
                    <th className="p-3">Reason</th>
                    <th className="p-3 text-right">Returns Units</th>
                    <th className="p-3 text-right">Return Units</th>
                    <th className="p-3 text-right">RTO Units</th>
                    <th className="p-3 text-right">% of Top</th>
                  </tr>
                </thead>
                <tbody>
                  {loading ? (
                    <tr>
                      <td className="p-3 text-muted-foreground" colSpan={5}>
                        Loading…
                      </td>
                    </tr>
                  ) : reasons.length ? (
                    reasons.map((r, i) => (
                      <tr key={`${r.reason}-${i}`} className="border-t">
                        <td className="p-3">{r.reason}</td>
                        <td className="p-3 text-right">{r.returns_units}</td>
                        <td className="p-3 text-right">{r.return_units}</td>
                        <td className="p-3 text-right">{r.rto_units}</td>
                        <td className="p-3 text-right">{fmtPct(r.pct_of_top)}</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td className="p-3 text-muted-foreground" colSpan={5}>
                        No data.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-2xl">
          <CardHeader className="flex flex-row items-center justify-between gap-3">
            <div className="flex items-center gap-3">
              <CardTitle className="text-base">{sectionTitle}</CardTitle>
              <ViewSwitch view={view} setView={setView} disabled={loading} />
            </div>

            <div className="flex items-center gap-2">
              {view === "style" ? (
                <Button
                  variant="outline"
                  className="rounded-xl"
                  disabled={loading || topStyleRows.length === 0}
                  onClick={exportStyleCSV}
                >
                  Export CSV
                </Button>
              ) : view === "sku" ? (
                <Button
                  variant="outline"
                  className="rounded-xl"
                  disabled={loading || topSkuRows.length === 0}
                  onClick={exportSkuCSV}
                >
                  Export CSV
                </Button>
              ) : (
                <Button
                  variant="outline"
                  className="rounded-xl"
                  disabled={loading || !heatmap}
                  onClick={exportHeatmapCSV}
                >
                  Export Heatmap CSV
                </Button>
              )}
            </div>
          </CardHeader>

          <CardContent>
            {view === "heatmap" ? (
              <div className="space-y-4">
                <div className="flex flex-wrap items-center gap-2">
                  <HeatmapKindSwitch kind={heatmapKind} setKind={setHeatmapKind} disabled={loading} />
                  <Input
                    className="w-24 rounded-xl"
                    type="number"
                    min={1}
                    max={10}
                    value={topReasons}
                    onChange={(e) =>
                      setTopReasons(Math.max(1, Math.min(10, Number(e.target.value || 10))))
                    }
                  />
                  <Input
                    className="w-24 rounded-xl"
                    type="number"
                    min={1}
                    value={topRows}
                    onChange={(e) => setTopRows(Math.max(1, Number(e.target.value || 50)))}
                  />
                  <Button
                    className="rounded-xl"
                    onClick={() => {
                      setLoading(true);
                      setErr(null);
                      loadHeatmap()
                        .catch((e: any) => setErr(String(e?.message ?? e)))
                        .finally(() => setLoading(false));
                    }}
                    disabled={loading}
                  >
                    Reload heatmap
                  </Button>
                </div>

                <HeatmapTable heatmap={heatmap} kind={heatmapKind} loading={loading} />
              </div>
            ) : view === "style" ? (
              <div className="overflow-auto rounded-xl border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="p-3">Brand</th>
                      <th className="p-3">Product</th>
                      <th className="p-3">Style</th>
                      <th className="p-3 text-right">Orders</th>
                      <th className="p-3 text-right">Returns</th>
                      <th className="p-3 text-right">Return %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {loading ? (
                      <tr>
                        <td className="p-3 text-muted-foreground" colSpan={6}>
                          Loading…
                        </td>
                      </tr>
                    ) : topStyleRows.length ? (
                      topStyleRows.map((r: any, i: number) => (
                        <tr key={`${r.style_key}-${i}`} className="border-t">
                          <td className="p-3">{r.brand}</td>
                          <td className="p-3">{r.product_name}</td>
                          <td className="p-3 font-mono text-xs">{r.style_key}</td>
                          <td className="p-3 text-right">{r.orders}</td>
                          <td className="p-3 text-right">{r.returns_units}</td>
                          <td className="p-3 text-right">{fmtPct(Number(r.return_pct || 0))}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td className="p-3 text-muted-foreground" colSpan={6}>
                          No results.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="overflow-auto rounded-xl border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="p-3">Brand</th>
                      <th className="p-3">Product</th>
                      <th className="p-3">SKU</th>
                      <th className="p-3">Style</th>
                      <th className="p-3 text-right">Orders</th>
                      <th className="p-3 text-right">Returns</th>
                      <th className="p-3 text-right">Return %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {loading ? (
                      <tr>
                        <td className="p-3 text-muted-foreground" colSpan={7}>
                          Loading…
                        </td>
                      </tr>
                    ) : topSkuRows.length ? (
                      topSkuRows.map((r: any, i: number) => (
                        <tr key={`${r.seller_sku_code}-${i}`} className="border-t">
                          <td className="p-3">{r.brand}</td>
                          <td className="p-3">{r.product_name}</td>
                          <td className="p-3 font-mono text-xs">{r.seller_sku_code}</td>
                          <td className="p-3 font-mono text-xs">{r.style_key}</td>
                          <td className="p-3 text-right">{r.orders}</td>
                          <td className="p-3 text-right">{r.returns_units}</td>
                          <td className="p-3 text-right">{fmtPct(Number(r.return_pct || 0))}</td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td className="p-3 text-muted-foreground" colSpan={7}>
                          No results.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </AppShell>
  );
}
