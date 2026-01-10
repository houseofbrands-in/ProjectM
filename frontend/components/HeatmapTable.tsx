"use client";

import * as React from "react";
import { Button } from "@/components/ui/button";

type HeatmapCol = { reason?: string; label?: string; key?: string };

type HeatmapPayload = {
  row_dim?: "style" | "sku" | "size";
  rows?: any[];
  cols?: HeatmapCol[];
  matrix_units?: number[][];
  matrix_pct?: (number | null)[][];
};

function fmtPct(v: number | null | undefined) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return "—";
  return `${Number(v).toFixed(1)}%`;
}

export default function HeatmapTable({
  heatmap,
  kind,
  loading,
}: {
  heatmap: HeatmapPayload | null;
  kind: "style" | "sku" | "heatmap" | "size";
  loading?: boolean;
}) {
  const [mode, setMode] = React.useState<"units" | "pct">("units");

  const rowDim =
    (heatmap?.row_dim as any) ||
    (kind === "size" ? "size" : kind === "sku" ? "sku" : "style");

  const rows = Array.isArray(heatmap?.rows) ? heatmap!.rows! : [];
  const cols = Array.isArray(heatmap?.cols) ? heatmap!.cols! : [];
  const mUnits = Array.isArray(heatmap?.matrix_units) ? heatmap!.matrix_units! : [];
  const mPct = Array.isArray(heatmap?.matrix_pct) ? heatmap!.matrix_pct! : [];

  const rowKeyLabel =
    rowDim === "sku" ? "SKU" : rowDim === "size" ? "Size" : "Style";

  // row key field name based on response
  const rowKeyField =
    rowDim === "sku"
      ? "seller_sku_code"
      : rowDim === "size"
      ? "size"
      : "style_key";

  function colLabel(c: HeatmapCol) {
    return String(c.reason ?? c.label ?? c.key ?? "");
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-medium">
          Heatmap ({rowKeyLabel} × Reason)
        </div>

        <div className="flex items-center gap-2">
          <Button
            type="button"
            variant={mode === "units" ? "default" : "ghost"}
            className="rounded-lg h-9"
            onClick={() => setMode("units")}
            disabled={!!loading}
          >
            Units
          </Button>
          <Button
            type="button"
            variant={mode === "pct" ? "default" : "ghost"}
            className="rounded-lg h-9"
            onClick={() => setMode("pct")}
            disabled={!!loading}
          >
            %
          </Button>
        </div>
      </div>

      <div className="overflow-auto rounded-xl border">
        <table className="w-full text-sm">
          <thead className="bg-muted/50">
            <tr className="text-left">
              {/* left meta columns */}
              <th className="p-3">Brand</th>
              <th className="p-3">Product</th>
              <th className="p-3">{rowKeyLabel}</th>
              {rowDim === "sku" ? <th className="p-3">Style</th> : null}
              <th className="p-3 text-right">Orders</th>
              <th className="p-3 text-right">Returns</th>

              {/* reason columns */}
              {cols.map((c, j) => (
                <th key={`c-${j}`} className="p-3 text-right whitespace-nowrap">
                  {colLabel(c)}
                </th>
              ))}
            </tr>
          </thead>

          <tbody>
            {loading ? (
              <tr>
                <td className="p-3 text-muted-foreground" colSpan={6 + cols.length}>
                  Loading…
                </td>
              </tr>
            ) : !rows.length ? (
              <tr>
                <td className="p-3 text-muted-foreground" colSpan={6 + cols.length}>
                  No heatmap data.
                </td>
              </tr>
            ) : (
              rows.map((r: any, i: number) => {
                const key = String(r?.[rowKeyField] ?? "");
                const styleKey = String(r?.style_key ?? "");

                const orders = Number(r?.orders ?? 0);
                const returnsUnits = Number(r?.returns_units ?? 0);

                return (
                  <tr key={`${key}-${i}`} className="border-t">
                    <td className="p-3">{r?.brand ?? ""}</td>
                    <td className="p-3">{r?.product_name ?? ""}</td>
                    <td className="p-3 font-mono text-xs">{key}</td>

                    {rowDim === "sku" ? (
                      <td className="p-3 font-mono text-xs">{styleKey}</td>
                    ) : null}

                    <td className="p-3 text-right">{orders}</td>
                    <td className="p-3 text-right">{returnsUnits}</td>

                    {cols.map((_, j) => {
                      const u = mUnits?.[i]?.[j] ?? 0;
                      const p = mPct?.[i]?.[j] ?? null;
                      return (
                        <td key={`v-${i}-${j}`} className="p-3 text-right whitespace-nowrap">
                          {mode === "units" ? Number(u) : fmtPct(p)}
                        </td>
                      );
                    })}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <div className="text-xs text-muted-foreground">
        Tip: Use <span className="font-medium">%</span> to compare across rows, and{" "}
        <span className="font-medium">Units</span> to see absolute impact.
      </div>
    </div>
  );
}
