// frontend/lib/report.ts
import JSZip from "jszip";
import {
  getKpiSummary,
  getReturnsTrend,
  getTopReturnStyles,
  getTopReturnSkus,
  getZeroSalesSinceLive,
} from "@/lib/api";

function toCSV(rows: any[], headers: string[]) {
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
    ...rows.map((r) => headers.map((h) => esc((r as any)[h])).join(",")),
  ];

  return "\uFEFF" + lines.join("\n");
}

function downloadBlob(filename: string, blob: Blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function safeFilePart(s: string) {
  return String(s).replace(/[<>:"/\\|?*\u0000-\u001F]/g, "-");
}

export async function downloadDashboardReportZip(params: {
  workspace_slug: string;
  start: string;
  end: string;
  mode?: "month" | "matched";
  top_n?: number;
  min_orders?: number;
  zero_min_days_live?: number;
  zero_top_n?: number;
}) {
  const {
    workspace_slug,
    start,
    end,
    mode = "month",
    top_n = 50,
    min_orders = 10,
    zero_min_days_live = 7,
    zero_top_n = 200,
  } = params;

  const [kpi, trend, topStyles, topSkus, zeroSales] = await Promise.all([
    getKpiSummary({ start, end, workspace_slug }),
    getReturnsTrend({ start, end, workspace_slug }),
    getTopReturnStyles({ start, end, workspace_slug, mode, top_n, min_orders }),
    getTopReturnSkus({ start, end, workspace_slug, mode, top_n, min_orders }),
    getZeroSalesSinceLive({ workspace_slug, min_days_live: zero_min_days_live, top_n: zero_top_n }),
  ]);

  const zip = new JSZip();

  zip.file("kpi_summary.json", JSON.stringify(kpi, null, 2));
  zip.file("returns_trend.csv", toCSV(trend, ["date", "returns_units", "return_units", "rto_units"]));

  zip.file(
    "top_return_styles.csv",
    toCSV(topStyles, [
      "brand",
      "product_name",
      "style_key",
      "orders",
      "returns_units",
      "return_units",
      "rto_units",
      "return_pct",
      "last_order_date",
    ])
  );

  zip.file(
    "top_return_skus.csv",
    toCSV(topSkus, [
      "brand",
      "product_name",
      "seller_sku_code",
      "style_key",
      "orders",
      "returns_units",
      "return_units",
      "rto_units",
      "return_pct",
      "last_order_date",
    ])
  );

  zip.file(
    "zero_sales_since_live.csv",
    toCSV(zeroSales, [
      "brand",
      "product_name",
      "style_key",
      "seller_sku_code",
      "live_date",
      "days_live",
      "orders",
    ])
  );

  const blob = await zip.generateAsync({ type: "blob" });
  const filename = `projectm_${safeFilePart(workspace_slug)}_${safeFilePart(start)}_to_${safeFilePart(end)}.zip`;
  downloadBlob(filename, blob);
}
