"use client";

import * as React from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { DateRangeBar } from "@/components/date-range-bar";
import { ReturnsTrendChart } from "@/components/returns-trend-chart";
import { toast } from "sonner";

import {
  getKpiSummary,
  getReturnsTrend,
  getTopReturnStyles,
  getTopReturnSkus,
  getGmvAsp,
  type KpiSummary,
  type ReturnsTrendPoint,
  type TopReturnStyleRow,
  type TopReturnSkuRow,
  type GmvAspKpi,
} from "@/lib/api";


import { getReturnsCohort, type ReturnsCohortRow } from "@/lib/api";
import { downloadDashboardReportZip } from "@/lib/report";
import { useWorkspace } from "@/lib/workspace-context";
import { set } from "date-fns";

type Mode = "month" | "matched";

type CohortScope = "overall" | "style" | "sku";
type CohortMetric = "returns_units" | "return_units" | "rto_units";
type CohortView = "overall" | "same_month";

type ActionBoardRef = "today" | "month_start";

type ActionBoardRow = {
  style_key: string;
  seller_sku_code?: string;
  orders: number;
  returns: number;
  return_pct: number | null;
  last_order_date: string | null;
  style_catalogued_date?: string | null;
};

type ActionBoardResponse = {
  workspace_slug: string;
  month_start: string | null;
  params?: any;
  scale_now: ActionBoardRow[];
  profit_leak: ActionBoardRow[];
  new_potential?: ActionBoardRow[];
};

type SortDir = "asc" | "desc";
type ActionSortKey =
  | "style_key"
  | "orders"
  | "returns"
  | "return_pct"
  | "style_catalogued_date";

type ActionSort = { key: ActionSortKey; dir: SortDir };

// ---- Style Details (drawer) ----
type StyleDetailsMonthly = {
  month_start: string | null;
  orders: number;
  returns: number;
  return_pct: number | null;
  last_order_date: string | null;
};

type StyleDetailsRange = {
  start: string;
  end: string;
  orders: number;
  returns_units: number;
  return_units: number;
  rto_units: number;
  return_pct: number | null;
};

type StyleDetailsResponse = {
  workspace_slug: string;
  style_key: string;
  brand: string | null;
  product_name: string | null;
  live_date: string | null;
  last_order_date: string | null;
  range: StyleDetailsRange | null;
  monthly: StyleDetailsMonthly[];
};

function fmtPct(n: number) {
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(2)}%`;
}

function fmtPctOrDash(n: number | null | undefined) {
  if (n === null || n === undefined || !Number.isFinite(Number(n))) return "—";
  return `${Number(n).toFixed(1)}%`;
}

function fmtInr(n: number) {
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 0,
  }).format(n);
}

function fmtDelta(pct: number | null | undefined) {
  if (pct === null || pct === undefined || !Number.isFinite(Number(pct))) return "—";
  const n = Number(pct);
  const sign = n > 0 ? "+" : "";
  const arrow = n > 0 ? "↑" : n < 0 ? "↓" : "→";
  return `${arrow} ${sign}${n.toFixed(1)}%`;
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

function exportBucketCSV(
  filename: string,
  rows: ActionBoardRow[],
  extra?: Record<string, any>
) {
  const headers = [
  "seller_sku_code",
  "style_key",
  "orders",
  "returns",
  "return_pct",
  "last_order_date",
  "style_catalogued_date",
  ...(extra ? Object.keys(extra) : []),
];


  const outRows = rows.map((r) => ({
  seller_sku_code: r.seller_sku_code ?? "",
  style_key: r.style_key,
  orders: r.orders ?? 0,
  returns: r.returns ?? 0,
  return_pct: r.return_pct ?? "",
  last_order_date: r.last_order_date ?? "",
  style_catalogued_date: r.style_catalogued_date ?? "",
  ...(extra ?? {}),
}));


  downloadCSV(filename, outRows, headers);
}

function nextSort(prev: ActionSort, key: ActionSortKey): ActionSort {
  if (prev.key !== key) return { key, dir: "desc" };
  return { key, dir: prev.dir === "desc" ? "asc" : "desc" };
}

function sortActionRows(rows: ActionBoardRow[], sort: ActionSort): ActionBoardRow[] {
  const dirMul = sort.dir === "asc" ? 1 : -1;

  const get = (r: ActionBoardRow) => {
    switch (sort.key) {
      case "style_key":
        return (r.seller_sku_code ?? r.style_key ?? "");
      case "orders":
        return Number(r.orders ?? 0);
      case "returns":
        return Number(r.returns ?? 0);
      case "return_pct":
        return r.return_pct === null || r.return_pct === undefined
          ? Number.NEGATIVE_INFINITY
          : Number(r.return_pct);
      case "style_catalogued_date":
        return r.style_catalogued_date ?? "";
      default:
        return "";
    }
  };

  return [...rows].sort((a, b) => {
    const av = get(a);
    const bv = get(b);

    if (sort.key === "return_pct") {
      const aNull = a.return_pct === null || a.return_pct === undefined;
      const bNull = b.return_pct === null || b.return_pct === undefined;
      if (aNull && !bNull) return 1;
      if (!aNull && bNull) return -1;
    }

    if (typeof av === "number" && typeof bv === "number") return (av - bv) * dirMul;
    return String(av).localeCompare(String(bv)) * dirMul;
  });
}

function sortIcon(active: boolean, dir: SortDir) {
  if (!active) return "↕";
  return dir === "desc" ? "↓" : "↑";
}

async function fetchActionBoard(params: Record<string, any>) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/action-board?${qs.toString()}`, {
    cache: "no-store",
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`Action board failed: ${res.status} ${t}`.trim());
  }
  return (await res.json()) as ActionBoardResponse;
}

type BrandsResp = {
  workspace_slug: string;
  count: number;
  brands: string[];
};

async function fetchBrands(params: { workspace_slug: string }) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/brands?${qs.toString()}`, { cache: "no-store" });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`brands failed: ${res.status} ${t}`.trim());
  }
  return (await res.json()) as BrandsResp;
}

type BrandGmvAspRow = {
  brand: string;
  orders: number;
  gmv: number;
  asp: number;
  share_pct: number;
};

type BrandGmvAspResp = {
  workspace_slug: string;
  window: { start: string; end: string };
  total_gmv: number;
  total_orders: number;
  rows: BrandGmvAspRow[];
};

async function fetchBrandGmvAsp(params: {
  workspace_slug: string;
  start: string;
  end: string;
  brand?: string;
  top_n?: number;
}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/kpi/brand-gmv-asp?${qs.toString()}`, {
    cache: "no-store",
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`brand-gmv-asp failed: ${res.status} ${t}`.trim());
  }
  return (await res.json()) as BrandGmvAspResp;
}


async function fetchStyleDetails(params: {
  workspace_slug: string;
  style_key: string;
  start?: string; // YYYY-MM-DD
  end?: string; // YYYY-MM-DD
}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (!v) return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/style/details?${qs.toString()}`, {
    cache: "no-store",
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`style/details failed: ${res.status} ${t}`.trim());
  }
  return (await res.json()) as StyleDetailsResponse;
}

// ---- Return Reasons (drawer) ----
type ReturnReasonRow = {
  reason: string; // bucket e.g. FIT_NOT_LIKED, RTO_NO_REASON...
  returns_units: number;
  rto_units: number;
  return_units: number;
  pct_of_top: number;
};

type ReturnsReasonsResp = {
  workspace_slug: string;
  window: { start: string; end: string };
  top_n: number;
  rows: ReturnReasonRow[];
};

async function fetchReturnsReasons(params: {
  workspace_slug: string;
  start: string; // YYYY-MM-DD
  end: string; // YYYY-MM-DD
  style_key?: string;
  top_n?: number;
}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    qs.set(k, String(v));
  });

  const res = await fetch(`/api/db/returns/reasons?${qs.toString()}`, {
    cache: "no-store",
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`returns/reasons ${res.status} ${t}`.trim());
  }
  return (await res.json()) as ReturnsReasonsResp;
}

function prettyBucket(b: string) {
  return (b || "")
    .replaceAll("_", " ")
    .toLowerCase()
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function actionLabel(portal: string | undefined, r: ActionBoardRow) {
  return portal === "flipkart" ? (r.seller_sku_code ?? r.style_key) : r.style_key;
}


function pctFromOrdersAndReturns(r: any) {
  const orders = Number(r?.orders ?? 0);
  const returns = Number(r?.returns_units ?? 0);
  return orders > 0 ? returns / orders : 0;
}

export function DateRangeClient() {
  const [mounted, setMounted] = React.useState(false);
  const { workspaceSlug, portal, start, end, setStart, setEnd } = useWorkspace();
  

  // Controls
  const [mode, setMode] = React.useState<Mode>("month");
  const [topN, setTopN] = React.useState(50);
  const [minOrders, setMinOrders] = React.useState(10);

  // Action Board controls
  const [highReturnPct, setHighReturnPct] = React.useState(30);
  const [newDays, setNewDays] = React.useState(30);
  const [newRef, setNewRef] = React.useState<ActionBoardRef>("today");
  const [actionBoardDim, setActionBoardDim] = React.useState<"style" | "sku">("style");

  // Action Board data
  const [actionBoard, setActionBoard] = React.useState<ActionBoardResponse | null>(null);

  // Sorting per bucket
  const [scaleSort, setScaleSort] = React.useState<ActionSort>({ key: "orders", dir: "desc" });
  const [profitSort, setProfitSort] = React.useState<ActionSort>({ key: "orders", dir: "desc" });
  const [newSort, setNewSort] = React.useState<ActionSort>({ key: "orders", dir: "desc" });

  // Drawer state
  const [selectedStyle, setSelectedStyle] = React.useState<{
    bucket: "scale_now" | "profit_leak" | "new_potential";
    row: ActionBoardRow;
  } | null>(null);

  const [styleDetails, setStyleDetails] = React.useState<StyleDetailsResponse | null>(null);
  const [styleDetailsLoading, setStyleDetailsLoading] = React.useState(false);
  const [styleDetailsError, setStyleDetailsError] = React.useState<string | null>(null);

  const [reasonRows, setReasonRows] = React.useState<ReturnReasonRow[]>([]);
  const [reasonsLoading, setReasonsLoading] = React.useState(false);
  const [reasonsError, setReasonsError] = React.useState<string | null>(null);
  const [reasonsMode, setReasonsMode] = React.useState<"return" | "rto" | "total">("return");

  // Cohort controls
  const [cohortView, setCohortView] = React.useState<CohortView>("overall");
  const [cohortScope, setCohortScope] = React.useState<CohortScope>("overall");
  const [cohortStyleKey, setCohortStyleKey] = React.useState<string>("");
  const [cohortSellerSku, setCohortSellerSku] = React.useState<string>("");
  const [cohortMetric, setCohortMetric] = React.useState<CohortMetric>("return_units");

  // Data
  const [kpi, setKpi] = React.useState<KpiSummary | null>(null);
  const [gmvAsp, setGmvAsp] = React.useState<GmvAspKpi | null>(null);

  const [brandGmvAsp, setBrandGmvAsp] = React.useState<BrandGmvAspResp | null>(null);
  const [brandGmvAspErr, setBrandGmvAspErr] = React.useState<string | null>(null);

  type BrandSortKey = "brand" | "orders" | "gmv" | "asp" | "share_pct";
  const [brandSort, setBrandSort] = React.useState<{ key: BrandSortKey; dir: SortDir }>({
  key: "gmv",
  dir: "desc",
  });


  const [trend, setTrend] = React.useState<ReturnsTrendPoint[]>([]);
  const [topStyles, setTopStyles] = React.useState<TopReturnStyleRow[]>([]);
  const [topSkus, setTopSkus] = React.useState<TopReturnSkuRow[]>([]);
  const [cohortRows, setCohortRows] = React.useState<ReturnsCohortRow[]>([]);

  // Brand filter
  const [brand, setBrand] = React.useState<string>("");

  const [returnMode, setReturnMode] = React.useState<"overall" | "same_month">("overall");
  const [returnType, setReturnType] = React.useState<"all" | "customer">("all");
  const [brands, setBrands] = React.useState<string[]>([]);
  const [brandsLoading, setBrandsLoading] = React.useState(false);
  const [brandsError, setBrandsError] = React.useState<string | null>(null);

  // UI
  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);
  const [lastRefreshed, setLastRefreshed] = React.useState<string>("—");

  const [styleSort, setStyleSort] = React.useState<
    "returns_units_desc" | "orders_desc" | "return_pct_desc"
  >("returns_units_desc");

  const [skuSort, setSkuSort] = React.useState<
    "returns_units_desc" | "orders_desc" | "return_pct_desc"
  >("returns_units_desc");

  async function load(s: string, e: string) {
    setLoading(true);
    setErr(null);

    const brandParam = brand ? brand : undefined;

    try {
      const [summary, trendData, styles, skus, cohortData, action, gmvAspKpi, brandTable] =
  await Promise.all([
        getKpiSummary({
          portal,
          start: s,
          end: e,
          workspace_slug: workspaceSlug,
          brand: brandParam,
          return_mode: returnMode, 
          } as any), 

getReturnsTrend({
  portal,
  start: s,
  end: e,
  workspace_slug: workspaceSlug,
  brand: brandParam,
  return_mode: returnMode,
} as any),

        getTopReturnStyles({
          portal,
          start: s,
          end: e,
          workspace_slug: workspaceSlug,
          top_n: topN,
          min_orders: minOrders,
          mode,
          brand: brandParam,
        } as any),
        getTopReturnSkus({
          portal,
          start: s,
          end: e,
          workspace_slug: workspaceSlug,
          top_n: topN,
          min_orders: minOrders,
          mode,
          brand: brandParam,
        } as any),
        getReturnsCohort({
          portal,
          start: s,
          end: e,
          workspace_slug: workspaceSlug,
          style_key: cohortScope === "style" && cohortStyleKey ? cohortStyleKey : undefined,
          seller_sku_code: cohortScope === "sku" && cohortSellerSku ? cohortSellerSku : undefined,
          brand: brandParam,
        } as any),
        fetchActionBoard({
          portal,
          row_dim: actionBoardDim,
          workspace_slug: workspaceSlug,
          top_n: topN,
          min_orders: minOrders,
          good_return_pct: highReturnPct,
          high_return_pct: highReturnPct,
          new_days: newDays,
          new_ref: newRef,
          new_min_orders: minOrders,
          brand: brandParam,
        }),
        getGmvAsp({ portal, start: s, end: e, workspace_slug: workspaceSlug, brand: brandParam } as any),
        fetchBrandGmvAsp({
          workspace_slug: workspaceSlug,
          start: s,
          end: e,
          brand: brandParam,
          top_n: 50,
         }),
     ]);

      setKpi(summary);
      setGmvAsp(gmvAspKpi ?? null);
      setBrandGmvAsp(brandTable ?? null);
      setBrandGmvAspErr(null);

      setTrend(Array.isArray(trendData) ? trendData : []);
      const styleRows = Array.isArray(styles)
  ? styles
  : Array.isArray((styles as any)?.rows)
  ? (styles as any).rows
  : [];

const skuRows = Array.isArray(skus)
  ? skus
  : Array.isArray((skus as any)?.rows)
  ? (skus as any).rows
  : [];

setTopStyles(styleRows);
setTopSkus(skuRows);

      setCohortRows(Array.isArray(cohortData?.rows) ? cohortData.rows : []);
      setActionBoard(action ?? null);
      setLastRefreshed(new Date().toLocaleString());
    } catch (e2: any) {
      setErr(String(e2?.message ?? e2));
      setKpi(null);
      setGmvAsp(null);
      setTrend([]);
      setTopStyles([]);
      setTopSkus([]);
      setCohortRows([]);
      setActionBoard(null);
      setBrandGmvAsp(null);
      setBrandGmvAspErr(String(e2?.message ?? e2));      
    } finally {
      setLoading(false);
    }
  }

  const sortedTopStyles = React.useMemo(() => {
    const arr = [...topStyles];
    arr.sort((a: any, b: any) => {
      if (styleSort === "orders_desc") return Number(b.orders ?? 0) - Number(a.orders ?? 0);
      if (styleSort === "return_pct_desc")
        return pctFromOrdersAndReturns(b) - pctFromOrdersAndReturns(a);
      return Number(b.returns_units ?? 0) - Number(a.returns_units ?? 0);
    });
    return arr;
  }, [topStyles, styleSort]);

  const sortedTopSkus = React.useMemo(() => {
    const arr = [...topSkus];
    arr.sort((a: any, b: any) => {
      if (skuSort === "orders_desc") return Number(b.orders ?? 0) - Number(a.orders ?? 0);
      if (skuSort === "return_pct_desc")
        return pctFromOrdersAndReturns(b) - pctFromOrdersAndReturns(a);
      return Number(b.returns_units ?? 0) - Number(a.returns_units ?? 0);
    });
    return arr;
  }, [topSkus, skuSort]);

  React.useEffect(() => setMounted(true), []);

  // ✅ load brands when workspace changes
  React.useEffect(() => {
    setBrandsLoading(true);
    setBrandsError(null);
    setBrands([]);
    setBrand(""); // reset to All brands when workspace changes

    fetchBrands({ workspace_slug: workspaceSlug })
      .then((r) => setBrands(Array.isArray(r?.brands) ? r.brands : []))
      .catch((e2: any) => setBrandsError(String(e2?.message ?? e2)))
      .finally(() => setBrandsLoading(false));
  }, [workspaceSlug]);

  React.useEffect(() => {
    if (!mounted) return;
    load(start, end);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    mounted,
    workspaceSlug,
    start,
    end,
    mode,
    topN,
    minOrders,
    cohortScope,
    cohortStyleKey,
    cohortSellerSku,
    highReturnPct,
    newDays,
    newRef,
    brand, // ✅ important
    returnMode,
  ]);

  type KpiCard = { title: string; value: string; meta?: string };

const cards: KpiCard[] = [
  { title: "Orders", value: kpi ? Number(kpi.orders ?? 0).toLocaleString() : "—" },

  ...(returnType === "customer"
    ? ([
        { title: "Customer Returns (Units)", value: kpi ? Number(kpi.return_units ?? 0).toLocaleString() : "—" },
        { title: "RTO (Units)", value: kpi ? Number(kpi.rto_units ?? 0).toLocaleString() : "—" },
        { title: "Customer Return %", value: kpi ? fmtPct(Number(kpi.return_only_pct ?? 0)) : "—" },
        { title: "RTO %", value: kpi ? fmtPct(Number(kpi.rto_pct ?? 0)) : "—" },
      ] as KpiCard[])
    : ([
        {
          title: "Returns (Units)",
          value: kpi
            ? Number(
                (kpi as any).returns_total_units ??
                  (kpi as any).returns_units ??
                  (kpi as any).returns ??
                  0
              ).toLocaleString()
            : "—",
        },
        { title: "Return %", value: kpi ? fmtPct(Number((kpi as any).return_pct ?? 0)) : "—" },
      ] as KpiCard[])),

  {
    title: "GMV",
    value: gmvAsp ? fmtInr(gmvAsp.gmv) : "—",
    meta: gmvAsp ? `Prev: ${fmtInr(gmvAsp.prev_gmv)} • ${fmtDelta(gmvAsp.gmv_change_pct)}` : undefined,
  },
  {
    title: "ASP",
    value: gmvAsp ? fmtInr(gmvAsp.asp) : "—",
    meta: gmvAsp ? `Prev: ${fmtInr(gmvAsp.prev_asp)} • ${fmtDelta(gmvAsp.asp_change_pct)}` : undefined,
  },
];

  // ===== Cohort helpers =====
  const orderMonths = React.useMemo(() => {
    return Array.from(new Set(cohortRows.map((r) => r.order_month))).sort();
  }, [cohortRows]);

  const returnMonths = React.useMemo(() => {
    return Array.from(new Set(cohortRows.map((r) => r.return_month))).sort();
  }, [cohortRows]);

  const cohortMap = React.useMemo(() => {
    const m = new Map<string, ReturnsCohortRow>();
    cohortRows.forEach((r) => m.set(`${r.order_month}|${r.return_month}`, r));
    return m;
  }, [cohortRows]);

  const maxVal = React.useMemo(() => {
    return Math.max(1, ...cohortRows.map((x) => Number((x as any)[cohortMetric] ?? 0)));
  }, [cohortRows, cohortMetric]);

  async function openStyle(
    bucket: "scale_now" | "profit_leak" | "new_potential",
    r: ActionBoardRow
  ) {
    setSelectedStyle({ bucket, row: r });

    // reset drawer data
    setStyleDetails(null);
    setStyleDetailsError(null);
    setStyleDetailsLoading(true);

    setReasonRows([]);
    setReasonsError(null);
    setReasonsLoading(true);
    setReasonsMode("return");

    // fetch reasons (non-blocking for drawer open)
    fetchReturnsReasons({
      workspace_slug: workspaceSlug,
      start,
      end,
      style_key: r.style_key,
      top_n: 20,
    })
      .then((data) => setReasonRows(data.rows ?? []))
      .catch((e2) => setReasonsError((e2 as any)?.message ?? "Failed to load reasons"))
      .finally(() => setReasonsLoading(false));

    // fetch style details (await)
    try {
      const data = await fetchStyleDetails({
        workspace_slug: workspaceSlug,
        style_key: r.style_key,
        start,
        end,
      });
      setStyleDetails(data);
    } catch (e2: any) {
      setStyleDetailsError(String(e2?.message ?? e2));
    } finally {
      setStyleDetailsLoading(false);
    }
  }

  function closeDrawer() {
    setSelectedStyle(null);

    setStyleDetails(null);
    setStyleDetailsError(null);
    setStyleDetailsLoading(false);

    setReasonRows([]);
    setReasonsError(null);
    setReasonsLoading(false);
  }

  function nextBrandSort(
  prev: { key: BrandSortKey; dir: SortDir },
  key: BrandSortKey
) {
  if (prev.key !== key) return { key, dir: "desc" as SortDir };
  return { key, dir: prev.dir === "desc" ? ("asc" as SortDir) : ("desc" as SortDir) };
}

const sortedBrandRows = React.useMemo(() => {
  const rows = [...(brandGmvAsp?.rows ?? [])];
  const dirMul = brandSort.dir === "asc" ? 1 : -1;

  rows.sort((a, b) => {
    const k = brandSort.key;
    const av: any = (a as any)[k];
    const bv: any = (b as any)[k];

    if (k === "brand") return String(av).localeCompare(String(bv)) * dirMul;
    return (Number(av ?? 0) - Number(bv ?? 0)) * dirMul;
  });

  return rows;
}, [brandGmvAsp, brandSort]);


  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Dashboard</div>
          <div className="text-sm text-muted-foreground">
            Sales window & returns window ({mode})
          </div>
          <div className="text-xs text-muted-foreground mt-1">
            Workspace: {workspaceSlug} • Last refreshed: {lastRefreshed}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <select
            value={mode}
            onChange={(e) => setMode(e.target.value as Mode)}
            className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm"
            disabled={loading}
            title="Window match mode"
          >
            <option value="month">month</option>
            <option value="matched">matched</option>
          </select>

          {/* ✅ Brand dropdown */}
          <select
            value={brand}
            onChange={(e) => setBrand(e.target.value)}
            className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm"
            disabled={loading || brandsLoading}
            title={brandsError ? `Brand load failed: ${brandsError}` : "Brand filter"}
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

            {/* Return mode */}
            <select
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              value={returnMode}
              onChange={(e) => setReturnMode(e.target.value as any)}
              title="Return mode"
            >
              <option value="overall">Overall returns</option>
              <option value="same_month">Same-month returns</option>
            </select>
             {/* Return type */}
<select
  className="h-9 rounded-xl border bg-background px-3 text-sm"
  value={returnType}
  onChange={(e) => setReturnType(e.target.value as any)}
  title="Return type"
>
  <option value="all">All returns (Customer + RTO)</option>
  <option value="customer">Customer return + RTO split</option>
</select>

          <input
            type="number"
            min={1}
            value={topN}
            onChange={(e) => setTopN(Number(e.target.value || 1))}
            className="h-10 w-24 rounded-xl border bg-background px-3 text-sm shadow-sm"
            disabled={loading}
            title="Top N"
          />

          <input
            type="number"
            min={0}
            value={minOrders}
            onChange={(e) => setMinOrders(Number(e.target.value || 0))}
            className="h-10 w-28 rounded-xl border bg-background px-3 text-sm shadow-sm"
            disabled={loading}
            title="Min orders"
          />

          <input
            type="number"
            min={0}
            max={100}
            step={1}
            value={highReturnPct}
            onChange={(e) => setHighReturnPct(Number(e.target.value || 0))}
            className="h-10 w-28 rounded-xl border bg-background px-3 text-sm shadow-sm"
            disabled={loading}
            title="High return threshold (%)"
          />

          <select
            value={newDays}
            onChange={(e) => setNewDays(Number(e.target.value))}
            className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm"
            disabled={loading}
            title="New live days"
          >
            <option value={15}>New days: 15</option>
            <option value={30}>New days: 30</option>
            <option value={60}>New days: 60</option>
            <option value={365}>New days: 365</option>
          </select>

          <select
            value={newRef}
            onChange={(e) => setNewRef(e.target.value as ActionBoardRef)}
            className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm"
            disabled={loading}
            title="New reference"
          >
            <option value="today">New ref: today</option>
            <option value="month_start">New ref: month</option>
          </select>

          {portal === "myntra" && (
            <select
              value={actionBoardDim}
              onChange={(e) => setActionBoardDim(e.target.value as any)}
              className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm"
              disabled={loading}
              title="Action board dimension"
            >
              <option value="style">Action board: Style</option>
              <option value="sku">Action board: SKU</option>
            </select>
          )}

          {mounted ? (
            <DateRangeBar
              start={start}
              end={end}
              onApply={({ start: s, end: e }) => {
                setStart(s);
                setEnd(e);
                load(s, e);
              }}
            />
          ) : null}

          <Button
            variant="outline"
            className="rounded-xl"
            disabled={loading || trend.length === 0}
            onClick={() => {
              if (!trend.length) return;
              downloadCSV(
                `returns_trend_${workspaceSlug}_${start}_to_${end}.csv`,
                trend,
                ["date", "returns_units", "return_units", "rto_units"]
              );
              toast.success("Trend CSV downloaded");
            }}
          >
            Export Trend CSV
          </Button>

          <a
            href="/ad-recommendations"
            className="h-10 rounded-xl border bg-background px-3 text-sm shadow-sm inline-flex items-center"
          >
            Open Ad Recommendations
          </a>

          <Button
            className="rounded-xl"
            disabled={loading || !kpi}
            onClick={async () => {
              const id = toast.loading("Preparing report...");
              try {
                await downloadDashboardReportZip({
                  workspace_slug: workspaceSlug,
                  start,
                  end,
                  mode,
                  top_n: topN,
                  min_orders: minOrders,
                });
                toast.success("Report downloaded");
              } catch (e2: any) {
                toast.error("Report failed", { description: String(e2?.message ?? e2) });
              } finally {
                toast.dismiss(id);
              }
            }}
          >
            Download report
          </Button>
        </div>
      </div>

      {brandsError ? (
        <div className="text-xs text-red-600">Brand load error: {brandsError}</div>
      ) : null}

      {err ? <div className="text-sm text-red-600">Error: {err}</div> : null}

      {/* KPI Cards */}
      <div className="grid gap-4 md:grid-cols-6">

        {cards.map((c) => (
          <Card key={c.title} className="rounded-2xl">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm text-muted-foreground">{c.title}</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-semibold">{loading ? "…" : c.value}</div>
              {c.meta ? <div className="text-xs text-muted-foreground mt-1">{c.meta}</div> : null}

              <div className="text-xs text-muted-foreground mt-1">
                {start} → {end}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Brand-wise GMV + ASP */}
<Card className="rounded-2xl">
  <CardHeader className="flex flex-row items-center justify-between gap-2">
    <CardTitle className="text-base">Brand GMV & ASP</CardTitle>

    <div className="flex items-center gap-2">
      <Button
        variant="outline"
        className="rounded-xl"
        disabled={loading || !sortedBrandRows.length}
        onClick={() => {
          downloadCSV(
            `brand_gmv_asp_${workspaceSlug}_${start}_to_${end}.csv`,
            sortedBrandRows,
            ["brand", "orders", "gmv", "asp", "share_pct"]
          );
          toast.success("Brand GMV/ASP CSV downloaded");
        }}
      >
        Export CSV
      </Button>
    </div>
  </CardHeader>

  <CardContent>
    {brandGmvAspErr ? (
      <div className="text-sm text-red-600">Brand table error: {brandGmvAspErr}</div>
    ) : loading ? (
      <div className="text-sm text-muted-foreground">Loading…</div>
    ) : !sortedBrandRows.length ? (
      <div className="text-sm text-muted-foreground">No brand GMV data.</div>
    ) : (
      <div className="overflow-auto rounded-xl border">
        <table className="w-full text-sm">
          <thead className="bg-muted/50">
            <tr className="text-left">
              <th
                className="p-3 cursor-pointer select-none"
                onClick={() => setBrandSort((p) => nextBrandSort(p, "brand"))}
              >
                Brand{" "}
                <span className="text-xs opacity-70">
                  {sortIcon(brandSort.key === "brand", brandSort.dir)}
                </span>
              </th>
              <th
                className="p-3 text-right cursor-pointer select-none"
                onClick={() => setBrandSort((p) => nextBrandSort(p, "orders"))}
              >
                Orders{" "}
                <span className="text-xs opacity-70">
                  {sortIcon(brandSort.key === "orders", brandSort.dir)}
                </span>
              </th>
              <th
                className="p-3 text-right cursor-pointer select-none"
                onClick={() => setBrandSort((p) => nextBrandSort(p, "gmv"))}
              >
                GMV{" "}
                <span className="text-xs opacity-70">
                  {sortIcon(brandSort.key === "gmv", brandSort.dir)}
                </span>
              </th>
              <th
                className="p-3 text-right cursor-pointer select-none"
                onClick={() => setBrandSort((p) => nextBrandSort(p, "asp"))}
              >
                ASP{" "}
                <span className="text-xs opacity-70">
                  {sortIcon(brandSort.key === "asp", brandSort.dir)}
                </span>
              </th>
              <th
                className="p-3 text-right cursor-pointer select-none"
                onClick={() => setBrandSort((p) => nextBrandSort(p, "share_pct"))}
              >
                Share %{" "}
                <span className="text-xs opacity-70">
                  {sortIcon(brandSort.key === "share_pct", brandSort.dir)}
                </span>
              </th>
            </tr>
          </thead>

          <tbody>
            {sortedBrandRows.map((r, i) => (
              <tr key={`${r.brand}-${i}`} className="border-t">
                <td className="p-3">{r.brand}</td>
                <td className="p-3 text-right">{Number(r.orders ?? 0).toLocaleString()}</td>
                <td className="p-3 text-right font-mono">{fmtInr(Number(r.gmv ?? 0))}</td>
                <td className="p-3 text-right font-mono">{fmtInr(Number(r.asp ?? 0))}</td>
                <td className="p-3 text-right font-mono">
                  {Number.isFinite(Number(r.share_pct)) ? `${Number(r.share_pct).toFixed(1)}%` : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )}
  </CardContent>
</Card>


      {/* Action Board */}
      <Card className="rounded-2xl">
        <CardHeader className="flex flex-row items-center justify-between gap-2">
          <CardTitle className="text-base">Action Board (Monthly Snapshots)</CardTitle>
          <div className="text-xs text-muted-foreground">
            High Return ≥ {highReturnPct}% • New = {newDays} days • Ref = {newRef} • Snapshot month:{" "}
            {actionBoard?.month_start ?? "—"}
          </div>
        </CardHeader>

        <CardContent>
          {!actionBoard ? (
            <div className="text-sm text-muted-foreground">No action-board data yet.</div>
          ) : (
            <div className="grid gap-4 md:grid-cols-3">
              {/* Scale Now */}
              <div className="rounded-2xl border p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-sm font-semibold">Scale Now</div>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={loading || !(actionBoard?.scale_now?.length)}
                    onClick={() => {
                      const rows = sortActionRows(actionBoard?.scale_now ?? [], scaleSort);
                      exportBucketCSV(
                        `action_board_scale_now_${actionBoard?.month_start ?? "na"}.csv`,
                        rows,
                        {
                          workspace: workspaceSlug,
                          month_start: actionBoard?.month_start ?? "",
                          min_orders: minOrders,
                          high_return_pct: highReturnPct,
                        }
                      );
                    }}
                  >
                    Export
                  </Button>
                </div>

                <div className="text-xs text-muted-foreground mb-2">
                  Orders ≥ {minOrders} & Return% ≤ {highReturnPct}
                </div>

                {actionBoard.scale_now?.length ? (
                  <div className="overflow-auto rounded-xl border">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th
                            className="p-2 cursor-pointer select-none"
                            onClick={() => setScaleSort((p) => nextSort(p, "style_key"))}
                          >
                            Style{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(scaleSort.key === "style_key", scaleSort.dir)}
                            </span>
                          </th>
                          <th
                            className="p-2 text-right cursor-pointer select-none"
                            onClick={() => setScaleSort((p) => nextSort(p, "orders"))}
                          >
                            Orders{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(scaleSort.key === "orders", scaleSort.dir)}
                            </span>
                          </th>
                          <th
                            className="p-2 text-right cursor-pointer select-none"
                            onClick={() => setScaleSort((p) => nextSort(p, "return_pct"))}
                          >
                            Ret%{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(scaleSort.key === "return_pct", scaleSort.dir)}
                            </span>
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortActionRows(actionBoard.scale_now ?? [], scaleSort)
                          .slice(0, 20)
                          .map((r, i) => (
                            <tr key={`${(r.seller_sku_code ?? r.style_key)}-${i}`} className="border-t">
                             <td className="p-2">
  {portal === "flipkart" ? (
    <span className="font-mono text-xs">{actionLabel(portal, r)}</span>
  ) : (
    <button
      className="font-mono text-xs underline underline-offset-2 hover:opacity-80"
      onClick={() => openStyle("scale_now", r)}
      type="button"
    >
      {actionLabel(portal, r)}
    </button>
  )}
</td>

                              <td className="p-2 text-right">{Number(r.orders ?? 0)}</td>
                              <td className="p-2 text-right font-mono">{fmtPctOrDash(r.return_pct)}</td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No styles match.</div>
                )}
              </div>

              {/* Profit Leak */}
              <div className="rounded-2xl border p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-sm font-semibold">Profit Leak</div>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={loading || !(actionBoard?.profit_leak?.length)}
                    onClick={() => {
                      const rows = sortActionRows(actionBoard?.profit_leak ?? [], profitSort);
                      exportBucketCSV(
                        `action_board_profit_leak_${actionBoard?.month_start ?? "na"}.csv`,
                        rows,
                        {
                          workspace: workspaceSlug,
                          month_start: actionBoard?.month_start ?? "",
                          min_orders: minOrders,
                          high_return_pct: highReturnPct,
                        }
                      );
                    }}
                  >
                    Export
                  </Button>
                </div>

                <div className="text-xs text-muted-foreground mb-2">
                  Orders ≥ {minOrders} & Return% ≥ {highReturnPct}
                </div>

                {actionBoard.profit_leak?.length ? (
                  <div className="overflow-auto rounded-xl border">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th
                            className="p-2 cursor-pointer select-none"
                            onClick={() => setProfitSort((p) => nextSort(p, "style_key"))}
                          >
                            Style{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(profitSort.key === "style_key", profitSort.dir)}
                            </span>
                          </th>
                          <th
                            className="p-2 text-right cursor-pointer select-none"
                            onClick={() => setProfitSort((p) => nextSort(p, "orders"))}
                          >
                            Orders{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(profitSort.key === "orders", profitSort.dir)}
                            </span>
                          </th>
                          <th
                            className="p-2 text-right cursor-pointer select-none"
                            onClick={() => setProfitSort((p) => nextSort(p, "returns"))}
                          >
                            Returns{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(profitSort.key === "returns", profitSort.dir)}
                            </span>
                          </th>
                          <th
                            className="p-2 text-right cursor-pointer select-none"
                            onClick={() => setProfitSort((p) => nextSort(p, "return_pct"))}
                          >
                            Ret%{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(profitSort.key === "return_pct", profitSort.dir)}
                            </span>
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortActionRows(actionBoard.profit_leak ?? [], profitSort)
                          .slice(0, 20)
                          .map((r, i) => (
                            <tr key={`${r.style_key}-${i}`} className="border-t">
                              <td className="p-2">
  {portal === "flipkart" ? (
    <span className="font-mono text-xs">{actionLabel(portal, r)}</span>
  ) : (
    <button
      className="font-mono text-xs underline underline-offset-2 hover:opacity-80"
      onClick={() => openStyle("profit_leak", r)}
      type="button"
    >
      {actionLabel(portal, r)}
    </button>
  )}
</td>

                              <td className="p-2 text-right">{Number(r.orders ?? 0)}</td>
                              <td className="p-2 text-right">{Number(r.returns ?? 0)}</td>
                              <td className="p-2 text-right font-mono">{fmtPctOrDash(r.return_pct)}</td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">No styles match.</div>
                )}
              </div>

              {/* New Potential */}
              <div className="rounded-2xl border p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="text-sm font-semibold">New Potential</div>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={loading || !((actionBoard?.new_potential ?? []).length)}
                    onClick={() => {
                      const rows = sortActionRows(actionBoard?.new_potential ?? [], newSort);
                      exportBucketCSV(
                        `action_board_new_potential_${actionBoard?.month_start ?? "na"}.csv`,
                        rows,
                        {
                          workspace: workspaceSlug,
                          month_start: actionBoard?.month_start ?? "",
                          min_orders: minOrders,
                          new_days: newDays,
                          new_ref: newRef,
                        }
                      );
                    }}
                  >
                    Export
                  </Button>
                </div>

                <div className="text-xs text-muted-foreground mb-2">
                  Live within {newDays} days ({newRef}) & Orders ≥ {minOrders}
                </div>

                {(actionBoard.new_potential ?? []).length ? (
                  <div className="overflow-auto rounded-xl border">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th
                            className="p-2 cursor-pointer select-none"
                            onClick={() => setNewSort((p) => nextSort(p, "style_key"))}
                          >
                            Style{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(newSort.key === "style_key", newSort.dir)}
                            </span>
                          </th>
                          <th
                            className="p-2 text-right cursor-pointer select-none"
                            onClick={() => setNewSort((p) => nextSort(p, "orders"))}
                          >
                            Orders{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(newSort.key === "orders", newSort.dir)}
                            </span>
                          </th>
                          <th
                            className="p-2 text-right cursor-pointer select-none"
                            onClick={() => setNewSort((p) => nextSort(p, "style_catalogued_date"))}
                          >
                            Live{" "}
                            <span className="text-xs opacity-70">
                              {sortIcon(newSort.key === "style_catalogued_date", newSort.dir)}
                            </span>
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {sortActionRows(actionBoard.new_potential ?? [], newSort)
                          .slice(0, 20)
                          .map((r, i) => (
                            <tr key={`${r.style_key}-${i}`} className="border-t">
                              <td className="p-2">
  {portal === "flipkart" ? (
    <span className="font-mono text-xs">{actionLabel(portal, r)}</span>
  ) : (
    <button
      className="font-mono text-xs underline underline-offset-2 hover:opacity-80"
      onClick={() => openStyle("new_potential", r)}
      type="button"
    >
      {actionLabel(portal, r)}
    </button>
  )}
</td>

                              <td className="p-2 text-right">{Number(r.orders ?? 0)}</td>
                              <td className="p-2 text-right font-mono text-xs">
                                {r.style_catalogued_date ? r.style_catalogued_date.slice(0, 10) : "—"}
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">
                    No styles match. Try New: 365d or switch Ref: month to verify.
                  </div>
                )}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Trend + Cohort */}
      <div className="grid gap-4 md:grid-cols-2">
        {/* Returns Trend */}
        <Card className="rounded-2xl">
          <CardHeader>
            <CardTitle className="text-base">Returns Trend</CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="text-sm text-muted-foreground">Loading…</div>
            ) : trend.length ? (
              <ReturnsTrendChart data={trend} />
            ) : (
              <div className="text-sm text-muted-foreground">No trend data.</div>
            )}
          </CardContent>
        </Card>

        {/* Returns Cohort */}
        <Card className="rounded-2xl">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle className="text-base">
              Returns Cohort (Order Month × Return Month)
            </CardTitle>

            <div className="flex items-center gap-2 flex-wrap justify-end">
              <Button
                variant={cohortView === "overall" ? "secondary" : "outline"}
                className="rounded-xl h-9"
                disabled={loading}
                onClick={() => setCohortView("overall")}
              >
                Overall returns
              </Button>

              <Button
                variant={cohortView === "same_month" ? "secondary" : "outline"}
                className="rounded-xl h-9"
                disabled={loading}
                onClick={() => setCohortView("same_month")}
              >
                Same-month returns
              </Button>

              <select
                className="h-9 rounded-xl border bg-background px-3 text-sm"
                value={cohortScope}
                onChange={(e) => {
                  const v = e.target.value as CohortScope;
                  setCohortScope(v);
                  if (v !== "style") setCohortStyleKey("");
                  if (v !== "sku") setCohortSellerSku("");
                }}
                disabled={loading}
                title="Cohort scope"
              >
                <option value="overall">Overall</option>
                <option value="style">By Style</option>
                <option value="sku">By SKU</option>
              </select>

              {cohortScope === "style" && (
                <select
                  className="h-9 rounded-xl border bg-background px-3 text-sm"
                  value={cohortStyleKey}
                  onChange={(e) => setCohortStyleKey(e.target.value)}
                  disabled={loading}
                  title="Select style_key"
                >
                  <option value="">Select style</option>
                  {topStyles.map((s: any) => (
                    <option key={String(s.style_key)} value={String(s.style_key)}>
                      {String(s.style_key)} — {s.product_name ?? ""}
                    </option>
                  ))}
                </select>
              )}

              {cohortScope === "sku" && (
                <select
                  className="h-9 rounded-xl border bg-background px-3 text-sm"
                  value={cohortSellerSku}
                  onChange={(e) => setCohortSellerSku(e.target.value)}
                  disabled={loading}
                  title="Select seller_sku_code"
                >
                  <option value="">Select SKU</option>
                  {topSkus.map((k: any) => (
                    <option key={String(k.seller_sku_code)} value={String(k.seller_sku_code)}>
                      {String(k.seller_sku_code)} — {k.product_name ?? ""}
                    </option>
                  ))}
                </select>
              )}

              <select
                className="h-9 rounded-xl border bg-background px-3 text-sm"
                value={cohortMetric}
                onChange={(e) => setCohortMetric(e.target.value as CohortMetric)}
                disabled={loading}
                title="Cohort metric"
              >
                <option value="returns_units">Returns (Total)</option>
                <option value="return_units">Return (Customer)</option>
                <option value="rto_units">RTO</option>
              </select>

              <Button
                variant="outline"
                className="rounded-xl"
                disabled={loading || cohortRows.length === 0}
                onClick={() =>
                  downloadCSV(
                    `returns_cohort_${workspaceSlug}_${start}_to_${end}.csv`,
                    cohortRows,
                    ["order_month", "return_month", "orders", "returns_units", "return_units", "rto_units"]
                  )
                }
              >
                Export CSV
              </Button>
            </div>
          </CardHeader>

          <CardContent>
            {cohortRows.length === 0 ? (
              <div className="text-sm text-muted-foreground">No cohort data.</div>
            ) : cohortView === "same_month" ? (
              <div className="overflow-auto rounded-xl border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="p-3">Order Month</th>
                      <th className="p-3 text-right">Selected Units</th>
                      <th className="p-3 text-right">Return</th>
                      <th className="p-3 text-right">RTO</th>
                      <th className="p-3 text-right">Total</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orderMonths.map((om) => {
                      const r = cohortMap.get(`${om}|${om}`);
                      const sel = r ? Number((r as any)[cohortMetric] ?? 0) : 0;
                      const ret = r ? Number(r.return_units ?? 0) : 0;
                      const rto = r ? Number(r.rto_units ?? 0) : 0;
                      const tot = r ? Number(r.returns_units ?? 0) : 0;

                      return (
                        <tr key={om} className="border-t">
                          <td className="p-3 font-mono text-xs">{om}</td>
                          <td className="p-3 text-right font-semibold">{sel}</td>
                          <td className="p-3 text-right">{ret}</td>
                          <td className="p-3 text-right">{rto}</td>
                          <td className="p-3 text-right">{tot}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="overflow-auto rounded-xl border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="p-3 sticky left-0 bg-muted/50 z-10">Order Month</th>
                      {returnMonths.map((m) => (
                        <th key={m} className="p-3 text-right whitespace-nowrap">
                          {m}
                        </th>
                      ))}
                    </tr>
                  </thead>

                  <tbody>
                    {orderMonths.map((om) => (
                      <tr key={om} className="border-t">
                        <td className="p-3 sticky left-0 bg-background z-10 font-mono text-xs">
                          {om}
                        </td>

                        {returnMonths.map((rm) => {
                          const r = cohortMap.get(`${om}|${rm}`);
                          const val = r ? Number((r as any)[cohortMetric] ?? 0) : 0;

                          const intensity = maxVal > 0 ? val / maxVal : 0;
                          const bg =
                            intensity > 0 ? `rgba(0,0,0,${0.06 + intensity * 0.22})` : "transparent";

                          return (
                            <td key={rm} className="p-3 text-right align-top">
                              <div
                                className="rounded-xl px-2 py-2"
                                style={{ backgroundColor: bg }}
                                title={
                                  r
                                    ? `Total: ${r.returns_units}, Return: ${r.return_units}, RTO: ${r.rto_units}`
                                    : ""
                                }
                              >
                                {r ? (
                                  <>
                                    <div className="font-semibold">{val}</div>
                                    <div className="text-xs text-muted-foreground">
                                      Ret {r.return_units} • RTO {r.rto_units}
                                    </div>
                                  </>
                                ) : (
                                  <span className="text-muted-foreground">—</span>
                                )}
                              </div>
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Top Styles + Top SKUs */}
      <div className="grid gap-4 md:grid-cols-2">
        <Card className="rounded-2xl">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle>Top Return Styles</CardTitle>

            <select
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              value={styleSort}
              onChange={(e) => setStyleSort(e.target.value as any)}
              disabled={loading}
              title="Sort styles"
            >
              <option value="returns_units_desc">Sort: Return Units ↓</option>
              <option value="orders_desc">Sort: Orders ↓</option>
              <option value="return_pct_desc">Sort: Return % ↓</option>
            </select>

            <Button
              variant="outline"
              className="rounded-xl"
              disabled={loading || topStyles.length === 0}
              onClick={() =>
                downloadCSV(
                  `top_return_styles_${workspaceSlug}_${start}_to_${end}.csv`,
                  topStyles,
                  ["brand", "product_name", "style_key", "orders", "returns_units", "return_units", "rto_units", "return_pct", "last_order_date"]
                )
              }
            >
              Export CSV
            </Button>
          </CardHeader>

          <CardContent>
            {loading ? (
              <div className="text-sm text-muted-foreground">Loading…</div>
            ) : topStyles.length ? (
              <div className="overflow-auto rounded-xl border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="p-3">Brand</th>
                      <th className="p-3">Style</th>
                      <th className="p-3 text-right">Orders</th>
                      <th className="p-3 text-right">Return %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedTopStyles.map((r, i) => {
                      const ordersTotal = Number((r as any).orders ?? 0);
                      const tot = Number((r as any).returns_units ?? 0);
                      const pct = ordersTotal > 0 ? (tot / ordersTotal) * 100 : null;

                      return (
                        <tr key={`${(r as any).style_key}-${i}`} className="border-t">
                          <td className="p-3">{(r as any).brand}</td>
                          <td className="p-3 font-mono text-xs">{(r as any).style_key}</td>
                          <td className="p-3 text-right">{ordersTotal}</td>
                          <td className="p-3 text-right font-mono">
                            {tot}
                            {pct === null ? "" : ` (${pct.toFixed(1)}%)`}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">
                No results (try lowering min_orders or increasing topN).
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="rounded-2xl">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle>Top Return SKUs</CardTitle>

            <select
              className="h-9 rounded-xl border bg-background px-3 text-sm"
              value={skuSort}
              onChange={(e) => setSkuSort(e.target.value as any)}
              disabled={loading}
              title="Sort SKUs"
            >
              <option value="returns_units_desc">Sort: Return Units ↓</option>
              <option value="orders_desc">Sort: Orders ↓</option>
              <option value="return_pct_desc">Sort: Return % ↓</option>
            </select>

            <Button
              variant="outline"
              className="rounded-xl"
              disabled={loading || topSkus.length === 0}
              onClick={() =>
                downloadCSV(
                  `top_return_skus_${workspaceSlug}_${start}_to_${end}.csv`,
                  topSkus,
                  ["brand", "product_name", "seller_sku_code", "style_key", "orders", "returns_units", "return_units", "rto_units", "return_pct", "last_order_date"]
                )
              }
            >
              Export CSV
            </Button>
          </CardHeader>

          <CardContent>
            {loading ? (
              <div className="text-sm text-muted-foreground">Loading…</div>
            ) : topSkus.length ? (
              <div className="overflow-auto rounded-xl border">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="p-3">Brand</th>
                      <th className="p-3">SKU</th>
                      <th className="p-3">Style</th>
                      <th className="p-3 text-right">Orders</th>
                      <th className="p-3 text-right">Return %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedTopSkus.map((r, i) => {
                      const ordersTotal = Number((r as any).orders ?? 0);
                      const tot = Number((r as any).returns_units ?? 0);
                      const pct = ordersTotal > 0 ? (tot / ordersTotal) * 100 : null;

                      return (
                        <tr
                          key={`${(r as any).seller_sku_code}-${(r as any).style_key}-${i}`}
                          className="border-t"
                        >
                          <td className="p-3">{(r as any).brand}</td>
                          <td className="p-3 font-mono text-xs">{(r as any).seller_sku_code}</td>
                          <td className="p-3 font-mono text-xs">{(r as any).style_key}</td>
                          <td className="p-3 text-right">{ordersTotal}</td>
                          <td className="p-3 text-right font-mono">
                            {tot}
                            {pct === null ? "" : ` (${pct.toFixed(1)}%)`}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">
                No results (try lowering min_orders or increasing topN).
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Style Drawer */}
      {selectedStyle && (
        <div className="fixed inset-0 z-50">
          <div className="absolute inset-0 bg-black/30" onClick={closeDrawer} />
          <div className="absolute right-0 top-0 h-full w-full max-w-md bg-background shadow-2xl overflow-y-auto">
            <div className="flex items-center justify-between border-b p-4">
              <div>
                <div className="text-sm text-muted-foreground">Style</div>
                <div className="font-mono text-lg">{selectedStyle.row.style_key}</div>
              </div>
              <Button variant="outline" size="sm" onClick={closeDrawer}>
                Close
              </Button>
            </div>

            <div className="p-4 space-y-3 overflow-y-auto" style={{ maxHeight: "calc(100vh - 73px)" }}>
              <div className="text-sm">
                <span className="text-muted-foreground">Bucket:</span>{" "}
                <span className="font-medium">{selectedStyle.bucket}</span>
              </div>

              <div className="grid grid-cols-3 gap-2">
                <div className="rounded-xl border p-3">
                  <div className="text-xs text-muted-foreground">Orders</div>
                  <div className="text-lg font-semibold">{selectedStyle.row.orders ?? 0}</div>
                </div>
                <div className="rounded-xl border p-3">
                  <div className="text-xs text-muted-foreground">Returns</div>
                  <div className="text-lg font-semibold">{selectedStyle.row.returns ?? 0}</div>
                </div>
                <div className="rounded-xl border p-3">
                  <div className="text-xs text-muted-foreground">Return%</div>
                  <div className="text-lg font-semibold">
                    {fmtPctOrDash(selectedStyle.row.return_pct)}
                  </div>
                </div>
              </div>

              {/* Details */}
              {styleDetailsLoading ? (
                <div className="text-sm text-muted-foreground">Loading details…</div>
              ) : styleDetailsError ? (
                <div className="text-sm text-red-600">{styleDetailsError}</div>
              ) : styleDetails ? (
                <div className="space-y-3">
                  <div className="rounded-xl border p-3">
                    <div className="text-xs text-muted-foreground">Catalog</div>
                    <div className="text-sm">
                      <div>
                        <span className="text-muted-foreground">Brand:</span>{" "}
                        {styleDetails.brand ?? "—"}
                      </div>
                      <div>
                        <span className="text-muted-foreground">Product:</span>{" "}
                        {styleDetails.product_name ?? "—"}
                      </div>
                      <div className="font-mono text-xs">
                        <span className="text-muted-foreground">Live:</span>{" "}
                        {styleDetails.live_date ? styleDetails.live_date.slice(0, 10) : "—"}
                      </div>
                    </div>
                  </div>

                  {styleDetails.range && (
                    <div className="rounded-xl border p-3">
                      <div className="text-xs text-muted-foreground">Selected Date Range</div>
                      <div className="grid grid-cols-2 gap-2 mt-2 text-sm">
                        <div>
                          <span className="text-muted-foreground">Orders:</span>{" "}
                          {styleDetails.range.orders}
                        </div>
                        <div>
                          <span className="text-muted-foreground">Returns:</span>{" "}
                          {styleDetails.range.returns_units}
                        </div>
                        <div>
                          <span className="text-muted-foreground">Return:</span>{" "}
                          {styleDetails.range.return_units}
                        </div>
                        <div>
                          <span className="text-muted-foreground">RTO:</span>{" "}
                          {styleDetails.range.rto_units}
                        </div>
                        <div className="col-span-2 font-mono text-xs">
                          <span className="text-muted-foreground">Return%:</span>{" "}
                          {fmtPctOrDash(styleDetails.range.return_pct)}
                        </div>
                      </div>
                    </div>
                  )}

                  {/* ✅ Return Reasons block */}
                  <div className="rounded-xl border p-3">
                    <div className="flex items-center justify-between">
                      <div className="text-xs text-muted-foreground">Top Return Reasons</div>

                      <div className="flex gap-1">
                        <Button
                          variant={reasonsMode === "return" ? "default" : "outline"}
                          size="sm"
                          onClick={() => setReasonsMode("return")}
                        >
                          Return
                        </Button>
                        <Button
                          variant={reasonsMode === "rto" ? "default" : "outline"}
                          size="sm"
                          onClick={() => setReasonsMode("rto")}
                        >
                          RTO
                        </Button>
                        <Button
                          variant={reasonsMode === "total" ? "default" : "outline"}
                          size="sm"
                          onClick={() => setReasonsMode("total")}
                        >
                          Total
                        </Button>
                      </div>
                    </div>

                    {reasonsLoading ? (
                      <div className="mt-2 text-sm text-muted-foreground">Loading reasons…</div>
                    ) : reasonsError ? (
                      <div className="mt-2 text-sm text-red-600">{reasonsError}</div>
                    ) : reasonRows?.length ? (
                      <div className="mt-2 overflow-auto rounded-xl border">
                        <table className="w-full text-sm">
                          <thead className="bg-muted/50">
                            <tr className="text-left">
                              <th className="p-2">Reason</th>
                              <th className="p-2 text-right">Units</th>
                              <th className="p-2 text-right">%</th>
                            </tr>
                          </thead>
                          <tbody>
                            {reasonRows.map((r, i) => {
                              const units =
                                reasonsMode === "return"
                                  ? Number(r.return_units ?? 0)
                                  : reasonsMode === "rto"
                                  ? Number(r.rto_units ?? 0)
                                  : Number(r.returns_units ?? 0);

                              return (
                                <tr key={`${r.reason}-${i}`} className="border-t">
                                  <td className="p-2 text-xs">{prettyBucket(r.reason)}</td>
                                  <td className="p-2 text-right font-mono">{units}</td>
                                  <td className="p-2 text-right font-mono">
                                    {Number.isFinite(Number(r.pct_of_top))
                                      ? `${Math.round(Number(r.pct_of_top) * 10) / 10}%`
                                      : "—"}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="mt-2 text-sm text-muted-foreground">No reasons found.</div>
                    )}
                  </div>

                  <div className="rounded-xl border p-3">
                    <div className="text-xs text-muted-foreground mb-2">Monthly History</div>
                    {styleDetails.monthly?.length ? (
                      <div className="overflow-auto rounded-xl border">
                        <table className="w-full text-sm">
                          <thead className="bg-muted/50">
                            <tr className="text-left">
                              <th className="p-2">Month</th>
                              <th className="p-2 text-right">Orders</th>
                              <th className="p-2 text-right">Returns</th>
                              <th className="p-2 text-right">Ret%</th>
                            </tr>
                          </thead>
                          <tbody>
                            {styleDetails.monthly.slice(0, 12).map((m, i) => (
                              <tr key={`${m.month_start}-${i}`} className="border-t">
                                <td className="p-2 font-mono text-xs">
                                  {m.month_start ? m.month_start.slice(0, 7) : "—"}
                                </td>
                                <td className="p-2 text-right">{m.orders}</td>
                                <td className="p-2 text-right">{m.returns}</td>
                                <td className="p-2 text-right font-mono">
                                  {fmtPctOrDash(m.return_pct)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="text-sm text-muted-foreground">No monthly history.</div>
                    )}
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
