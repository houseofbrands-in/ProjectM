// frontend/lib/api.ts
// Single, clean API client for Project M (NO duplicate helpers)

// =====================
// Helpers
// =====================
function isServer() {
  return typeof window === "undefined";
}

function internalBase() {
  // Next server -> FastAPI direct (Codespaces/VM)
  return process.env.INTERNAL_API_URL ?? "http://127.0.0.1:8000";
}

function publicBase() {
  // Browser should call Next.js proxy route: /app/api/[...path]/route.ts
  return "/api";
}

const API_BASE = typeof window === "undefined" ? internalBase() : publicBase();


/**
 * Build URL for backend routes.
 * - On server: uses INTERNAL_API_URL
 * - On browser: uses Next proxy /api (avoid CORS)
 */
function buildDbUrl(path: string, params?: Record<string, any>) {
  const qs = new URLSearchParams();
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined || v === null || v === "") continue;
      qs.set(k, String(v));
    }
  }
  const prefix = isServer() ? internalBase() : "/api";
  return `${prefix}${path}${qs.toString() ? `?${qs.toString()}` : ""}`;
}

/** Fetch JSON with better FastAPI error messages */
async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers as any);
  headers.set("Cache-Control", "no-store");

  const res = await fetch(url, {
    ...(init ?? {}),
    headers,
  });

  const raw = await res.text().catch(() => "");
  let json: any = null;
  try {
    json = raw ? JSON.parse(raw) : null;
  } catch {
    json = null;
  }

  if (!res.ok) {
    if (json && Array.isArray(json.detail)) {
      const msgs = json.detail
        .map((d: any) => d?.msg || JSON.stringify(d))
        .filter(Boolean);
      throw new Error(msgs.join(" | ") || `Request failed: ${res.status}`);
    }

    const msg =
      (json && (json.detail || json.message)) ||
      raw ||
      `Request failed: ${res.status}`;
    throw new Error(String(msg));
  }

  return (json ?? (raw as any)) as T;
}


const DEFAULT_WS = "default";

// =====================
// Generic GET used by dashboard files
// =====================
export async function apiGet<T>(
  path: string,
  params: Record<string, any> = {}
): Promise<T> {
  return fetchJson<T>(buildDbUrl(path, params));
}

// =====================
// Workspaces
// =====================
export type WorkspaceRow = {
  id: string;
  slug: string;
  name: string;
};

export async function getWorkspaces() {
  return fetchJson<WorkspaceRow[]>(buildDbUrl("/db/workspaces"));
}
export const listWorkspaces = getWorkspaces;

export async function createWorkspace(input: { slug: string; name: string }) {
  return fetchJson<WorkspaceRow>(buildDbUrl("/db/workspaces"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
}

export async function deleteWorkspace(args: { slug: string; force?: boolean }) {
  const slug = args.slug;
  const force = args.force ? "true" : "";
  return fetchJson<{ deleted: boolean; workspace_slug: string; force: boolean; counts: Record<string, number> }>(
    buildDbUrl(`/db/workspaces/${encodeURIComponent(slug)}`, { force }),
    { method: "DELETE" }
  );
}

export function uploadFlipkartEvents(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/flipkart/events", args, replace, workspace_slug);
}

export function uploadFlipkartListing(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/flipkart/listing", args, replace, workspace_slug);
}

export function uploadFlipkartOrders(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/flipkart/orders", args, replace, workspace_slug);
}

export function uploadFlipkartReturns(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/flipkart/returns", args, replace, workspace_slug);
}

// =====================
// KPI Summary
// =====================
export type KpiSummary = {
  workspace_slug?: string;
  window: { start: string; end: string };
  orders: number;
  returns_total_units: number;
  return_pct: number;
  rto_units: number;
  rto_pct: number;
  return_units: number;
  return_only_pct: number;
};


type ReturnMode = "overall" | "same_month";

export async function getReturnsTrend(
  a:
    | {
        start: string;
        end: string;
        workspace_slug?: string;
        portal?: string;
        brand?: string;
        return_mode?: string;
      }
    | string,
  b?: string,
  c?: string
) {
  const start = typeof a === "string" ? a : a.start;
  const end = typeof a === "string" ? (b as string) : a.end;

  const workspace_slug = typeof a === "string" ? DEFAULT_WS : (a.workspace_slug ?? DEFAULT_WS);
  const portal = typeof a === "string" ? undefined : (a as any).portal;

  const brand = typeof a === "string" ? undefined : a.brand;
  const return_mode = typeof a === "string" ? c : (a.return_mode ?? undefined);

  return fetchJson<ReturnsTrendPoint[]>(
    buildDbUrl("/db/kpi/returns-trend", {
      portal,
      start,
      end,
      workspace_slug,
      brand,
      return_mode,
    })
  );
}


export async function getKpiSummary(
  a:
    | {
        start: string;
        end: string;
        workspace_slug?: string;
        portal?: string;
        brand?: string;
        return_mode?: string;
      }
    | string,
  b?: string,
  c?: string
) {
  const start = typeof a === "string" ? a : a.start;
  const end = typeof a === "string" ? (b as string) : a.end;

  const workspace_slug = typeof a === "string" ? DEFAULT_WS : (a.workspace_slug ?? DEFAULT_WS);
  const portal = typeof a === "string" ? undefined : (a as any).portal;

  const brand = typeof a === "string" ? undefined : a.brand;
  const return_mode = typeof a === "string" ? c : (a.return_mode ?? undefined);

  return fetchJson<KpiSummary>(
    buildDbUrl("/db/kpi/summary", {
      portal,
      start,
      end,
      workspace_slug,
      brand,
      return_mode,
    })
  );
}





// =====================
// GMV + ASP (Myntra sales)
// =====================
export type GmvAspKpi = {
  gmv: number;
  orders: number;
  units: number;
  asp: number;

  prev_gmv: number;
  prev_orders: number;
  prev_units: number;
  prev_asp: number;

  gmv_change_pct: number | null;
  asp_change_pct: number | null;
};

export async function getGmvAsp(
  a:
    | {
        start: string;
        end: string;
        workspace_slug?: string;
        portal?: string;
        brand?: string;
      }
    | string,
  b?: string
) {
  const start = typeof a === "string" ? a : a.start;
  const end = typeof a === "string" ? (b as string) : a.end;

  const workspace_slug = typeof a === "string" ? DEFAULT_WS : (a.workspace_slug ?? DEFAULT_WS);
  const portal = typeof a === "string" ? undefined : (a as any).portal;

  const brand = typeof a === "string" ? undefined : a.brand;

  return fetchJson<any>(
    buildDbUrl("/db/kpi/gmv-asp", {
      portal,
      start,
      end,
      workspace_slug,
      brand,
    })
  );
}

// =====================
// ASP Optimizer (Pricing Insights)
// =====================
export type AspOptimizerBand = {
  from: number;
  to: number;
  mid: number;
};

export type AspOptimizerBucket = {
  band: AspOptimizerBand;
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

export type AspOptimizerRow = {
  key: string; // style_key / sku / brand (depending on level)
  current_asp: number;
  days_active: number;
  units: number;
  confidence: "high" | "medium" | "low";
  current_avg_units_per_day: number;

  best_volume_band: AspOptimizerBucket | null;
  best_net_band: AspOptimizerBucket | null;

  lift_units_pct: number | null;
};

export type AspOptimizerTimeseriesPoint = {
  date: string; // YYYY-MM-DD
  units: number;
  gmv: number;
  asp: number | null;
  returns_units: number;
};

export type AspOptimizerResp = {
  portal: string | null;
  level: "brand" | "style" | "sku";
  start: string;
  end: string;
  bucket_size: number;
  rows: AspOptimizerRow[];
  deep_dive: null | {
    timeseries: AspOptimizerTimeseriesPoint[];
  };
  note?: string;
};

export async function getAspOptimizer(args: {
  start: string;
  end: string;
  workspace_slug?: string;
  portal?: string;
  brand?: string;

  level?: "brand" | "style" | "sku";
  key?: string;

  bucket_size?: number;
  top_n?: number;
  min_days?: number;
  min_units?: number;
}): Promise<AspOptimizerResp> {
  const workspace_slug = args.workspace_slug ?? DEFAULT_WS;
  return apiGet<AspOptimizerResp>("/db/kpi/asp-optimizer", {
    start: args.start,
    end: args.end,
    workspace_slug,
    portal: args.portal,
    brand: args.brand,
    level: args.level ?? "style",
    key: args.key,
    bucket_size: args.bucket_size ?? 50,
    top_n: args.top_n ?? 50,
    min_days: args.min_days ?? 7,
    min_units: args.min_units ?? 10,
  });
}




export type BrandGmvAspRow = {
  brand: string;
  orders: number;
  gmv: number;
  asp: number;
  share_pct: number;
};

export type BrandGmvAspResp = {
  workspace_slug: string;
  window: { start: string; end: string };
  total_gmv: number;
  total_orders: number;
  rows: BrandGmvAspRow[];
};

export async function getBrandGmvAsp(args: {
  start: string;
  end: string;
  workspace_slug?: string;
}): Promise<BrandGmvAspResp> {
  const workspace_slug = args.workspace_slug ?? DEFAULT_WS;
  return apiGet<BrandGmvAspResp>("/db/kpi/brand-gmv-asp", {
    start: args.start,
    end: args.end,
    workspace_slug,
  });
}



// =====================
// Monthly Snapshots (style_monthly)
// =====================
export type StyleMonthlyTotalsRow = {
  month_start: string; // YYYY-MM-01
  orders: number;
  returns: number;
  return_pct: number | null;
};

export type StyleMonthlyRow = {
  month_start: string; // YYYY-MM-01
  style_key: string;
  orders: number;
  returns: number;
  return_pct: number | null;
  last_order_date: string | null;
};

export type StyleMonthlyResponse = {
  workspace_slug: string;
  filters: {
    start?: string | null;
    end?: string | null;
    month_start?: string | null;
    top_n: number;
  };
  month_totals: StyleMonthlyTotalsRow[];
  rows: StyleMonthlyRow[];
};

export async function getStyleMonthly(args: {
  workspace_slug?: string;
  // optional range
  start?: string; // YYYY-MM-01
  end?: string;   // YYYY-MM-01
  // optional specific month list
  month_start?: string; // YYYY-MM-01
  top_n?: number;
}) {
  return apiGet<StyleMonthlyResponse>("/db/style-monthly", args);
}

// =====================
// Brands
// =====================
export type BrandsResponse = {
  workspace_slug: string;
  count: number;
  brands: string[];
};

export async function getBrands(args: { workspace_slug?: string; portal?: string } = {}) {
  return apiGet<BrandsResponse>("/db/brands", args);
}



// =====================
// Returns Trend
// =====================
export type ReturnsTrendPoint = {
  date: string; // YYYY-MM-DD
  returns_units: number;
  rto_units: number;
  return_units: number;
};



// =====================
// Ingest Uploads
// =====================
export type IngestResult = {
  filename: string;
  rows_in_file: number;
  inserted: number;
  replace: boolean;
  workspace_slug?: string;
  detected?: any;
};

type UploadArgs =
  | { file: File; replace?: boolean; workspace_slug?: string }
  | File;

async function postFile(
  endpoint: string,
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  let file: File;
  let rep: boolean;
  let ws: string;

  if (args instanceof File) {
    file = args;
    rep = replace ?? true;
    ws = workspace_slug ?? DEFAULT_WS;
  } else {
    file = args.file;
    rep = args.replace ?? true;
    ws = args.workspace_slug ?? DEFAULT_WS;
  }

  const form = new FormData();
  form.append("file", file);

  // Uploads are client-side → always go through Next proxy
  const url = `/api${endpoint}?replace=${rep ? "true" : "false"}&workspace_slug=${encodeURIComponent(
    ws
  )}`;

  return fetchJson<IngestResult>(url, { method: "POST", body: form });
}

export function uploadSales(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/sales", args, replace, workspace_slug);
}
export function uploadReturns(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/returns", args, replace, workspace_slug);
}
export function uploadCatalog(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/catalog", args, replace, workspace_slug);
}
export function uploadStock(
  args: UploadArgs,
  replace?: boolean,
  workspace_slug?: string
) {
  return postFile("/db/ingest/stock", args, replace, workspace_slug);
}


// =====================
// Zero Sales Since Live
// =====================
export type ZeroSalesRow = {
  style_key: string;
  seller_sku_code?: string | null;
  brand?: string | null;
  product_name?: string | null;
  live_date: string; // YYYY-MM-DD
  days_live: number;
  orders: number;
};

function mapZeroSalesRow(r: any): ZeroSalesRow {
  const liveRaw = r.LiveDate ?? r.live_date ?? "";
  const liveStr = String(liveRaw);
  const liveDate = liveStr ? liveStr.slice(0, 10) : "";

  return {
    style_key: String(r.StyleKey ?? r.style_key ?? ""),
    brand: (r.Brand ?? r.brand ?? null) as string | null,
    product_name: (r.ProductName ?? r.product_name ?? null) as string | null,
    seller_sku_code: (r.SellerSkuCode ?? r.seller_sku_code ?? null) as
      | string
      | null,
    live_date: liveDate,
    days_live: Number(r.DaysLive ?? r.days_live ?? 0),
    orders: Number(r.Orders ?? r.orders ?? 0),
  };
}

export async function getZeroSalesSinceLive(params: {
  workspace_slug?: string;
  min_days_live?: number;
  top_n?: number;
  brand?: string;
  sort_dir?: "asc" | "desc";
}): Promise<ZeroSalesRow[]> {
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;
  const min_days_live = params.min_days_live ?? 7;
  const top_n = params.top_n ?? 100;
  const brand = params.brand;
  const sort_dir = params.sort_dir ?? "desc";

  const json = await fetchJson<any>(
    buildDbUrl("/db/kpi/zero-sales-since-live", {
      workspace_slug,
      min_days_live,
      top_n,
      brand,
      sort_dir,
    })
  );

  const arr =
    (Array.isArray(json) && json) ||
    (json && Array.isArray(json.result) && json.result) ||
    (json && Array.isArray(json.rows) && json.rows) ||
    (json && Array.isArray(json.data) && json.data) ||
    (json && Array.isArray(json.items) && json.items) ||
    null;

  if (!arr) {
    throw new Error(
      `Zero-sales API returned unexpected JSON: ${JSON.stringify(json).slice(0, 300)}`
    );
  }

  return arr.map(mapZeroSalesRow);
}


// =====================
// RETURNS INSIGHTS
// =====================
export type ReturnsSummary = {
  workspace_slug: string;
  window: { start: string; end: string };
  orders: number;
  returns_units: number;
  return_pct: number;
  rto_units: number;
  rto_pct: number;
  return_units: number;
  return_only_pct: number;
};

export async function getReturnsSummary(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
  brand?: string;
}) {
  const { portal, start, end, brand } = params;
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;

  return fetchJson<ReturnsSummary>(
    buildDbUrl("/db/returns/summary", { portal, start, end, workspace_slug, brand })
  );
}


export type ReturnReasonRow = {
  reason: string;
  returns_units: number;
  rto_units: number;
  return_units: number;
  pct_of_top: number;
};

export type ReturnsReasonsResponse = {
  workspace_slug: string;
  window: { start: string; end: string };
  top_n: number;
  rows: ReturnReasonRow[];
};

// Canonical function (the one returns-insights imports)
export async function getReturnsReasons(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
  top_n?: number;
  brand?: string;
  style_key?: string;
}) {
  const {
    portal,
    start,
    end,
    workspace_slug = DEFAULT_WS,
    top_n = 20,
    brand,
    style_key,
  } = params;

  return fetchJson<ReturnsReasonsResponse>(
    buildDbUrl("/db/returns/reasons", {
      portal,
      start,
      end,
      workspace_slug,
      top_n,
      brand,
      style_key,
    })
  );
}

// Backward-compat (ONLY if you already had this old name used elsewhere)
export const getReturnReasons = getReturnsReasons;



// =====================
// RETURNS INSIGHTS — STYLE-WISE + SKU-WISE
// =====================
export type ReturnsStyleRow = {
  style_key: string;
  brand: string;
  product_name: string;
  orders: number;
  returns_units: number;
  return_units: number;
  rto_units: number;
  return_pct: number;
  last_order_date: string | null;
  window: { start: string; end: string };
};

export async function getReturnsStyleWise(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
  top_n?: number;
  min_orders?: number;
  brand?: string;
}) {
  const { portal, start, end, top_n = 50, min_orders = 10, brand } = params;
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;

  return fetchJson<ReturnsStyleRow[]>(
    buildDbUrl("/db/returns/style-wise", {  portal, start,
      end,
      workspace_slug,
      top_n,
      min_orders,
      brand, })
  );
}


export type ReturnsSkuRow = {
  seller_sku_code: string;
  style_key: string;
  brand: string;
  product_name: string;
  orders: number;
  returns_units: number;
  return_units: number;
  rto_units: number;
  return_pct: number;
  last_order_date: string | null;
  window: { start: string; end: string };
};

export async function getReturnsSkuWise(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
  top_n?: number;
  min_orders?: number;
  brand?: string;
}) {
  const { portal, start, end, top_n = 50, min_orders = 10, brand } = params;
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;

  return fetchJson<ReturnsSkuRow[]>(
    buildDbUrl("/db/returns/sku-wise", {  portal, start,
      end,
      workspace_slug,
      top_n,
      min_orders,
      brand, })
  );
}


// =====================
// HEATMAP: Style/SKU/Size × Return Reason
// =====================
export type ReasonHeatmapCol = { reason: string };

export type ReasonHeatmapRow = {
  style_key?: string | null;
  seller_sku_code?: string | null;
  size?: string | null;
  brand?: string | null;
  product_name?: string | null;
  orders?: number;
  returns_units?: number;
};

export type ReasonHeatmapResponse = {
  workspace_slug: string;
  row_dim: "style" | "sku" | "size";
  window: { start: string; end: string };
  top_reasons: number;
  top_rows: number;
  rows: ReasonHeatmapRow[];
  cols: ReasonHeatmapCol[];
  matrix_units: number[][];
  matrix_pct: (number | null)[][];
};

// aliases used in page imports
export type HeatmapResponseStyle = ReasonHeatmapResponse;
export type HeatmapResponseSku = ReasonHeatmapResponse;
export type HeatmapResponseSize = ReasonHeatmapResponse;

export async function getHeatmapStyleReason(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
  top_reasons?: number;
  top_rows?: number;
  brand?: string;
}) {
  const { portal, start, end, top_reasons = 10, top_rows = 30, brand } = params;
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;

  return fetchJson<ReasonHeatmapResponse>(
    buildDbUrl("/db/returns/heatmap/style-reason", {  portal, start,
      end,
      workspace_slug,
      top_reasons,
      top_rows,
      brand, })
  );
}


export async function getHeatmapSkuReason(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
  top_reasons?: number; // max 10
  top_rows?: number;
  brand?: string;
}) {
  const { portal, start, end, top_reasons = 10, top_rows = 30, brand } = params;
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;

  return fetchJson<ReasonHeatmapResponse>(
    buildDbUrl("/db/returns/heatmap/sku-reason", {  portal, start,
      end,
      workspace_slug,
      top_reasons,
      top_rows,
      brand, })
  );
}


export async function getSizeReasonHeatmap(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
  top_reasons?: number; // max 10
  top_rows?: number;
}) {
  const { portal, start, end, top_reasons = 10, top_rows = 30 } = params;
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;

  return fetchJson<ReasonHeatmapResponse>(
    buildDbUrl("/db/returns/heatmap/size-reason", {  portal, start,
      end,
      workspace_slug,
      top_reasons,
      top_rows, })
  );
}

// =====================
// Size KPI
// =====================
export type SizeKpiRow = {
  size: string;
  orders: number;
  returns_units: number;
  rto_units: number;
  return_units: number;
  return_pct: number | null;
  rto_pct: number | null;
  return_only_pct: number | null;
};

export type SizeKpiResponse = {
  workspace_slug: string;
  window: { start: string; end: string };
  rows: SizeKpiRow[];
};

export async function getSizeKpi(params: {
  portal?: string;
  start: string;
  end: string;
  workspace_slug?: string;
}) {
  const { portal, start, end } = params;
  const workspace_slug = params.workspace_slug ?? DEFAULT_WS;

  return fetchJson<SizeKpiResponse>(
    buildDbUrl("/db/returns/size-kpi", {  portal, start, end, workspace_slug })
  );
}

// ==============================
// Dashboard: Top Return Styles / SKUs + Cohort
// ==============================
export type TopReturnStyleRow = {
  style_key: string;
  brand?: string;
  product_name?: string;
  orders: number;
  returns_units: number;
  return_units: number;
  rto_units: number;
  return_pct: number;
  last_order_date?: string | null;
};

export async function getTopReturnStyles(
  a:
    | {
        start: string;
        end: string;
        workspace_slug?: string;
        portal?: string;
        brand?: string;
        top_n?: number;
        min_orders?: number;
        return_mode?: string;
      }
    | string,
  b?: string,
  c?: string
) {
  const start = typeof a === "string" ? a : a.start;
  const end = typeof a === "string" ? (b as string) : a.end;

  const workspace_slug = typeof a === "string" ? DEFAULT_WS : (a.workspace_slug ?? DEFAULT_WS);
  const portal = typeof a === "string" ? undefined : (a as any).portal;

  const brand = typeof a === "string" ? undefined : a.brand;
  const top_n = typeof a === "string" ? undefined : a.top_n;
  const min_orders = typeof a === "string" ? undefined : a.min_orders;
  const return_mode = typeof a === "string" ? c : (a.return_mode ?? undefined);

  return fetchJson<any[]>(
    buildDbUrl("/db/kpi/top-return-styles", {
      portal,
      start,
      end,
      workspace_slug,
      brand,
      top_n,
      min_orders,
      return_mode,
    })
  );
}

// frontend/lib/api.ts

export async function uploadFlipkartTraffic(
  file: File,
  opts: { workspace_slug: string; replace_history?: boolean }
): Promise<IngestResult> {
  const { workspace_slug, replace_history = false } = opts;

  const form = new FormData();
  form.append("file", file);

  const qs = new URLSearchParams({
    workspace_slug,
    replace_history: String(replace_history),
  });

  const res = await fetch(`${API_BASE}/db/ingest/flipkart-traffic?${qs.toString()}`, {
    method: "POST",
    body: form,
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data?.detail || "Flipkart traffic upload failed");
  }
  return data as IngestResult;
}

export async function uploadFlipkartGstrSales({
  file,
  replace,
  workspace_slug,
}: {
  file: File;
  replace: boolean;
  workspace_slug: string;
}) {
  const form = new FormData();
  form.append("file", file);

  const qs = new URLSearchParams();
  qs.set("workspace_slug", workspace_slug || "default");
  qs.set("replace", String(!!replace));

  const res = await fetch(`/api/db/ingest/flipkart-gstr-sales?${qs.toString()}`, {
    method: "POST",
    body: form,
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || `Upload failed (${res.status})`);
  }

  return res.json();
}


export type TopReturnSkuRow = {
  seller_sku_code: string;
  style_key?: string;
  brand?: string;
  product_name?: string;
  orders: number;
  returns_units: number;
  return_units: number;
  rto_units: number;
  return_pct: number;
  last_order_date?: string | null;
};

export async function getTopReturnSkus(
  a:
    | {
        start: string;
        end: string;
        workspace_slug?: string;
        portal?: string;
        brand?: string;
        top_n?: number;
        min_orders?: number;
        return_mode?: string;
      }
    | string,
  b?: string,
  c?: string
) {
  const start = typeof a === "string" ? a : a.start;
  const end = typeof a === "string" ? (b as string) : a.end;

  const workspace_slug = typeof a === "string" ? DEFAULT_WS : (a.workspace_slug ?? DEFAULT_WS);
  const portal = typeof a === "string" ? undefined : (a as any).portal;

  const brand = typeof a === "string" ? undefined : a.brand;
  const top_n = typeof a === "string" ? undefined : a.top_n;
  const min_orders = typeof a === "string" ? undefined : a.min_orders;
  const return_mode = typeof a === "string" ? c : (a.return_mode ?? undefined);

  return fetchJson<any[]>(
    buildDbUrl("/db/kpi/top-return-skus", {
      portal,
      start,
      end,
      workspace_slug,
      brand,
      top_n,
      min_orders,
      return_mode,
    })
  );
}


export type ReturnsCohortRow = {
  order_month: string; // "YYYY-MM"
  return_month: string; // "YYYY-MM"
  orders: number;
  returns_units: number;
  return_units: number;
  rto_units: number;
  orders_total?: number;
orders_with_returns?: number;

};

export async function getReturnsCohort(
  a:
    | {
        start: string;
        end: string;
        workspace_slug?: string;
        portal?: string;
        brand?: string;
        return_mode?: string;
      }
    | string,
  b?: string,
  c?: string
) {
  const start = typeof a === "string" ? a : a.start;
  const end = typeof a === "string" ? (b as string) : a.end;

  const workspace_slug = typeof a === "string" ? DEFAULT_WS : (a.workspace_slug ?? DEFAULT_WS);
  const portal = typeof a === "string" ? undefined : (a as any).portal;

  const brand = typeof a === "string" ? undefined : a.brand;
  const return_mode = typeof a === "string" ? c : (a.return_mode ?? undefined);

  return fetchJson<any[]>(
    buildDbUrl("/db/kpi/returns-cohort", {
      portal,
      start,
      end,
      workspace_slug,
      brand,
      return_mode,
    })
  );
}


export type HouseGmvRow = {
  workspace_slug: string;
  workspace_name: string;
  orders: number;
  gmv: number;
  share_pct: number;
};

export type HouseGmvResponse = {
  mode: "all_time" | "range";
  window: { start: string | null; end: string | null };
  total_gmv: number;
  total_orders: number;
  rows: HouseGmvRow[];
};

export async function getHouseGmv(params?: { start?: string; end?: string }) {
  return fetchJson<HouseGmvResponse>(buildDbUrl("/db/kpi/house-gmv", params));
}


// =====================
// Admin • House (All clients)
// =====================
export type HouseSummaryRow = {
  workspace_slug: string;
  workspace_name: string;
  orders: number;
  gmv: number;
  share_pct: number;
  returns_total: number;      // overall (includes RTO) ✅ option B
  returns_rto: number;
  returns_customer: number;
};

export type HouseSummaryResponse = {
  mode: "all_time" | "range";
  window: { start: string | null; end: string | null };
  totals: {
    gmv: number;
    orders: number;
    returns_total: number;
    returns_rto: number;
    returns_customer: number;
  };
  rows: HouseSummaryRow[];
};

export type HouseMonthlyRow = {
  month: string; // YYYY-MM
  orders: number;
  gmv: number;
  returns_total: number;
  returns_rto: number;
  returns_customer: number;
};

export type HouseMonthlyResponse = {
  months: number;
  rows: HouseMonthlyRow[];
};

export async function getHouseSummary(params?: { start?: string; end?: string; portal?: "all" | "myntra" | "flipkart" }) {
  return apiGet<HouseSummaryResponse>("/db/kpi/house-summary", params ?? {});
}

export async function getHouseMonthly(params?: { months?: number; portal?: "all" | "myntra" | "flipkart" }) {
  return apiGet<HouseMonthlyResponse>("/db/kpi/house-monthly", params ?? {});
}




// =====================
// Ads Recommendations
// =====================
export type AdsRecoRow = {
  style_key: string;
  brand?: string | null;
  product_name?: string | null;
  style_total_qty?: number | null;

  live_date?: string | null;
  age_days?: number | null;

  orders_30d: number;
  orders_prev_30d: number;
  momentum?: number | null;

  returns_units_30d: number;
  return_units_30d: number;
  rto_units_30d: number;

  // 0..1
  return_pct_30d?: number | null;
  rto_share_30d?: number | null;
  return_share_30d?: number | null;

  orders_since_live?: number | null;

  snapshot_at?: string | null;
  impressions: number;
  clicks: number;
  add_to_carts: number;
  purchases: number;

  tag: string;
  listing_id?: string | null;
  why: string;
};

// Request params you send (querystring)
export type AdsRecommendationsParams = {
  workspace_slug: string;
  start: string;
  end: string;
  brand?: string;
  new_age_days?: number;
  min_orders?: number;
  high_return_pct?: number;
  in_stock_only?: boolean;
};

// ✅ Actual API response shape (what backend returns)
export type AdsRecommendationsResponse = {
  workspace_slug: string;
  as_of: string;
  params: {
    brand?: string | null;
    new_age_days: number;
    min_orders: number;
    high_return_pct: number;
    latest_snapshot_at: string | null;
    latest_stock_snapshot_at: string | null;
  };
  rows: AdsRecoRow[];
};

export async function getAdsRecommendations(args: {
  start: string;
  end: string;
  workspace_slug?: string;
  portal?: string;          // ✅ add
  brand?: string;
  new_age_days?: number;
  min_orders?: number;
  high_return_pct?: number;
  in_stock_only?: boolean;
}) {
  const workspace_slug = args.workspace_slug ?? DEFAULT_WS;
  return apiGet<any>("/db/ads/recommendations", {
    start: args.start,
    end: args.end,
    workspace_slug,
    portal: args.portal,     // ✅ add
    brand: args.brand,
    new_age_days: args.new_age_days,
    min_orders: args.min_orders,
    high_return_pct: args.high_return_pct,
    in_stock_only: args.in_stock_only,
  });
}

// =====================
// Reconciliation API Functions
// =====================
// Add these to the bottom of frontend/lib/api.ts

// --- Ingestion ---

export async function uploadReconPgForward(
  file: File,
  opts: { workspace_slug: string; status: "settled" | "unsettled"; replace?: boolean }
) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({
    workspace_slug: opts.workspace_slug,
    status: opts.status,
    replace: String(opts.replace ?? true),
  });
  const res = await fetch(`/api/db/recon/ingest/myntra/pg-forward?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "PG Forward upload failed");
  return data;
}

export async function uploadReconPgReverse(
  file: File,
  opts: { workspace_slug: string; status: "settled" | "unsettled"; replace?: boolean }
) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({
    workspace_slug: opts.workspace_slug,
    status: opts.status,
    replace: String(opts.replace ?? true),
  });
  const res = await fetch(`/api/db/recon/ingest/myntra/pg-reverse?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "PG Reverse upload failed");
  return data;
}

export async function uploadReconNonOrder(
  file: File,
  opts: { workspace_slug: string; replace?: boolean }
) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({
    workspace_slug: opts.workspace_slug,
    replace: String(opts.replace ?? true),
  });
  const res = await fetch(`/api/db/recon/ingest/myntra/non-order-settlement?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "Non-order upload failed");
  return data;
}

export async function uploadReconOrderFlow(
  file: File,
  opts: { workspace_slug: string; replace?: boolean }
) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({
    workspace_slug: opts.workspace_slug,
    replace: String(opts.replace ?? true),
  });
  const res = await fetch(`/api/db/recon/ingest/myntra/order-flow?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "Order flow upload failed");
  return data;
}

export async function uploadReconSkuMap(
  file: File,
  opts: { workspace_slug: string; replace?: boolean }
) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({
    workspace_slug: opts.workspace_slug,
    replace: String(opts.replace ?? true),
  });
  const res = await fetch(`/api/db/recon/ingest/myntra/sku-map?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "SKU map upload failed");
  return data;
}

// --- Analytics ---

export async function getReconSummary(params: { workspace_slug?: string }) {
  return apiGet<any>("/db/recon/summary", params);
}

export async function getReconCommissionAudit(params: { workspace_slug?: string; expected_rate?: number }) {
  return apiGet<any>("/db/recon/commission-audit", params);
}

export async function getReconSkuPnl(params: { workspace_slug?: string; top_n?: number }) {
  return apiGet<any>("/db/recon/sku-pnl", params);
}

export async function getReconSettlementTracker(params: { workspace_slug?: string }) {
  return apiGet<any>("/db/recon/settlement-tracker", params);
}

export async function getReconPenaltyAudit(params: { workspace_slug?: string }) {
  return apiGet<any>("/db/recon/penalty-audit", params);
}

// =====================
// Flipkart Reconciliation API Functions
// =====================
// Add these to the bottom of frontend/lib/api.ts

export async function uploadFkSkuPnl(file: File, opts: { workspace_slug: string }) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({ workspace_slug: opts.workspace_slug });
  const res = await fetch(`/api/db/recon/flipkart/ingest/sku-pnl?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "FK SKU PNL upload failed");
  return data;
}

export async function uploadFkOrderPnl(file: File, opts: { workspace_slug: string }) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({ workspace_slug: opts.workspace_slug });
  const res = await fetch(`/api/db/recon/flipkart/ingest/order-pnl?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "FK Order PNL upload failed");
  return data;
}

export async function uploadFkPaymentReport(file: File, opts: { workspace_slug: string }) {
  const form = new FormData();
  form.append("file", file);
  const qs = new URLSearchParams({ workspace_slug: opts.workspace_slug });
  const res = await fetch(`/api/db/recon/flipkart/ingest/payment-report?${qs}`, { method: "POST", body: form });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.detail || "FK Payment report upload failed");
  return data;
}

export async function getFkReconSummary(params: { workspace_slug?: string }) {
  return apiGet<any>("/db/recon/flipkart/summary", params);
}

export async function getFkSkuPnl(params: { workspace_slug?: string; top_n?: number }) {
  return apiGet<any>("/db/recon/flipkart/sku-pnl", params);
}

