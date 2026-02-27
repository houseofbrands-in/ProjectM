"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { toast } from "sonner";
import {
  getHouseMonthly,
  getHouseSummary,
  type HouseMonthlyRow,
  type HouseSummaryResponse,
} from "@/lib/api";

type Portal = "all" | "myntra" | "flipkart";

function fmtINR(n: number) {
  try {
    return new Intl.NumberFormat("en-IN", {
      style: "currency",
      currency: "INR",
      maximumFractionDigits: 0,
    }).format(Number(n || 0));
  } catch {
    return `₹${Math.round(Number(n || 0))}`;
  }
}

function iso(d: Date) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function monthToRange(monthYYYYMM: string) {
  const [yy, mm] = monthYYYYMM.split("-").map((x) => Number(x));
  const start = new Date(yy, (mm || 1) - 1, 1);
  const end = new Date(yy, (mm || 1), 0); // last day of month
  return { start: iso(start), end: iso(end) };
}

export default function AdminOverviewPage() {
  const router = useRouter();

  // selection (top)
  const [selectedMonth, setSelectedMonth] = React.useState<string>("");
  const [start, setStart] = React.useState("");
  const [end, setEnd] = React.useState("");

  // NEW: portal filter
  const [portal, setPortal] = React.useState<Portal>("all");

  // data
  const [monthly, setMonthly] = React.useState<HouseMonthlyRow[]>([]);
  const [summary, setSummary] = React.useState<HouseSummaryResponse | null>(null);

  const [loadingMonthly, setLoadingMonthly] = React.useState(false);
  const [loadingSummary, setLoadingSummary] = React.useState(false);

  async function logout() {
    await fetch("/api/admin/logout", { method: "POST" }).catch(() => null);
    router.replace("/admin/login");
  }

  async function loadMonthly(pOverride?: Portal) {
    const p = pOverride ?? portal;
    setLoadingMonthly(true);
    try {
      const res = await getHouseMonthly({ months: 12, portal: p });
      setMonthly(res.rows || []);
    } catch (e: any) {
      toast.error(String(e?.message ?? e));
    } finally {
      setLoadingMonthly(false);
    }
  }

  async function loadSummary(params?: { start?: string; end?: string }, pOverride?: Portal) {
    const p = pOverride ?? portal;
    setLoadingSummary(true);
    try {
      const res = await getHouseSummary({ ...(params ?? {}), portal: p });
      setSummary(res);
    } catch (e: any) {
      toast.error(String(e?.message ?? e));
    } finally {
      setLoadingSummary(false);
    }
  }

  React.useEffect(() => {
    // initial load (all-time)
    loadMonthly("all");
    loadSummary(undefined, "all");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function applyAllTime() {
    setSelectedMonth("");
    setStart("");
    setEnd("");
    loadSummary();
  }

  function applyRange(s: string, e: string) {
    setSelectedMonth("");
    setStart(s);
    setEnd(e);
    loadSummary({ start: s, end: e });
  }

  function applyMonth(m: string) {
    const r = monthToRange(m);
    setSelectedMonth(m);
    setStart(r.start);
    setEnd(r.end);
    loadSummary({ start: r.start, end: r.end });
  }

  function applyPortal(next: Portal) {
    setPortal(next);
    // keep current selection (month / range / all-time) and just refresh data
    loadMonthly(next);

    if (start && end) {
      loadSummary({ start, end }, next);
    } else {
      loadSummary(undefined, next);
    }
  }

  const title =
    summary?.mode === "range" && summary.window?.start && summary.window?.end
      ? `House Summary • ${summary.window.start} → ${summary.window.end}`
      : "House Summary • All-time";

  return (
    <div className="min-h-screen bg-background">
      <header className="sticky top-0 z-10 border-b bg-background/80 backdrop-blur">
        <div className="flex h-14 items-center justify-between px-4 md:px-6">
          <div className="text-sm font-medium">Admin • Overview</div>
          <div className="flex items-center gap-2">
            <Button variant="outline" className="rounded-xl" onClick={() => router.push("/admin/clients")}>
              Clients
            </Button>
            <Button variant="outline" className="rounded-xl" onClick={logout}>
              Logout
            </Button>
          </div>
        </div>
      </header>

      <main className="px-4 md:px-6 py-6 space-y-4">
        {/* Top controls: Portal + Month + Date Range */}
        <Card className="rounded-2xl">
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle className="text-base">Filters</CardTitle>
            <div className="flex items-center gap-2">
              <Button variant="outline" className="rounded-xl" onClick={applyAllTime}>
                All-time
              </Button>
              <Button
                variant="outline"
                className="rounded-xl"
                onClick={() => loadSummary(start && end ? { start, end } : undefined)}
                disabled={loadingSummary}
              >
                {loadingSummary ? "Refreshing..." : "Refresh"}
              </Button>
            </div>
          </CardHeader>

          <CardContent className="space-y-4">
            {/* Portal + Month dropdown */}
            <div className="grid gap-3 md:grid-cols-4">
              <div>
                <div className="text-xs text-muted-foreground mb-1">Portal</div>
                <select
                  className="w-full h-10 rounded-xl border bg-background px-3 text-sm"
                  value={portal}
                  onChange={(e) => applyPortal(e.target.value as Portal)}
                  disabled={loadingMonthly || loadingSummary}
                >
                  <option value="all">All</option>
                  <option value="myntra">Myntra</option>
                  <option value="flipkart">Flipkart</option>
                </select>
              </div>

              <div className="md:col-span-2">
                <div className="text-xs text-muted-foreground mb-1">Quick month</div>
                <select
                  className="w-full h-10 rounded-xl border bg-background px-3 text-sm"
                  value={selectedMonth}
                  onChange={(e) => {
                    const v = e.target.value;
                    setSelectedMonth(v);
                    if (v) applyMonth(v);
                  }}
                  disabled={loadingMonthly}
                >
                  <option value="">Select month…</option>
                  {[...monthly].slice().reverse().map((m) => (
                    <option key={m.month} value={m.month}>
                      {m.month}
                    </option>
                  ))}
                </select>
                {loadingMonthly ? (
                  <div className="mt-1 text-[11px] text-muted-foreground">Loading months…</div>
                ) : null}
              </div>
            </div>

            {/* Month cards */}
            <div className="overflow-x-auto">
              <div className="flex gap-2 min-w-max">
                {monthly.map((m) => {
                  const active = selectedMonth === m.month;
                  return (
                    <button
                      key={m.month}
                      onClick={() => applyMonth(m.month)}
                      className={[
                        "rounded-2xl border px-3 py-2 text-left hover:bg-muted/40 transition",
                        active ? "bg-muted/50 border-muted-foreground/20" : "",
                      ].join(" ")}
                    >
                      <div className="text-xs font-mono text-muted-foreground">{m.month}</div>
                      <div className="text-sm font-semibold">{fmtINR(m.gmv)}</div>
                      <div className="text-[11px] text-muted-foreground">
                        Returns: {Number(m.returns_total || 0).toLocaleString()}
                      </div>
                    </button>
                  );
                })}
                {!monthly.length && !loadingMonthly ? (
                  <div className="text-sm text-muted-foreground">No monthly data</div>
                ) : null}
              </div>
            </div>

            {/* Date range */}
            <div className="grid gap-3 md:grid-cols-4">
              <div>
                <div className="text-xs text-muted-foreground mb-1">Start</div>
                <Input className="rounded-xl" type="date" value={start} onChange={(e) => setStart(e.target.value)} />
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">End</div>
                <Input className="rounded-xl" type="date" value={end} onChange={(e) => setEnd(e.target.value)} />
              </div>
              <div className="flex items-end gap-2">
                <Button
                  className="rounded-xl"
                  onClick={() => {
                    if (!start || !end) return toast.error("Start and End are required");
                    applyRange(start, end);
                  }}
                  disabled={loadingSummary}
                >
                  Apply
                </Button>
                <Button
                  variant="outline"
                  className="rounded-xl"
                  onClick={() => {
                    setSelectedMonth("");
                    setStart("");
                    setEnd("");
                    loadSummary();
                  }}
                  disabled={loadingSummary}
                >
                  Clear
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Unified Summary (changes for month/date range/all-time) */}
        <Card className="rounded-2xl">
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle className="text-base">{title}</CardTitle>
            {loadingSummary ? <div className="text-xs text-muted-foreground">Loading…</div> : null}
          </CardHeader>

          <CardContent className="space-y-3">
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
              <Card className="rounded-2xl">
                <CardContent className="p-4">
                  <div className="text-xs text-muted-foreground">Total GMV</div>
                  <div className="text-2xl font-semibold">{fmtINR(summary?.totals.gmv ?? 0)}</div>
                </CardContent>
              </Card>

              <Card className="rounded-2xl">
                <CardContent className="p-4">
                  <div className="text-xs text-muted-foreground">Orders (Units)</div>
                  <div className="text-2xl font-semibold">{Number(summary?.totals.orders ?? 0).toLocaleString()}</div>
                </CardContent>
              </Card>

              <Card className="rounded-2xl">
                <CardContent className="p-4">
                  <div className="text-xs text-muted-foreground">Returns (Overall)</div>
                  <div className="text-2xl font-semibold">
                    {Number(summary?.totals.returns_total ?? 0).toLocaleString()}
                  </div>
                </CardContent>
              </Card>

              <Card className="rounded-2xl">
                <CardContent className="p-4">
                  <div className="text-xs text-muted-foreground">RTO</div>
                  <div className="text-2xl font-semibold">
                    {Number(summary?.totals.returns_rto ?? 0).toLocaleString()}
                  </div>
                </CardContent>
              </Card>

              <Card className="rounded-2xl">
                <CardContent className="p-4">
                  <div className="text-xs text-muted-foreground">Customer Returns</div>
                  <div className="text-2xl font-semibold">
                    {Number(summary?.totals.returns_customer ?? 0).toLocaleString()}
                  </div>
                </CardContent>
              </Card>
            </div>

            <div className="overflow-x-auto rounded-xl border">
              <table className="w-full text-sm">
                <thead className="bg-muted/50">
                  <tr className="text-left">
                    <th className="p-3">Client</th>
                    <th className="p-3 text-right">Orders</th>
                    <th className="p-3 text-right">GMV</th>
                    <th className="p-3 text-right">Returns</th>
                    <th className="p-3 text-right">RTO</th>
                    <th className="p-3 text-right">Customer</th>
                    <th className="p-3 text-right">Share %</th>
                  </tr>
                </thead>
                <tbody>
                  {(summary?.rows ?? []).map((r) => (
                    <tr
                      key={r.workspace_slug}
                      className="border-t cursor-pointer hover:bg-muted/30"
                      onClick={() => {
                        router.push(`/dashboard?workspace=${encodeURIComponent(r.workspace_slug)}`);
                      }}
                    >
                      <td className="p-3">
                        <div className="font-medium">{r.workspace_name}</div>
                        <div className="text-xs text-muted-foreground font-mono">{r.workspace_slug}</div>
                      </td>
                      <td className="p-3 text-right">{Number(r.orders || 0).toLocaleString()}</td>
                      <td className="p-3 text-right">{fmtINR(r.gmv || 0)}</td>
                      <td className="p-3 text-right">{Number(r.returns_total || 0).toLocaleString()}</td>
                      <td className="p-3 text-right">{Number(r.returns_rto || 0).toLocaleString()}</td>
                      <td className="p-3 text-right">{Number(r.returns_customer || 0).toLocaleString()}</td>
                      <td className="p-3 text-right">{Number(r.share_pct || 0).toFixed(2)}%</td>
                    </tr>
                  ))}

                  {!summary?.rows?.length ? (
                    <tr>
                      <td className="p-3 text-muted-foreground" colSpan={7}>
                        No data.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      </main>
    </div>
  );
}
