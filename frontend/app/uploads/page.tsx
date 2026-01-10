// frontend/app/uploads/page.tsx
"use client";

import * as React from "react";
import { AppShell } from "@/components/app-shell";
import WorkspaceBar from "@/components/WorkspaceBar";
import { useWorkspace } from "@/lib/workspace-context";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";
import {
  uploadSales,
  uploadReturns,
  uploadCatalog,
  uploadStock,
  uploadFlipkartOrders,
  uploadFlipkartReturns,
  uploadFlipkartListing,
  uploadFlipkartTraffic,
  type IngestResult,
} from "@/lib/api";

type UploadKind = "sales" | "returns" | "catalog" | "stock" | "flipkart_traffic";

type UploadState = {
  selected?: string | null;
  loading?: boolean;
  lastUploadAt?: string | null;
  inserted?: number | null;
  error?: string | null;
  replace?: boolean;
};

export default function UploadsPage() {
  const { workspaceSlug, portal } = useWorkspace();
  const isFlipkart = portal === "flipkart";

  const [state, setState] = React.useState<Record<UploadKind, UploadState>>({
    sales: { replace: false },
    returns: { replace: false },
    catalog: { replace: false },
    stock: { replace: false },
    flipkart_traffic: { replace: false },
  });

  function setReplace(kind: UploadKind, value: boolean) {
    setState((prev) => ({
      ...prev,
      [kind]: { ...prev[kind], replace: value },
    }));
  }

  async function handleUpload(kind: UploadKind, file?: File) {
    if (!file) return;

    const replace = !!state[kind]?.replace;

    setState((prev) => ({
      ...prev,
      [kind]: {
        ...prev[kind],
        selected: file.name,
        loading: true,
        error: null,
      },
    }));

    try {
      // -------------------------------
      // FLIPKART MODE
      // -------------------------------
      if (isFlipkart) {
        if (kind === "sales") {
          const res: any = await uploadFlipkartOrders({ file, replace, workspace_slug: workspaceSlug });
          const inserted = Number(res?.inserted_sales ?? 0);

          setState((prev) => ({
            ...prev,
            [kind]: { ...prev[kind], loading: false, lastUploadAt: new Date().toLocaleString(), inserted, error: null },
          }));

          toast.success("FLIPKART ORDERS uploaded", {
            description: `Workspace: ${workspaceSlug} • Sales rows: ${res?.inserted_sales ?? 0} • Replace: ${
              replace ? "YES" : "NO"
            }`,
          });
          return;
        }

        if (kind === "returns") {
          const res: any = await uploadFlipkartReturns({ file, replace, workspace_slug: workspaceSlug });
          const inserted = Number(res?.inserted_returns ?? 0);

          setState((prev) => ({
            ...prev,
            [kind]: { ...prev[kind], loading: false, lastUploadAt: new Date().toLocaleString(), inserted, error: null },
          }));

          toast.success("FLIPKART RETURNS uploaded", {
            description: `Workspace: ${workspaceSlug} • Return rows: ${res?.inserted_returns ?? 0} • Replace: ${
              replace ? "YES" : "NO"
            }`,
          });
          return;
        }

        if (kind === "catalog") {
          const res: any = await uploadFlipkartListing({ file, replace, workspace_slug: workspaceSlug });
          const inserted = Number(res?.inserted_catalog ?? 0);

          setState((prev) => ({
            ...prev,
            [kind]: { ...prev[kind], loading: false, lastUploadAt: new Date().toLocaleString(), inserted, error: null },
          }));

          toast.success("FLIPKART LISTING uploaded", {
            description: `Workspace: ${workspaceSlug} • Catalog: ${res?.inserted_catalog ?? 0} • Stock rows: ${
              res?.inserted_stock ?? 0
            } • Replace: ${replace ? "YES" : "NO"}`,
          });
          return;
        }

        if (kind === "flipkart_traffic") {
          // backend expects: replace_history (not replace)
          const res: any = await uploadFlipkartTraffic(file, {
            replace_history: replace,
            workspace_slug: workspaceSlug,
          });

          const inserted = Number(res?.inserted ?? 0);

          setState((prev) => ({
            ...prev,
            [kind]: { ...prev[kind], loading: false, lastUploadAt: new Date().toLocaleString(), inserted, error: null },
          }));

          toast.success("FLIPKART TRAFFIC uploaded", {
            description: `Workspace: ${workspaceSlug} • Inserted: ${inserted} • Replace: ${replace ? "YES" : "NO"}`,
          });
          return;
        }

        if (kind === "stock") {
          throw new Error("Flipkart mode: stock comes via Listing upload (Catalog+Stock).");
        }
      }

      // -------------------------------
      // MYNTRA MODE (existing)
      // -------------------------------
      let r: IngestResult;

      if (kind === "sales") {
        r = await uploadSales({ file, replace, workspace_slug: workspaceSlug });
      } else if (kind === "returns") {
        r = await uploadReturns({ file, replace, workspace_slug: workspaceSlug });
      } else if (kind === "catalog") {
        r = await uploadCatalog({ file, replace, workspace_slug: workspaceSlug });
      } else if (kind === "stock") {
        r = await uploadStock({ file, replace, workspace_slug: workspaceSlug });
      } else {
        // traffic upload is only for flipkart
        throw new Error("Flipkart Traffic upload is only available when portal=flipkart.");
      }

      setState((prev) => ({
        ...prev,
        [kind]: {
          ...prev[kind],
          loading: false,
          lastUploadAt: new Date().toLocaleString(),
          inserted: r.inserted ?? 0,
          error: null,
        },
      }));

      toast.success(`${kind.toUpperCase()} uploaded`, {
        description: `Workspace: ${workspaceSlug} • Inserted ${r.inserted ?? 0} rows • Replace: ${replace ? "YES" : "NO"}`,
      });
    } catch (e: any) {
      const msg = String(e?.message ?? e);

      setState((prev) => ({
        ...prev,
        [kind]: { ...prev[kind], loading: false, error: msg },
      }));

      toast.error(`${kind.toUpperCase()} upload failed`, { description: msg });
    }
  }

  return (
    <AppShell>
      <div className="space-y-4">
        <WorkspaceBar />

        <div className="grid gap-4 md:grid-cols-4">
          <UploadCard
            kind="sales"
            title={isFlipkart ? "Flipkart Orders Upload" : "Sales Upload"}
            hint={isFlipkart ? "orders → sales_raw" : "sales_raw"}
            accept={isFlipkart ? ".xlsx,.xls,.csv" : ".csv"}
            data={state.sales}
            onUpload={handleUpload}
            onReplaceChange={setReplace}
          />

          <UploadCard
            kind="returns"
            title={isFlipkart ? "Flipkart Returns Upload" : "Returns Upload"}
            hint={isFlipkart ? "returns → returns_raw" : "returns_raw"}
            accept={isFlipkart ? ".xlsx,.xls,.csv" : ".csv"}
            data={state.returns}
            onUpload={handleUpload}
            onReplaceChange={setReplace}
          />

          <UploadCard
            kind="catalog"
            title={isFlipkart ? "Flipkart Listing Upload" : "Catalog Upload"}
            hint={isFlipkart ? "listing → catalog_raw + stock_raw" : "catalog_raw"}
            accept={isFlipkart ? ".xlsx,.xls,.csv" : ".csv"}
            data={state.catalog}
            onUpload={handleUpload}
            onReplaceChange={setReplace}
          />

          {isFlipkart ? (
            <UploadCard
              kind="flipkart_traffic"
              title="Flipkart Traffic Upload"
              hint="search traffic → flipkart_traffic_raw"
              accept=".xlsx,.xls"
              data={state.flipkart_traffic}
              onUpload={handleUpload}
              onReplaceChange={setReplace}
            />
          ) : (
            <UploadCard
              kind="stock"
              title="Stock Upload"
              hint="stock_raw (SKU + qty)"
              accept=".csv"
              data={state.stock}
              onUpload={handleUpload}
              onReplaceChange={setReplace}
            />
          )}
        </div>

        {isFlipkart ? (
          <div className="text-xs text-muted-foreground">
            Flipkart mode: upload <b>Orders</b>, <b>Returns</b>, <b>Listing</b> (Catalog+Stock), and <b>Traffic</b>.
          </div>
        ) : null}
      </div>
    </AppShell>
  );
}

function UploadCard({
  kind,
  title,
  hint,
  accept,
  data,
  onUpload,
  onReplaceChange,
}: {
  kind: UploadKind;
  title: string;
  hint: string;
  accept: string;
  data: UploadState;
  onUpload: (kind: UploadKind, file?: File) => void;
  onReplaceChange: (kind: UploadKind, value: boolean) => void;
}) {
  const inputRef = React.useRef<HTMLInputElement | null>(null);

  function pick() {
    inputRef.current?.click();
  }

  return (
    <Card className="rounded-2xl">
      <CardHeader className="pb-2">
        <CardTitle className="text-base">{title}</CardTitle>
        <div className="text-xs text-muted-foreground">{hint}</div>
      </CardHeader>

      <CardContent className="space-y-3">
        <input
          ref={inputRef}
          type="file"
          accept={accept}
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            onUpload(kind, f);
            e.currentTarget.value = "";
          }}
        />

        <label className="flex items-center gap-2 text-xs select-none">
          <input
            type="checkbox"
            checked={!!data.replace}
            onChange={(e) => onReplaceChange(kind, e.target.checked)}
          />
          Replace existing data for this workspace
        </label>

        <div className="text-xs text-muted-foreground">
          Last upload: {data.lastUploadAt ?? "—"}
          <br />
          Rows inserted: {data.inserted ?? "—"}
          <br />
          Selected: {data.selected ?? "—"}
        </div>

        {data.error ? <div className="text-xs text-red-600 break-words">{data.error}</div> : null}

        <Button className="w-full" onClick={pick} disabled={data.loading}>
          {data.loading ? "Uploading…" : "Upload file"}
        </Button>
      </CardContent>
    </Card>
  );
}
