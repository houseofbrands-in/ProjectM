"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { getWorkspaces, type WorkspaceRow } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace-context";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

const PORTALS = [
  { label: "Myntra", value: "myntra" },
  { label: "Flipkart", value: "flipkart" },
  { label: "Amazon", value: "amazon" },
  { label: "Ajio", value: "ajio" },
  { label: "Meesho", value: "meesho" },
  { label: "Snapdeal", value: "snapdeal" },
];

export default function WorkspaceBar() {
  const router = useRouter();

  const { workspaceSlug, portal, setPortal, start, end, setStart, setEnd } = useWorkspace();

  const [items, setItems] = React.useState<WorkspaceRow[]>([]);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    let alive = true;
    (async () => {
      try {
        setLoading(true);
        const ws = await getWorkspaces();
        if (!alive) return;
        setItems(ws);
      } catch {
        if (!alive) return;
        setItems([]);
      } finally {
        if (!alive) return;
        setLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  const wsName = React.useMemo(() => {
    const found = items.find((x) => x.slug === workspaceSlug);
    return found?.name ?? null;
  }, [items, workspaceSlug]);

  return (
    <Card className="rounded-2xl p-3">
      <div className="flex flex-wrap items-center gap-3">
        <div className="text-sm font-medium">Workspace</div>

        <div className="h-9 rounded-xl border bg-background px-3 text-sm flex items-center">
          {loading ? "Loading…" : wsName ? `${wsName} (${workspaceSlug})` : workspaceSlug}
        </div>

        <Button
          variant="outline"
          className="h-9 rounded-xl"
          onClick={() => router.push("/admin/clients")}
        >
          Change client
        </Button>

        <div className="text-sm font-medium ml-2">Portal</div>

        <select
          className="h-9 rounded-xl border bg-background px-3 text-sm"
          value={portal}
          onChange={(e) => setPortal(e.target.value)}
        >
          {PORTALS.map((p) => (
            <option key={p.value} value={p.value}>
              {p.label}
            </option>
          ))}
        </select>

        <div className="ml-auto flex flex-wrap items-center gap-2">
          <div className="text-sm text-muted-foreground">Date range</div>

          <input
            type="date"
            className="h-9 rounded-xl border bg-background px-3 text-sm"
            value={start}
            onChange={(e) => setStart(e.target.value)}
          />
          <span className="text-muted-foreground">to</span>
          <input
            type="date"
            className="h-9 rounded-xl border bg-background px-3 text-sm"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
          />
        </div>
      </div>
    </Card>
  );
}
