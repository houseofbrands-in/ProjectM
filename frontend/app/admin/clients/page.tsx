"use client";

import * as React from "react";
import { useRouter } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { toast } from "sonner";
import {
  createWorkspace,
  deleteWorkspace,
  getWorkspaces,
  type WorkspaceRow,
} from "@/lib/api";

function slugify(s: string) {
  return s
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_-]/g, "")
    .slice(0, 64);
}

export default function AdminClientsPage() {
  const router = useRouter();

  const [items, setItems] = React.useState<WorkspaceRow[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const [showAdd, setShowAdd] = React.useState(false);
  const [newName, setNewName] = React.useState("");
  const [newSlug, setNewSlug] = React.useState("");
  const [saving, setSaving] = React.useState(false);

  async function load() {
    setLoading(true);
    try {
      setErr(null);
      const ws = await getWorkspaces();
      setItems(ws);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load();
  }, []);

  async function onCreate() {
    const name = newName.trim();
    const slug = newSlug.trim();

    if (!name) return toast.error("Client name is required");
    if (!slug) return toast.error("Slug is required");

    setSaving(true);
    try {
      const created = await createWorkspace({ name, slug });
      toast.success(`Client created: ${created.name}`);
      setShowAdd(false);
      setNewName("");
      setNewSlug("");
      await load();
    } catch (e: any) {
      toast.error(String(e?.message ?? e));
    } finally {
      setSaving(false);
    }
  }

  async function onDelete(w: WorkspaceRow) {
    const ok = window.confirm(
      `Delete client "${w.name}" (${w.slug})?\n\nSafe delete only works if this client has NO data.`
    );
    if (!ok) return;

    try {
      await deleteWorkspace({ slug: w.slug });
      toast.success(`Deleted: ${w.name}`);
      await load();
    } catch (e: any) {
      const msg = String(e?.message ?? e);

      // If backend says force=true required, ask again
      if (msg.includes("cannot be deleted without force=true")) {
        const ok2 = window.confirm(
          `This client has data.\n\nFORCE DELETE will permanently delete ALL sales/returns/catalog/stock/etc for "${w.name}".\n\nContinue?`
        );
        if (!ok2) return;

        try {
          await deleteWorkspace({ slug: w.slug, force: true });
          toast.success(`Force deleted: ${w.name}`);
          await load();
          return;
        } catch (e2: any) {
          toast.error(String(e2?.message ?? e2));
          return;
        }
      }

      toast.error(msg);
    }
  }

  async function logout() {
    await fetch("/api/admin/logout", { method: "POST" }).catch(() => null);
    router.replace("/admin/login");
  }

  return (
    <div className="min-h-screen bg-background">
      <header className="sticky top-0 z-10 border-b bg-background/80 backdrop-blur">
        <div className="flex h-14 items-center justify-between px-4 md:px-6">
          <div className="text-sm font-medium">Admin • Clients</div>
          <div className="flex items-center gap-2">
  <Button
    variant="outline"
    className="rounded-xl"
    onClick={() => router.push("/admin/overview")}
  >
    Overview
  </Button>

  <Button variant="outline" className="rounded-xl" onClick={() => setShowAdd((v) => !v)}>
    {showAdd ? "Close" : "Add client"}
  </Button>

  <Button variant="outline" className="rounded-xl" onClick={logout}>
    Logout
  </Button>
</div>

        </div>
      </header>

      <main className="px-4 md:px-6 py-6 space-y-4">
        {showAdd ? (
          <Card className="rounded-2xl">
            <CardHeader>
              <CardTitle className="text-base">Create new client</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-3 md:grid-cols-3">
              <div>
                <div className="text-xs text-muted-foreground mb-1">
                  Client name
                </div>
                <Input
                  className="rounded-xl"
                  value={newName}
                  onChange={(e) => {
                    const v = e.target.value;
                    setNewName(v);
                    if (!newSlug.trim()) setNewSlug(slugify(v));
                  }}
                  placeholder="Rekha Maniyar"
                  disabled={saving}
                />
              </div>

              <div>
                <div className="text-xs text-muted-foreground mb-1">
                  Workspace slug
                </div>
                <Input
                  className="rounded-xl font-mono"
                  value={newSlug}
                  onChange={(e) => setNewSlug(e.target.value)}
                  placeholder="rekha_maniyar"
                  disabled={saving}
                />
                <div className="mt-1 text-[11px] text-muted-foreground">
                  lowercase, numbers, _ or - only
                </div>
              </div>

              <div className="flex items-end gap-2">
                <Button className="rounded-xl" onClick={onCreate} disabled={saving}>
                  {saving ? "Creating..." : "Create"}
                </Button>
                <Button
                  variant="outline"
                  className="rounded-xl"
                  onClick={() => {
                    setNewName("");
                    setNewSlug("");
                  }}
                  disabled={saving}
                >
                  Clear
                </Button>
              </div>
            </CardContent>
          </Card>
        ) : null}

        {err ? (
          <div className="rounded-xl border border-red-300 bg-red-50 p-3 text-sm text-red-700">
            {err}
          </div>
        ) : null}

        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {loading ? (
            <div className="text-sm text-muted-foreground">Loading clients…</div>
          ) : (
            items.map((w) => (
              <button
                key={w.slug}
                className="text-left"
                onClick={() => {
                  router.push(`/dashboard?workspace=${encodeURIComponent(w.slug)}`);
                }}
              >
                <Card className="rounded-2xl hover:bg-muted/40 transition">
                  <CardContent className="p-4 space-y-3">
                    <div>
                      <div className="text-sm font-semibold line-clamp-1">
                        {w.name}
                      </div>
                      <div className="mt-1 text-xs text-muted-foreground font-mono">
                        {w.slug}
                      </div>
                    </div>

                    <div className="flex items-center justify-between gap-2">
                      <div className="text-xs text-muted-foreground">
                        Open dashboard →
                      </div>

                      <Button
                        variant="outline"
                        className="rounded-xl text-red-600 border-red-200 hover:bg-red-50"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          onDelete(w);
                        }}
                      >
                        Delete
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              </button>
            ))
          )}
        </div>
      </main>
    </div>
  );
}
