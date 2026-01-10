"use client";

import * as React from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

export default function AdminLoginPage() {
  const router = useRouter();
  const params = useSearchParams();
  const next = params.get("next") ?? "/admin/clients";

  const [username, setUsername] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [loading, setLoading] = React.useState(false);

  async function onLogin() {
    setLoading(true);
    try {
      const res = await fetch("/api/admin/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });

      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json?.message || `Login failed (${res.status})`);

      toast.success("Logged in");
      router.replace(next);
    } catch (e: any) {
      toast.error(String(e?.message ?? e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <Card className="w-full max-w-md rounded-2xl">
        <CardHeader>
          <CardTitle className="text-base">Admin Login</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div>
            <div className="text-xs text-muted-foreground mb-1">Username</div>
            <Input
              className="rounded-xl"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
            />
          </div>

          <div>
            <div className="text-xs text-muted-foreground mb-1">Password</div>
            <Input
              className="rounded-xl"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              type="password"
              autoComplete="current-password"
              onKeyDown={(e) => {
                if (e.key === "Enter") onLogin();
              }}
            />
          </div>

          <Button className="rounded-xl w-full" onClick={onLogin} disabled={loading}>
            {loading ? "Signing in..." : "Sign in"}
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
