"use client";

import { AppShell } from "@/components/app-shell";
import WorkspaceBar from "@/components/WorkspaceBar";
import OrdersClient from "./orders-client";

export default function OrdersPage() {
  return (
    <AppShell>
      <div className="space-y-4">
        <WorkspaceBar />
        <OrdersClient />
      </div>
    </AppShell>
  );
}
