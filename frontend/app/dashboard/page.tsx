// frontend/app/dashboard/page.tsx

"use client";

import { AppShell } from "@/components/app-shell";
import WorkspaceBar from "@/components/WorkspaceBar";
import { DateRangeClient } from "./range-client";

export default function DashboardPage() {
  return (
    <AppShell>
      <div className="space-y-4">
        <WorkspaceBar />
        <DateRangeClient />
      </div>
    </AppShell>
  );
}
