"use client";
import React, { Suspense } from "react";
import { WorkspaceProvider } from "../lib/workspace-context";

function Inner({ children }: { children: React.ReactNode }) {
  return <WorkspaceProvider>{children}</WorkspaceProvider>;
}

export default function Providers({ children }: { children: React.ReactNode }) {
  return (
    <Suspense fallback={null}>
      <Inner>{children}</Inner>
    </Suspense>
  );
}
