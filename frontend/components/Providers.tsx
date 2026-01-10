"use client";

import React from "react";
import { WorkspaceProvider } from "../lib/workspace-context";

export default function Providers({ children }: { children: React.ReactNode }) {
  return <WorkspaceProvider>{children}</WorkspaceProvider>;
}
