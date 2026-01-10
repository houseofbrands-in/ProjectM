import {
  defaultPortal,
  defaultRange,
  LS_END,
  LS_PORTAL,
  LS_START,
  LS_WORKSPACE,
  safeGetLS,
  safeSetLS,
} from "./workspaceDefaults";

export function getStoredWorkspaceSlug() {
  return safeGetLS(LS_WORKSPACE) ?? "default";
}

export function setStoredWorkspaceSlug(slug: string) {
  safeSetLS(LS_WORKSPACE, slug);
}

export function getStoredDateRange() {
  const def = defaultRange();
  return {
    start: safeGetLS(LS_START) ?? def.start,
    end: safeGetLS(LS_END) ?? def.end,
  };
}

export function setStoredDateRange(start: string, end: string) {
  safeSetLS(LS_START, start);
  safeSetLS(LS_END, end);
}

// ✅ NEW
export function getStoredPortal() {
  return safeGetLS(LS_PORTAL) ?? defaultPortal();
}

// ✅ NEW
export function setStoredPortal(portal: string) {
  safeSetLS(LS_PORTAL, portal);
}
