export const LS_WORKSPACE = "pm_workspace_slug";
export const LS_START = "pm_date_start";
export const LS_END = "pm_date_end";

// ✅ NEW
export const LS_PORTAL = "pm_portal";

export function formatDate(d: Date) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

export function defaultRange() {
  const end = new Date();
  const start = new Date();
  start.setDate(end.getDate() - 30);
  return { start: formatDate(start), end: formatDate(end) };
}

// ✅ NEW
export function defaultPortal() {
  return "myntra";
}

export function safeGetLS(key: string): string | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

export function safeSetLS(key: string, value: string) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(key, value);
  } catch {}
}
