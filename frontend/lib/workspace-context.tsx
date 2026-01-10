"use client";

import React, { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import {
  getStoredDateRange,
  getStoredWorkspaceSlug,
  setStoredDateRange,
  setStoredWorkspaceSlug,
} from "./workspaceStore";
import { defaultRange } from "./workspaceDefaults";

export type Portal =
  | "myntra"
  | "flipkart"
  | "amazon"
  | "ajio"
  | "meesho"
  | "snapdeal";

type WorkspaceCtx = {
  workspaceSlug: string;
  setWorkspaceSlug: (s: string) => void;

  portal: Portal;
  setPortal: (p: Portal) => void;

  start: string;
  end: string;
  setStart: (s: string) => void;
  setEnd: (s: string) => void;
};

const Ctx = createContext<WorkspaceCtx | null>(null);

const LS_PORTAL_KEY = "projectm_portal";

function getStoredPortal(): Portal | null {
  try {
    const v = localStorage.getItem(LS_PORTAL_KEY);
    if (!v) return null;
    return v as Portal;
  } catch {
    return null;
  }
}

function setStoredPortal(p: Portal) {
  try {
    localStorage.setItem(LS_PORTAL_KEY, p);
  } catch {
    // ignore
  }
}

function setParam(
  router: ReturnType<typeof useRouter>,
  pathname: string,
  params: URLSearchParams,
  key: string,
  value: string
) {
  const next = new URLSearchParams(params.toString());
  next.set(key, value);
  router.replace(`${pathname}?${next.toString()}`);
}

export function WorkspaceProvider({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();

  const [workspaceSlug, _setWorkspaceSlug] = useState("default");
  const [portal, _setPortal] = useState<Portal>("myntra");
  const [start, _setStart] = useState(defaultRange().start);
  const [end, _setEnd] = useState(defaultRange().end);

  const didInitRef = useRef(false);
  const paramsKey = params.toString();

  useEffect(() => {
    const urlWs = params.get("workspace");
    const urlPortal = (params.get("portal") || "").trim().toLowerCase();
    const urlStart = params.get("start");
    const urlEnd = params.get("end");

    if (!didInitRef.current) {
      const lsWs = getStoredWorkspaceSlug();
      const lsRange = getStoredDateRange();
      const lsPortal = getStoredPortal();
      const def = defaultRange();

      const resolvedWs = urlWs ?? lsWs ?? "default";

      const resolvedPortal: Portal =
        (urlPortal as Portal) ||
        lsPortal ||
        "myntra";

      const resolvedStart = urlStart ?? lsRange.start ?? def.start;
      const resolvedEnd = urlEnd ?? lsRange.end ?? def.end;

      _setWorkspaceSlug(resolvedWs);
      _setPortal(resolvedPortal);
      _setStart(resolvedStart);
      _setEnd(resolvedEnd);

      setStoredWorkspaceSlug(resolvedWs);
      setStoredPortal(resolvedPortal);
      setStoredDateRange(resolvedStart, resolvedEnd);

      didInitRef.current = true;
      return;
    }

    // After init: only sync FROM URL when URL explicitly has the param
    let nextWs = workspaceSlug;
    let nextPortal = portal;
    let nextStart = start;
    let nextEnd = end;
    let changed = false;

    if (urlWs && urlWs !== workspaceSlug) {
      nextWs = urlWs;
      changed = true;
    }

    if (urlPortal) {
      const p = urlPortal as Portal;
      if (p !== portal) {
        nextPortal = p;
        changed = true;
      }
    }

    if (urlStart && urlStart !== start) {
      nextStart = urlStart;
      changed = true;
    }
    if (urlEnd && urlEnd !== end) {
      nextEnd = urlEnd;
      changed = true;
    }

    if (changed) {
      _setWorkspaceSlug(nextWs);
      _setPortal(nextPortal);
      _setStart(nextStart);
      _setEnd(nextEnd);

      setStoredWorkspaceSlug(nextWs);
      setStoredPortal(nextPortal);
      setStoredDateRange(nextStart, nextEnd);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paramsKey]);

  const setWorkspaceSlug = (s: string) => {
    _setWorkspaceSlug(s);
    setStoredWorkspaceSlug(s);
    setParam(router, pathname, new URLSearchParams(params.toString()), "workspace", s);
  };

  const setPortal = (p: Portal) => {
    _setPortal(p);
    setStoredPortal(p);
    setParam(router, pathname, new URLSearchParams(params.toString()), "portal", p);
  };

  const setStart = (s: string) => {
    _setStart(s);
    setStoredDateRange(s, end);
    setParam(router, pathname, new URLSearchParams(params.toString()), "start", s);
  };

  const setEnd = (s: string) => {
    _setEnd(s);
    setStoredDateRange(start, s);
    setParam(router, pathname, new URLSearchParams(params.toString()), "end", s);
  };

  const value = useMemo(
    () => ({ workspaceSlug, setWorkspaceSlug, portal, setPortal, start, end, setStart, setEnd }),
    [workspaceSlug, portal, start, end]
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useWorkspace() {
  const v = useContext(Ctx);
  if (!v) throw new Error("useWorkspace must be used inside WorkspaceProvider");
  return v;
}
