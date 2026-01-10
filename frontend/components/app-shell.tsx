"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";

const nav = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/orders", label: "Orders" },                // ✅ NEW
  { href: "/returns-insights", label: "Returns Insights" },
  { href: "/ad-recommendations", label: "Ad Recommendations" },
  { href: "/uploads", label: "Uploads" },
  { href: "/zero-sales", label: "Zero Sales" },
];



export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="min-h-screen bg-background">
      <div className="flex">
        {/* Sidebar */}
        <aside className="hidden md:flex w-64 flex-col border-r bg-background">
          <div className="px-6 py-5">
            <div className="flex items-center justify-between">
              <div className="text-lg font-semibold">Project M</div>
              <Badge variant="secondary">Client Mode</Badge>
            </div>
            <div className="text-xs text-muted-foreground mt-1">
              Workspace: {pathname.startsWith("/admin") ? "Admin" : "Selected"}
            </div>
          </div>

          <Separator />

          <nav className="px-3 py-3 space-y-1">
            {nav.map((item) => {
              const active = pathname === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={[
                    "block rounded-md px-3 py-2 text-sm transition",
                    active
                      ? "bg-muted font-medium"
                      : "hover:bg-muted/60 text-muted-foreground hover:text-foreground",
                  ].join(" ")}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>

          <div className="mt-auto px-6 py-4 text-xs text-muted-foreground">
            v0.1 • FastAPI + Postgres
          </div>
        </aside>

        {/* Main */}
        <div className="flex-1">
          {/* Topbar */}
          <header className="sticky top-0 z-10 border-b bg-background/80 backdrop-blur">
            <div className="flex h-14 items-center justify-between px-4 md:px-6">
              <div className="text-sm text-muted-foreground">
                {nav.find((n) => n.href === pathname)?.label ?? "Project M"}

              </div>
              <div className="text-xs text-muted-foreground">
                API: <span className="text-foreground">Connected</span>
              </div>
            </div>
          </header>

          <main className="px-4 md:px-6 py-6">{children}</main>
        </div>
      </div>
    </div>
  );
}
