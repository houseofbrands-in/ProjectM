// frontend/app/layout.tsx

import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { Toaster } from "@/components/ui/sonner";
import Providers from "@/components/Providers";
import Link from "next/link";


const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Project M",
  description: "Project M Dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased`}>
  <Providers>
    <div className="min-h-screen">
      <header className="sticky top-0 z-50 border-b bg-background/80 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3">
          <div className="flex items-center gap-3">
            <div className="font-semibold">Project M</div>
            <div className="text-xs text-muted-foreground">Web Dashboard</div>
          </div>

          <nav className="flex items-center gap-2 text-sm">
            <Link className="rounded-xl px-3 py-2 hover:bg-muted" href="/">
              Dashboard
            </Link>
            <Link className="rounded-xl px-3 py-2 hover:bg-muted" href="/returns-insights">
              Returns Insights
            </Link>
            <Link className="rounded-xl px-3 py-2 hover:bg-muted" href="/ad-recommendations">
              Ad Recommendations
            </Link>
            <Link className="rounded-xl px-3 py-2 hover:bg-muted" href="/forecast">
              Forecast
            </Link>
            <Link className="rounded-xl px-3 py-2 hover:bg-muted" href="/asp-optimizer">
              ASP Optimizer
            </Link>


          </nav>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-4">{children}</main>
    </div>
  </Providers>

  <Toaster richColors />
</body>

    </html>
  );
}
