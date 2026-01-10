import type { NextConfig } from "next";

const nextConfig: NextConfig & { experimental?: any } = {
  experimental: {
    // ✅ This is the supported one now
    proxyClientMaxBodySize: "50mb",
  },

  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://127.0.0.1:8000/:path*",
      },
    ];
  },
};

export default nextConfig;
