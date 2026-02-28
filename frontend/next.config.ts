import type { NextConfig } from "next";

const nextConfig: NextConfig & { experimental?: any } = {
  experimental: {
    missingSuspenseWithCSRBailout: false,
    proxyClientMaxBodySize: "50mb",
  },
};

export default nextConfig;
